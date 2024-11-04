/* All the necessary headers */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <mongoc/mongoc.h>
#include <fcntl.h>        // For file descriptor operations
#include <sys/sendfile.h> // For sendfile()
#include <sys/mman.h>     // For memory mapping
#include <sys/syscall.h>  // For memfd_create()

/*To create another node, simply copy paste this node into a new node.c then after doing that, configure the ports according to the instructions*/
/*This setup enables each node to have access to direct writes to mongodb, fast read access to data using memory optimizations, and fast node updates across all nodes*/

#define _GNU_SOURCE // Define GNU feature test macro
#define WRITE_SERVER_PORT 6437 //Writes Server - different across all nodes
#define UPDATE_SERVER_PORT 6438 //Updates receiving server - receives update data from all nodes into shared memory
#define UPDATE_CLIENT_PORT 6439 //Sending Updates Client - currently its only one node client, add more ports to serve other nodes
#define SHM_KEY 0x5678 /* Key for the shared memory */
#define MAX_BATCH_SIZE 65536000 /* Max data size in shared memory */
#define BUFFER_SIZE 1024 /* Buffer size for client communication */
#define SIGNAL_READY 1 /* Signal ready for update */
#define SIGNAL_IDLE 0 /* Signal not ready for update */

char* dto_data = NULL;        // Data Transfer Object (DTO) for shared memory data
char* update_data = NULL;  
pthread_mutex_t dto_lock = PTHREAD_MUTEX_INITIALIZER;  // Mutex for DTO data

// Global MongoDB client and mutex for thread safety
mongoc_client_t *global_mongo_client;
pthread_mutex_t mongo_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t shm_lock = PTHREAD_MUTEX_INITIALIZER; // Mutex for shared memory


// MongoDB initialization function (called once)
void init_mongo_client() {
    mongoc_init();
    global_mongo_client = mongoc_client_new("mongodb://localhost:27017");

    if (!global_mongo_client) {
        fprintf(stderr, "Error: Failed to initialize MongoDB client. Check if MongoDB is running and accessible.\n");
        exit(1);  // Exit if MongoDB connection cannot be initialized
    } else {
        printf("MongoDB client successfully initialized.\n");
    }
}

// MongoDB cleanup function (called on shutdown)
void cleanup_mongo_client() {
    if (global_mongo_client) {
        mongoc_client_destroy(global_mongo_client);
        mongoc_cleanup();
        printf("MongoDB client successfully cleaned up.\n");
    }
}

// Read from MongoDB and update shared memory
void* read_to_shared_memory(void* arg) {
    mongoc_collection_t *collection;
    mongoc_cursor_t *cursor;
    bson_t *query;
    bson_error_t error;
    const bson_t *doc;
    char *json_str;
    int shmid;
    char *shared_mem;
    collection = mongoc_client_get_collection(global_mongo_client, "zero_db", "zero_collection");
    printf("Successful MongoDB initialization\n");
    // Create or access shared memory
    printf("Attempting to create shared memory with size: %lu\n", sizeof(int) + MAX_BATCH_SIZE);
    shmid = shmget(SHM_KEY, sizeof(int) + MAX_BATCH_SIZE, 0666 | IPC_CREAT);
    if (shmid == -1) {
        perror("shmget failed");
        exit(EXIT_FAILURE);
    } else {
        printf("Create or access shared memory successful\n");
    }
    // Attach shared memory
    shared_mem = (char *)shmat(shmid, NULL, 0);
    if (shared_mem == (char *)-1) {
        perror("shmat failed");
        exit(1);
    } else {
        printf("Shared memory attached successfully\n");
    }
    // Cast the first part of shared memory to signal memory for easier access
    int *signal_mem = (int *)shared_mem;
    query = bson_new();  // Empty query to get all documents
    cursor = mongoc_collection_find_with_opts(collection, query, NULL, NULL);

    // Buffer for batch data
    char *batch_data = shared_mem + sizeof(int); // The data follows the signal in shared memory
    memset(batch_data, 0, MAX_BATCH_SIZE);

    // Read documents and accumulate them in the batch
    while (mongoc_cursor_next(cursor, &doc)) {
        json_str = bson_as_json(doc, NULL);
        strncat(batch_data, json_str, MAX_BATCH_SIZE - strlen(batch_data) - 1);
        bson_free(json_str);
    }
    
    // Reset the signal
    *signal_mem = SIGNAL_IDLE;

    printf("Batch read and updated shared memory.\n");

    mongoc_cursor_destroy(cursor);
    bson_destroy(query);
    return NULL;
}

// Write received write request to MongoDB
void* write_to_mongo(void* arg) {
    const char* incoming_data = (const char*)arg;

    // Safely print the received data
    printf("Received Data: %s\n", incoming_data);

    // Check if MongoDB client is initialized
    if (!global_mongo_client) {
        fprintf(stderr, "Error: MongoDB client is not initialized. Exiting thread.\n");
        free((void*)incoming_data);  // Free the data before exiting
        return NULL;
    }

    // Prepare MongoDB write inside critical section
    pthread_mutex_lock(&mongo_mutex);  // Lock the mutex for thread-safe MongoDB access

    mongoc_collection_t *collection;
    bson_t *doc;
    bson_error_t error;

    // Get the MongoDB collection (reuse global client)
    collection = mongoc_client_get_collection(global_mongo_client, "zero_db", "zero_collection");
    if (!collection) {
        fprintf(stderr, "Error: Failed to get MongoDB collection. Check if the database and collection exist.\n");
        pthread_mutex_unlock(&mongo_mutex);  // Unlock the mutex before returning
        free((void*)incoming_data);
        return NULL;
    }

    // Create a BSON document from the received JSON data
    doc = bson_new_from_json((const uint8_t *)incoming_data, -1, &error);
    if (!doc) {
        fprintf(stderr, "Error creating BSON: %s\n", error.message);
        mongoc_collection_destroy(collection);
        pthread_mutex_unlock(&mongo_mutex);  // Unlock the mutex before returning
        free((void*)incoming_data);
        return NULL;
    }

    // Insert the document into MongoDB
    if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error)) {
        fprintf(stderr, "Error inserting document: %s\n", error.message);
    } else {
        printf("Document successfully inserted into MongoDB.\n");
    }

    // Cleanup BSON document and collection
    bson_destroy(doc);
    mongoc_collection_destroy(collection);

    // Unlock the mutex after MongoDB write is complete
    pthread_mutex_unlock(&mongo_mutex);

    // Free the memory after MongoDB write is done
    free((void*)incoming_data);

    return NULL;
}

// Update shared memory with received write request
void* shared_memory_updater(void* arg) {
    const char* incoming_data = (const char*)arg;

    int shmid = shmget(SHM_KEY, sizeof(int) + MAX_BATCH_SIZE, 0666);
    char* shm_addr = shmat(shmid, NULL, 0);
    int* signal_mem = (int*)shm_addr;

    pthread_mutex_lock(&shm_lock);

    // Perform a granular update (append new data to shared memory)
    size_t shm_data_len = strlen(shm_addr + sizeof(int));  // Existing shared memory data length
    size_t new_data_len = strlen(incoming_data);
    if (shm_data_len + new_data_len < MAX_BATCH_SIZE) {
        strncat(shm_addr + sizeof(int), incoming_data, new_data_len);  // Append data to shared memory
    }

    // Update the signal to SIGNAL_READY
    *signal_mem = SIGNAL_READY;

    pthread_mutex_unlock(&shm_lock);
    puts("successfully updated shared memory");

    free((void*)incoming_data);  // Free the memory allocated for shared memory data

    return NULL;
}

// Client to send received write request to this node to other shared memories
void* update_other_node_memory(void* arg) {
    int client_sock;
    struct sockaddr_in server;
    size_t data_len;
    int mem_fd;

    // Create client socket
    client_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (client_sock == -1) {
        perror("Failed to create socket");
        return NULL;
    }

    // Set up server address
    server.sin_family = AF_INET;
    server.sin_port = htons(UPDATE_CLIENT_PORT); // Change to NPORT for the client
    server.sin_addr.s_addr = inet_addr("127.0.0.1"); // Localhost

    // Connect to server
    if (connect(client_sock, (struct sockaddr*)&server, sizeof(server)) < 0) {
        perror("Connection failed");
        close(client_sock);
        return NULL;
    }

    const char* incoming_data = (const char*)arg;
    data_len = strlen(incoming_data);

    // Create a memory file using memfd_create
    mem_fd = syscall(SYS_memfd_create, "shm_memfile", 0);
    if (mem_fd == -1) {
        perror("memfd_create failed");
        close(client_sock);
        return NULL;
    }

    // Write shared memory data to the in-memory file
    if (write(mem_fd, incoming_data, data_len) != data_len) {
        perror("write to memory file failed");
        close(mem_fd);
        close(client_sock);
        return NULL;
    }

    // Reset file offset to the beginning for sendfile
    lseek(mem_fd, 0, SEEK_SET);

    // Send the data directly from the in-memory file descriptor to the server (other node)
    if (sendfile(client_sock, mem_fd, NULL, data_len) == -1) {
        puts("sendfile failed, try again bro");
        close(mem_fd);
        close(client_sock);
        return NULL;
    }

    // Clean up
    close(mem_fd);     // Close the in-memory file descriptor
    close(client_sock); // Close the client socket

    puts("Successfully updated other nodes");

    return NULL;
}

// Function to receive updates from other nodes and update shared memory
void* node_receive_updates(void* arg) {
    int server_fd, client_sock;
    struct sockaddr_in server, client;
    socklen_t client_len = sizeof(struct sockaddr_in);
    size_t file_size = MAX_BATCH_SIZE;
    int shm_fd;

    // Allocate memory for shared updates
    update_data = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    if (update_data == MAP_FAILED) {
        perror("Memory mapping failed");
        return NULL;
    }

    // Create server socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("Failed to create socket");
        return NULL;
    }

    // Prepare the sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(UPDATE_SERVER_PORT); // Change to UPORT for the updates server

    // Bind the socket
    if (bind(server_fd, (struct sockaddr*)&server, sizeof(server)) < 0) {
        perror("Bind failed");
        close(server_fd);
        return NULL;
    }

    // Listen for incoming connections
    if (listen(server_fd, 5) < 0) {
        perror("Listen failed");
        close(server_fd);
        return NULL;
    }

    printf("Waiting for incoming connections on UPORT...\n");

    // Accept incoming connections
    while ((client_sock = accept(server_fd, (struct sockaddr*)&client, &client_len)) >= 0) {
        printf("Connection accepted\n");

        // Create a shared memory file descriptor to receive the update
        shm_fd = memfd_create("shm_memfile", 0);
        if (shm_fd == -1) {
            perror("Failed to create memory file descriptor");
            close(client_sock);
            continue;
        }

        // Memory mapping for zero-copy data reception
        char *update_data = mmap(NULL, MAX_BATCH_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (update_data == MAP_FAILED) {
            perror("Memory mapping failed");
            close(client_sock);
            continue;
        }

        // Directly read data from the socket into the mapped memory region
        ssize_t bytes_received = recv(client_sock, update_data, MAX_BATCH_SIZE, 0);
        if (bytes_received == -1) {
            perror("Failed to receive data");
            munmap(update_data, MAX_BATCH_SIZE);  // Cleanup memory mapping on failure
            close(client_sock);  // Close socket on failure
            continue;
        }

        // Successfully received update data
        printf("Received node update data: %.*s\n", (int)bytes_received, update_data);

        // Further processing: append data to shared memory and set SIGNAL_READY
        int shmid = shmget(SHM_KEY, sizeof(int) + MAX_BATCH_SIZE, 0666);
        char* shm_addr = shmat(shmid, NULL, 0);
        int* signal_mem = (int*)shm_addr;

        pthread_mutex_lock(&shm_lock);

        // Append new data to shared memory
        size_t shm_data_len = strlen(shm_addr + sizeof(int));  // Existing shared memory data length
        size_t new_data_len = strlen(update_data);
        if (shm_data_len + new_data_len < MAX_BATCH_SIZE) {
            strncat(shm_addr + sizeof(int), update_data, new_data_len);  // Append data to shared memory
        }

        // Update the signal to SIGNAL_READY
        *signal_mem = SIGNAL_READY;

        pthread_mutex_unlock(&shm_lock);

        munmap(update_data, MAX_BATCH_SIZE); // Cleanup memory mapping for received data

        // Cleanup
        close(shm_fd);
        close(client_sock);
    }

    close(server_fd);
    return NULL;
}

// Function to handle the write server that receives write requests and stores as incoming_data
void* run_write_server(void* arg) {
    puts("running write server");
    int socket_desc, new_socket;
    struct sockaddr_in server, client;
    socklen_t c = sizeof(struct sockaddr_in);
    
    // Create socket
    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_desc == -1) {
        printf("Could not create socket");
        exit(1);
    }

    // Prepare the sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(WRITE_SERVER_PORT); // Change to WPORT for the write server

    // Bind
    if (bind(socket_desc, (struct sockaddr *)&server, sizeof(server)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen
    listen(socket_desc, 3);

    // Accept an incoming connection
    puts("Waiting for incoming connections on WPORT...");
    while ((new_socket = accept(socket_desc, (struct sockaddr *)&client, &c))) {
        printf("Connection accepted\n");

        // Memory mapping for zero-copy data reception
        char *incoming_data = mmap(NULL, MAX_BATCH_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (incoming_data == MAP_FAILED) {
            perror("Memory mapping failed");
            close(new_socket);
            continue;
        }

        // Directly read data from the socket into the mapped memory region
        ssize_t bytes_received = recv(new_socket, incoming_data, MAX_BATCH_SIZE, 0);
        if (bytes_received == -1) {
            perror("Failed to receive data");
            munmap(incoming_data, MAX_BATCH_SIZE);  // Cleanup memory mapping on failure
            close(new_socket);  // Close socket on failure
            continue;
        }

        // Successfully received update data
        printf("Received write update: %.*s\n", (int)bytes_received, incoming_data);

        pthread_t update_node_thread;
        pthread_create(&update_node_thread, NULL, update_other_node_memory, (void*)strdup(incoming_data));

        pthread_t mongo_thread;
        pthread_create(&mongo_thread, NULL, write_to_mongo, (void*)strdup(incoming_data));

        pthread_t shared_thread;
        pthread_create(&shared_thread, NULL, shared_memory_updater, (void*)strdup(incoming_data));

                // Detach threads for async behavior
        pthread_detach(update_node_thread);
            pthread_detach(mongo_thread);
            pthread_detach(shared_thread);

        puts("successfully gotten incoming data");

        // Cleanup
        munmap(incoming_data, MAX_BATCH_SIZE);
        close(new_socket);

    }

    close(socket_desc);
    return NULL;
}

//main
int main(int argc, char *argv[]) {
    pthread_t write_thread, receive_thread, shm_thread;
    //intialize mongo db
    init_mongo_client();

    // Create and launch thread to read data from MongoDB into shared memory
    pthread_create(&shm_thread, NULL, read_to_shared_memory, NULL);

    // Create and launch write thread (handles incoming data and MongoDB insertion)
    pthread_create(&write_thread, NULL, run_write_server, NULL);

    // Create and launch write thread (handles incoming data and MongoDB insertion)
    pthread_create(&receive_thread, NULL, node_receive_updates, NULL);

    // Join threads 
    pthread_join(write_thread, NULL);
    pthread_join(receive_thread, NULL);
    pthread_join(shm_thread, NULL);

    //shutdown mongodb
    cleanup_mongo_client();    
    return 0;
}
