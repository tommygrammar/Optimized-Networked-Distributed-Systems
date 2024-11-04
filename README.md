# Distributed Networking System

- This is a distributed systems project where I was enhancing data access and transmission speeds using memory optimizations and zero copy networking techniques.

## Optimization Results
- Within Local nodes, reads are able to happen at over 1000x the speed of normal mongodb reads.
- Data Transmission in the networks is 18x the speed of normal mongodb reads.

## Features
- Written in C language
- MongoDB
- Shared Memory
- Zero Copy techniques
- Multi-Threading


## How it Works
- When first ran, it loads mongo db instance data into its local shared memory, and then reads are able to happen from its shared memory faster, at more than 30x the speed of using normal mongodb queries.

- When it receives a write request to mongodb, what happens is that in parallel using multithreading, it is able to update its own local shared memory, write to mongodb and as a client send the latest write update to other nodes so that they can update their shared memories at more than 18x the speed.

- This ensures that data is always up to date in the fastest ways possible on all nodes.

- It also has a receiving updates server where it is able to also receive updates from other nodes to update its shared memory.

## How to setup
- Before all this, kindly make sure you have all the mongoc library.

- Compile node-1.c
```gcc -o node-1 node-1.c  -Ofast $(pkg-config --cflags --libs libmongoc-1.0 libbson-1.0) -lpthread -lrt
```
- Compile read.c
```gcc -o read.c -Ofast
```

- For a nodejs text write client:
``` node local_write.js
```

- To add more nodes, follow the instructions in the node-1.c on what exactly the nodes do and how to make sure they work. 

## Contributions
- To Contribute, fork the repository and make your modifications.

