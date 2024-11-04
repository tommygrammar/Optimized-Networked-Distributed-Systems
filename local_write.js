const net = require('net');

// Data to be sent in the write request
const postData = JSON.stringify({
    name: 'The problem is with the shared memory updater',
    value: 'problem is with the shared memory updater'
});

// Connect to the server using TCP
const client = new net.Socket();

// Define the server and port you want to connect to
const serverHost = 'localhost';
const serverPort = 6437;

// Start timer for execution time
console.time('Execution Time');

// Connect to the server
client.connect(serverPort, serverHost, () => {
    console.log('Connected to server');

    // Send data once connected
    client.write(postData);
});

// Handle data received from the server
client.on('data', (data) => {
    console.log('Received:', data.toString());
    // Close the connection after receiving data
    client.end();
});

// Handle connection close
client.on('close', () => {
    console.log('Connection closed');
    console.timeEnd('Execution Time');
});

// Handle any errors
client.on('error', (err) => {
    console.error(`Error occurred: ${err.message}`);
});
