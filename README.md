# Multi-threaded HTTP Server

This project is a custom multi-threaded HTTP server built from scratch in Python using low-level socket programming. It is designed to handle multiple concurrent clients, serve static and binary files, process JSON data via POST requests, and implement key security features of the HTTP protocol.

## Features

- [cite_start]**Multi-threaded Architecture**: Uses a thread pool to handle multiple client connections concurrently. [cite: 23]
- [cite_start]**HTTP/1.1 Compliant**: Supports persistent connections (`keep-alive`), GET and POST methods, and various status codes. [cite: 116, 41]
- **Static & Binary File Serving**:
    - [cite_start]Serves HTML, text, PNG, and JPEG files. [cite: 46, 51]
    - [cite_start]HTML files are rendered in the browser (`text/html`). [cite: 48]
    - [cite_start]Other file types (`.txt`, `.png`, `.jpg`) are served as `application/octet-stream` to trigger a file download. [cite: 52]
- [cite_start]**JSON API Endpoint**: Accepts POST requests with `application/json` data at the `/upload` endpoint and saves the content to a file. [cite: 78, 84]
- **Security**:
    - [cite_start]**Path Traversal Protection**: Prevents access to files outside the designated `resources` directory. [cite: 96]
    - [cite_start]**Host Header Validation**: Ensures the `Host` header matches the server's address to prevent certain types of attacks. [cite: 106]
- **Configurable & Robust**: Server host, port, and thread pool size can be configured via command-line arguments. [cite_start]Includes comprehensive logging and error handling. [cite: 11]




## How to Run the Server

### Prerequisites
- Python 3.6+

### Steps

1.  **Set up the files:**
    Ensure all files are in the structure described above. Make sure you have placed the required image and text files in the `resources` directory.

2.  **Start the Server:**
    Open a terminal in the project's root directory and run the `server.py` script.

    * [cite_start]**To run with default settings** (localhost, port 8080, 10 threads): [cite: 9, 10, 15]
        ```bash
        python server.py
        ```

    * **To run with custom settings**:
        [cite_start]The server accepts optional command-line arguments: `[port] [host] [thread_pool_size]`. [cite: 11]
        ```bash
        # Example: Run on port 8000, accessible on the local network, with 20 threads
        python server.py 8000 0.0.0.0 20
        ```

3.  **Access the Server:**
    Open your web browser and navigate to `http://127.0.0.1:8080` (or the custom host/port you specified).

## Implementation Details

### Thread Pool Architecture

[cite_start]The server uses a fixed-size thread pool to manage concurrency. [cite: 23] When the server starts, it initializes a specified number of worker threads that wait for tasks.

1.  [cite_start]**Request Queue**: A thread-safe queue is used to hold incoming client connections. [cite: 30]
2.  **Worker Threads**: When a client connects, the main server thread places the client's socket into the queue.
3.  [cite_start]**Task Distribution**: Worker threads continuously pull connections from the queue and handle the entire lifecycle of a client's request-response cycle. [cite: 25]
4.  [cite_start]**Saturation**: If all threads are busy, new connections will wait in the queue. [cite: 26] [cite_start]If the queue becomes full, the server responds with a `503 Service Unavailable` error to new connections. [cite: 167]
5.  [cite_start]**Synchronization**: Synchronization primitives like locks are used to ensure that shared resources are accessed in a thread-safe manner. [cite: 33]

### Binary File Transfer Implementation

Serving binary files correctly requires careful handling to avoid data corruption.

1.  [cite_start]**Binary Read Mode**: All files are opened and read in binary mode (`'rb'`) to preserve the raw byte data. [cite: 55, 248]
2.  [cite_start]**Content-Type**: The `Content-Type` header is set to `application/octet-stream` for all non-HTML files. [cite: 52]
3.  [cite_start]**Content-Disposition**: The `Content-Disposition: attachment; filename="..."` header is included to suggest a filename to the browser. [cite: 59]
4.  [cite_start]**Data Transmission**: The entire file content is read and sent as the body of the HTTP response. [cite: 56]

### Security Measures Implemented

1.  [cite_start]**Path Traversal Protection**: All file paths are validated to ensure they are within the designated `resources` directory. [cite: 96, 98] [cite_start]Any requests trying to access parent directories (`..`) are blocked with a `403 Forbidden` error. [cite: 100]
2.  [cite_start]**Host Header Validation**: Every request is checked for a `Host` header. [cite: 107] [cite_start]If the header is missing or does not match the server's address, the request is rejected. [cite: 112, 113]

## Known Limitations

- The server reads the entire file into memory before sending, which is inefficient for very large files.
- The thread pool size is fixed at startup.
- The server does not support HTTPS.

- HTTP request parsing is basic and may not handle all edge cases.
