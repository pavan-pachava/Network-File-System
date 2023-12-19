//ss3
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_PATH_LENGTH 512
#define MAX_PATH_ENTRIES 150
#define MAX_BUFFER_SIZE 1024

struct StorageServerInfo {
    int serverID;
    int port;
    int clientPort;
    char accessiblePath[MAX_PATH_LENGTH];
    char accessiblePathEntries[MAX_PATH_ENTRIES][MAX_PATH_LENGTH];
};

// Function to read the content of a file
int readFile(const char *file_path, char *file_content) {
    int fd = open(file_path, O_RDONLY);
    if (fd == -1) {
        perror("Error opening file");
        return -1;  // Return -1 to indicate failure
    }

    ssize_t bytesRead = read(fd, file_content, MAX_BUFFER_SIZE - 1);
    close(fd);

    if (bytesRead == -1) {
        perror("Error reading file");
        return -1;  // Return -1 to indicate failure
    }

    file_content[bytesRead] = '\0';  // Null-terminate the content
    return bytesRead;  // Return the number of bytes read
}

// Function to write content to a file
int writeFile(const char *file_path, const char *file_content) {
    int fd = open(file_path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
        perror("Error opening file");
        return -1;  // Return -1 to indicate failure
    }

    ssize_t bytesWritten = write(fd, file_content, strlen(file_content));
    close(fd);

    if (bytesWritten == -1) {
        perror("Error writing to file");
        return -1;  // Return -1 to indicate failure
    }

    return bytesWritten;  // Return the number of bytes written
}

void handleGetInfo(int clientSocket, const char *clientRequest) {
    // Extract the file path from the client request (assuming the request format is "GET_INFO filepath/file")
    const char *filePath = strchr(clientRequest, ' ') + 1;

    // Use stat to retrieve information about the file
    struct stat fileStat;
    if (stat(filePath, &fileStat) == 0) {
        // File information retrieved successfully

        // Construct a response message with file information
        char responseMessage[256];
        snprintf(responseMessage, sizeof(responseMessage),
                 "File Information: Size=%ld bytes, Permissions=%o", fileStat.st_size, fileStat.st_mode & 0777);
      //  printf("yo\n");
        // Send the response back to the client
        send(clientSocket, responseMessage, strlen(responseMessage), 0);
        // Send back "STOP" to indicate the operation is done successfully
        send(clientSocket, "STOP", sizeof("STOP"), 0);
    } else {
        // Error retrieving file information
      //  printf("oy\n");
        perror("Error in retrieving file information");

        // Send an error response back to the client
        const char *errorResponse = "Error retrieving file information.";
        send(clientSocket, errorResponse, strlen(errorResponse), 0);
    }
}
// Function to handle client requests
void handleClient(int clientSocket) {
    char buffer[MAX_BUFFER_SIZE];
    ssize_t bytesRead;

    // Receive "CLIENT_INFO" message from the client
    bytesRead = recv(clientSocket, buffer, sizeof(buffer), 0);
    if (bytesRead <= 0) {
        perror("Error receiving CLIENT_INFO message from client");
        close(clientSocket);
        return;
    }

    buffer[bytesRead] = '\0';
    // if (strcmp(buffer, "CLIENT_INFO") != 0) {
    //     fprintf(stderr, "Invalid message from client: %s\n", buffer);
    //     close(clientSocket);
    //     return;
    // }

    // Send an acknowledgment to the client
    //send(clientSocket, "ACK", sizeof("ACK"), 0);

    // Receive the actual client message
    // bytesRead = recv(clientSocket, buffer, sizeof(buffer), 0);
    // if (bytesRead <= 0) {
    //     perror("Error receiving client message");
    //     close(clientSocket);
    //     return;
    // }

  //  buffer[bytesRead] = '\0';

    // // Parse the client message (e.g., "OPERATION(READ/WRITE/GET_FILE_INFO) filePath")
    // char operation[32];
    // char filePath[MAX_PATH_LENGTH];
    // if (sscanf(buffer, "%31s %511s", operation, filePath) != 2) {
    //     fprintf(stderr, "Invalid client message format: %s\n", buffer);
    //     close(clientSocket);
    //     return;
    // }
    printf("BUFFER:%s\n",buffer);
    // Perform file operation based on the client request
    if (strstr(buffer, "READ")!=NULL) {
        // Handle READ operation
        // Implement your logic here
        const char *filePath = buffer + strlen("READ") + 1;
        char file_content[MAX_BUFFER_SIZE];
        int bytes_read = readFile(filePath, file_content);

        if (bytes_read != -1) {
            // Send the file content back to the client
            send(clientSocket, file_content, bytes_read, 0);
            // Send back "STOP" to indicate the operation is done successfully
        send(clientSocket, "STOP", sizeof("STOP"), 0);
        }
        else
        {
            send(clientSocket,"OPERATION UNSUCCESSFUL", sizeof("OPERATION UNSUCCESSFUL"), 0);
        }
        
    } else if (strstr(buffer, "GET_FILE_INFO")!=NULL) {
        // Handle WRITE operation
        // Implement your logic here
      //  printf("nawassoo\n");
        handleGetInfo(clientSocket, buffer);
        
    } else if (strstr(buffer, "WRITE") !=NULL) {
        // Handle GET_FILE_INFO operation
        // Implement your logic here
        // Extract the file path and content from the request
        const char *filePath = strtok(buffer + strlen("WRITE") + 1, " ");
        const char *file_content = strtok(NULL, "");
      //  printf("WRITE OPERATIONNNNNNNNNN!!!!!!!!1\n");
        // Write the content to the file
        printf("%s\n",file_content);
        printf("%s\n",filePath);
        int bytes_written = writeFile(filePath, file_content);

        if (bytes_written != -1) {
            // Send an acknowledgment back to the client
            send(clientSocket, "WRITE_SUCCESS", strlen("WRITE_SUCCESS"), 0);
            // Send back "STOP" to indicate the operation is done successfully
        send(clientSocket, "STOP", sizeof("STOP"), 0);
        }
         else
        {
            send(clientSocket,"OPERATION UNSUCCESSFUL", sizeof("OPERATION UNSUCCESSFUL"), 0);
        }
        
    } else {
        fprintf(stderr, "Unsupported operation: %s\n", buffer);
        // Send an error message back to the client
        send(clientSocket, "ERROR", sizeof("ERROR"), 0);
    }

    close(clientSocket);
}

void sendDetailsToNamingServer(const struct StorageServerInfo *details, int namingServerPort) {
    int socket_fd;
    struct sockaddr_in namingServerAddr;

    // Create socket
    if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation error");
        exit(EXIT_FAILURE);
    }

    namingServerAddr.sin_family = AF_INET;
    namingServerAddr.sin_port = htons(namingServerPort);
    if (inet_pton(AF_INET, "127.0.0.1", &namingServerAddr.sin_addr) <= 0) {
        perror("Address conversion error");
        exit(EXIT_FAILURE);
    }

    // Connect to the Naming Server
    if (connect(socket_fd, (struct sockaddr *)&namingServerAddr, sizeof(namingServerAddr)) < 0) {
        perror("Connection to Naming Server failed");
        exit(EXIT_FAILURE);
    }

    // Send server details
    send(socket_fd, "STORAGE_SERVER_INFO", sizeof("STORAGE_SERVER_INFO"), 0);
    usleep(200);
    send(socket_fd, details, sizeof(struct StorageServerInfo), 0);
   // usleep(200);
    
    for (int i = 0; i < MAX_PATH_ENTRIES; ++i) {
    send(socket_fd, details->accessiblePathEntries[i], sizeof(details->accessiblePathEntries[i]), 0);
    usleep(200);
}
    // Receive and display the response from the Naming Server
    char response[1024];
    recv(socket_fd, response, sizeof(response), 0);
    printf("Response from Naming Server: %s\n", response);

    close(socket_fd);
}


// Function to accept and handle client connections
void acceptAndHandleClients(int serverSocket) {
    struct sockaddr_in clientAddr;
    socklen_t clientAddrLen = sizeof(clientAddr);

    while (1) {
        int clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddr, &clientAddrLen);
        if (clientSocket < 0) {
            perror("Error accepting client connection");
            continue;
        }

        // Receive identification message from the connected entity
        char identification[32];
       // char buffer[MAX_BUFFER_SIZE];
        ssize_t bytesRead = recv(clientSocket, identification, sizeof(identification), 0);
        if (bytesRead <= 0) {
            perror("Error receiving identification message");
            close(clientSocket);
            continue;
        }


        identification[bytesRead] = '\0';

        // Branch into appropriate logic based on the identification message
        if (strcmp(identification, "CLIENT_INFO") == 0) {
            // Handle client logic
            printf("CLIENT HEREEEEEEEEEEE!!!!!!!!!!!\n");
            handleClient(clientSocket);
        } else if (strcmp(identification, "NAMING_SERVER_INFO") == 0) {
            // Handle naming server logic
            // Add logic for handling messages from the naming server if needed
        } else {
            fprintf(stderr, "Unknown entity: %s\n", identification);
            close(clientSocket);
        }
    }
}


void initializeStorageServer(int serverID, int port, const char *accessiblePath, int namingServerPort, int clientPort) {
    printf("Storage Server %d initialized with Port %d, Accessible Path %s, and Client Port %d\n", serverID, port, accessiblePath, clientPort);

    // List accessible paths
    DIR *dir;
    struct dirent *entry;

    char accessiblePathEntries[MAX_PATH_ENTRIES][MAX_PATH_LENGTH];
    int entryCount = 0;

    // Open the directory
    if ((dir = opendir(accessiblePath)) != NULL) {
        // Read entries in the directory
        while ((entry = readdir(dir)) != NULL && entryCount < MAX_PATH_ENTRIES) {
            if (entry->d_type == DT_REG || entry->d_type == DT_DIR) {
                // Print the accessible path
                snprintf(accessiblePathEntries[entryCount], MAX_PATH_LENGTH, "%s/%s", accessiblePath, entry->d_name);
                printf("%s\n", accessiblePathEntries[entryCount]);
                entryCount++;
            }
        }
        strcpy(accessiblePathEntries[entryCount++],"0");
        // Close the directory
        closedir(dir);
    } else {
        perror("Unable to open directory");
        exit(EXIT_FAILURE);
    }

    // Send details to Naming Server
    struct StorageServerInfo details;
    details.serverID = serverID;
    details.clientPort = clientPort;
    details.port = port;
    strncpy(details.accessiblePath, accessiblePath, sizeof(details.accessiblePath));
    memcpy(details.accessiblePathEntries, accessiblePathEntries, sizeof(details.accessiblePathEntries));

    sendDetailsToNamingServer(&details, namingServerPort);

    // Initialize a socket to accept client connections
    int serverSocket;
    struct sockaddr_in serverAddr;

    if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket creation error");
        exit(EXIT_FAILURE);
    }

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(clientPort);

    if (bind(serverSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Bind error");
        exit(EXIT_FAILURE);
    }

    if (listen(serverSocket, 10) < 0) {
        perror("Listen error");
        exit(EXIT_FAILURE);
    }

    printf("Storage Server listening on port %d for client connections...\n", clientPort);

    // Accept and handle client connections
    acceptAndHandleClients(serverSocket);

    // Close the server socket (this will not be reached in this example)
    close(serverSocket);
}


int main() {
    int serverID = 3;
    int port = 5003;
    int clientPort = 6003;
    const char *accessiblePath = ".";
    int namingServerPort = 8888;

    initializeStorageServer(serverID, port, accessiblePath, namingServerPort, clientPort);

    return 0;
}
