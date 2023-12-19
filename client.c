#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/time.h> // For gettimeofday for unique request ID
#include <inttypes.h> // For portable fixed size integers

#define MAX_PATH_LENGTH 256
#define ACK_TIMEOUT 5  // Timeout for ACK (in seconds)
#define MAX_PACKET_SIZE 1024

// Error handling macro for socket-related operations
#define CHECK_SOCKET_ERROR(condition, message) \
    if (condition) { \
        perror(message); \
        exit(EXIT_FAILURE); \
    }
// Function to wait for a response from the server
// Returns 0 on success (received ACK), or -1 on timeout
int waitForAck(int sock) {
    fd_set set;
    struct timeval timeout;
    int rv;
    char buffer[10];
    FD_ZERO(&set);            /* clear the set */
    FD_SET(sock, &set);       /* add our file descriptor to the set */
    timeout.tv_sec = ACK_TIMEOUT;
    timeout.tv_usec = 0;
    rv = select(sock + 1, &set, NULL, NULL, &timeout);
    if (rv == -1) {
        perror("select"); // error occurred in select()
        return -1;
    } else if (rv == 0) {
        printf("Timeout occurred! No ACK received from Naming Server\n");
        return -1; // timeout occurred
    } else {
        // one or more descriptors are readable, read the data
        if (recv(sock, buffer, sizeof(buffer), 0) > 0) {
            if (strstr(buffer, "ACK") != NULL) {
                return 0; // ACK received
            }
        }
    }
    return -1;
}

void sendFilePathToNamingServer(const char *filePath, int namingServerPort) {
    int socket_fd;
    struct sockaddr_in namingServerAddr;
    uint64_t requestId;

    // Generate a unique request ID based on the current timestamp
    struct timeval time;
    gettimeofday(&time, NULL);
    requestId = (uint64_t)(time.tv_sec) * 1000 + (time.tv_usec / 1000);

    // Create socket
    CHECK_SOCKET_ERROR((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0, "Socket creation error");

    namingServerAddr.sin_family = AF_INET;
    namingServerAddr.sin_port = htons(namingServerPort);
    if (inet_pton(AF_INET, "127.0.0.1", &namingServerAddr.sin_addr) <= 0) {
        perror("Address conversion error");
        exit(EXIT_FAILURE);
    }

    // Connect to the Naming Server
   CHECK_SOCKET_ERROR(connect(socket_fd, (struct sockaddr *)&namingServerAddr, sizeof(namingServerAddr)) < 0, "Connection to Naming Server failed");

    // Send client information header and wait for an ACK
    send(socket_fd, "CLIENT_INFO", sizeof("CLIENT_INFO"), 0);
    if (waitForAck(socket_fd) < 0) {
      // Close the socket if we don't receive an ACK
      close(socket_fd);
      exit(EXIT_FAILURE);
    }

    // Construct and send the unique request ID along with the file path
    char requestData[MAX_PATH_LENGTH + sizeof(uint64_t)];
    // Copy the file path first
    strcpy(requestData, filePath);
    // Concatenate the request ID
    sprintf(requestData + strlen(filePath) + 1, "%" PRIu64, requestId);
    // Note: Adding 1 to strlen(filePath) to ensure there is a null-terminator between file path and request ID
    send(socket_fd, requestData, strlen(filePath) + 1 + strlen(requestData + strlen(filePath) + 1), 0);

    // Receive and display the response from the Naming Server
    char response[MAX_PATH_LENGTH];
    recv(socket_fd, response, sizeof(response), 0);
    printf("Response from Naming Server: %s\n", response);

    // Check if the response contains IP address and port
    if (strstr(response, ":") != NULL) {
        // Split IP address and port
        char *ipAddress = strtok(response, ":");
        char *portStr = strtok(NULL, ":");
        int port = atoi(portStr);

        // Connect to the Storage Server
        struct sockaddr_in storageServerAddr;
        storageServerAddr.sin_family = AF_INET;
        storageServerAddr.sin_port = htons(port);
        if (inet_pton(AF_INET, ipAddress, &storageServerAddr.sin_addr) <= 0) {
            perror("Address conversion error");
            exit(EXIT_FAILURE);
        }

        int storageSocket;
        CHECK_SOCKET_ERROR((storageSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0, "Storage Socket creation error");

        if (connect(storageSocket, (struct sockaddr *)&storageServerAddr, sizeof(storageServerAddr)) < 0) {
            perror("Connection to Storage Server failed");
            exit(EXIT_FAILURE);
        }

        // Send operation message to Storage Server
        char operation[MAX_PATH_LENGTH];
        printf("Enter the operation: ");
        fgets(operation, sizeof(operation), stdin);

         // Remove the newline character at the end
        size_t len = strlen(operation);
        if (len > 0 && operation[len - 1] == '\n') {
            operation[len - 1] = '\0';
        }
       // snprintf(operation,MAX_PATH_LENGTH,"%s %s",operation,filePath);
       strcat(operation," ");
       strcat(operation,filePath);

        if(strstr(operation,"WRITE")!=NULL)
        {
            char op[1000];
            printf("Enter text to be written into desired file: ");
            fgets(op,sizeof(op),stdin);
            strcat(operation," ");
            strcat(operation,op);
        }       

        // Send operation message to Storage Server
        send(storageSocket, "CLIENT_INFO", sizeof("CLIENT_INFO"), 0);
        usleep(150);
        send(storageSocket, operation, strlen(operation), 0);

        // Wait for the storage server to send back "STOP" implying process is completed successfully
        char stopMessage[1024];
        recv(storageSocket, stopMessage, sizeof(stopMessage), 0);
        if(strstr(stopMessage,"STOP")!=NULL)
        {
            stopMessage[strlen(stopMessage)-4]='\0';
            printf("Operation completed: %s\n", stopMessage);
        }
        else
        {
            printf("Operation unsuccessful\n");
        }    

        // Close the storage server socket
        close(storageSocket);
    }

    // Close the socket
    close(socket_fd);
}

int main() {
    char filePath[MAX_PATH_LENGTH];

    // Get the file path from the user
    printf("Enter the file path: ");
    fgets(filePath, sizeof(filePath), stdin);

    // Remove the newline character at the end
    size_t len = strlen(filePath);
    if (len > 0 && filePath[len - 1] == '\n') {
        filePath[len - 1] = '\0';
    }

    int namingServerPort = 8888;  // Assuming the naming server is running on port 8888
    sendFilePathToNamingServer(filePath, namingServerPort);

    return 0;
}