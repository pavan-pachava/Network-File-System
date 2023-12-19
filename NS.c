#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <time.h>
#include <stdbool.h>

#define MAX_STORAGE_SERVERS 10
#define MAX_PATH_LENGTH 256
#define MAX_PATH_ENTRIES 150
#define MAX_ACCEPTED_CONNECTIONS 10
#define HEARTBEAT_INTERVAL 5 // in seconds
#define TIMEOUT_THRESHOLD 15  // in seconds


// Log levels
enum LogLevel {
    INFO,
    WARNING,
    ERROR
};

// Log entry structure
struct LogEntry {
    time_t timestamp;
    enum LogLevel level;
    char message[256];
};

struct StorageServerStatus {
    int serverID;
    time_t lastHeartbeatTime;
    bool isFailed;
};

struct StorageServerStatus storageServerStatus[MAX_STORAGE_SERVERS];
int numRegisteredStorageServers = 0;

void updateHeartbeat(int serverID) {
    struct StorageServerStatus *status = NULL;
    for (int i = 0; i < numRegisteredStorageServers; ++i) {
        if (storageServerStatus[i].serverID == serverID) {
            status = &storageServerStatus[i];
            break;
        }
    }

    if (status != NULL) {
        status->lastHeartbeatTime = time(NULL);
    }
}

// Function to convert log level to string
const char *levelToString(enum LogLevel level) {
    switch (level) {
        case INFO:
            return "INFO";
        case WARNING:
            return "WARNING";
        case ERROR:
            return "ERROR";
        default:
            return "UNKNOWN";
    }
}

// Log entries array
struct LogEntry logEntries[1000];  // Adjust the size as needed
int numLogEntries = 0;

struct StorageServerInfo {
    int serverID;
    int port;
    int clientPort;
    char accessiblePath[MAX_PATH_LENGTH];
    char accessiblePathEntries[MAX_PATH_ENTRIES][MAX_PATH_LENGTH];
};

struct StorageServerInfo storageServerInfo[MAX_STORAGE_SERVERS];
int numAcceptedConnections = 0;

// Error handling macro for socket-related operations
#define CHECK_SOCKET_ERROR(condition, message) \
    if (condition) { \
        perror(message); \
        logEntry(ERROR, message); \
        exit(EXIT_FAILURE); \
    }

// Function to log an entry
void logEntry(enum LogLevel level, const char *message) {
    // Create a log entry
    struct LogEntry entry;
    entry.timestamp = time(NULL);
    entry.level = level;
    snprintf(entry.message, sizeof(entry.message), "%s", message);

    // Add the log entry to the array
    logEntries[numLogEntries++] = entry;

    // Open the log file in append mode
    FILE *logFile = fopen("nfs_log.txt", "a");
    if (logFile != NULL) {
        // Write the log entry to the log file
        fprintf(logFile, "[%ld] %s: %s\n", entry.timestamp, levelToString(level), entry.message);

        // Close the log file
        fclose(logFile);
    }
}

void registerStorageServer(const struct StorageServerInfo *serverDetails) {
    if (numRegisteredStorageServers < MAX_STORAGE_SERVERS) {
        storageServerInfo[numRegisteredStorageServers] = *serverDetails;
        printf("Storage Server %d registered with Port %d, Client Port %d, and Accessible Path %s\n",
               serverDetails->serverID, serverDetails->port, serverDetails->clientPort, serverDetails->accessiblePath);
        logEntry(INFO, "Storage Server registered");
        // Display accessible paths of the registered storage server
        // printf("Accessible Paths on Storage Server %d:\n", serverDetails->serverID);
        // for (int i = 0; i < MAX_PATH_ENTRIES && serverDetails->accessiblePathEntries[i][0] != '\0'; ++i) {
        //     printf("%s\n", serverDetails->accessiblePathEntries[i]);
        // }

        numRegisteredStorageServers++;
    } else {
        printf("Cannot register more storage servers. Maximum limit reached.\n");
                logEntry(WARNING, "Cannot register more storage servers. Maximum limit reached.");
    }
}

void handleStorageServerInfo(int clientSocket) {
    struct StorageServerInfo serverDetails;

    // Receive server details
    recv(clientSocket, &serverDetails, sizeof(serverDetails), 0);
    usleep(200);



    // Receive the accessiblePathEntries separately
    for (int i = 0; i < MAX_PATH_ENTRIES; ++i) {
        recv(clientSocket, serverDetails.accessiblePathEntries[i], sizeof(serverDetails.accessiblePathEntries[i]), 0);
        usleep(200);
    }

    storageServerStatus[numRegisteredStorageServers].serverID = serverDetails.serverID;
    storageServerStatus[numRegisteredStorageServers].lastHeartbeatTime = time(NULL);
    storageServerStatus[numRegisteredStorageServers].isFailed = false;

    // Register the storage server
    registerStorageServer(&serverDetails);

    // Send a response back to the storage server
    const char *response = "Server information received!";
    send(clientSocket, response, strlen(response), 0);
}

void handleClientInfo(int clientSocket,struct sockaddr_in clientAddr) {
    char filePath[MAX_PATH_LENGTH];
    recv(clientSocket, filePath, sizeof(filePath), 0);
    printf("filePath:%s\n",filePath);
   // printf("%d")
    // Check which storage server has the accessible path containing the specified file
    int storageServerIndex = -1;
    for (int i = 0; i < numRegisteredStorageServers; ++i) {
       // for (int j = 0; j < MAX_PATH_ENTRIES && storageServerInfo[i].accessiblePathEntries[j][0] != '\0'; ++j) {
           // printf("%s:%s\n",filePath,storageServerInfo[i].accessiblePath);
            if (strstr(filePath,storageServerInfo[i].accessiblePath) != NULL) {
                storageServerIndex = i;
                break;
            }
        //}
        if (storageServerIndex != -1) {
            break;
        }
    }

    // Send the IP address and port of the storage server back to the client
    if (storageServerIndex != -1) {
        char response[MAX_PATH_LENGTH];
        snprintf(response, sizeof(response), "%s:%d",
                 inet_ntoa(clientAddr.sin_addr), storageServerInfo[storageServerIndex].clientPort);
        send(clientSocket, response, strlen(response), 0);
    } else {
        // If the file is not found in any accessible path, send an appropriate response
        const char *response = "File not found in any accessible path.";
        send(clientSocket, response, strlen(response), 0);
    }
}

void handleFailure(int serverID) {
    logEntry(ERROR, "Storage server is offline");
}


void checkForFailures() {
    time_t currentTime = time(NULL);

    for (int i = 0; i < numRegisteredStorageServers; ++i) {
        struct StorageServerStatus *status = &storageServerStatus[i];

        if (status->isFailed) {
            // Implement recovery mechanism if needed
            // For simplicity, this example does not include a recovery mechanism
        } else {
            if (currentTime - status->lastHeartbeatTime > TIMEOUT_THRESHOLD) {
                status->isFailed = true;
                handleFailure(status->serverID);
            }
        }
    }
}

void initializeNamingServer() {
    int serverSocket;
    struct sockaddr_in serverAddr, clientAddr;
    socklen_t addrLen = sizeof(clientAddr);

    // Create socket
    CHECK_SOCKET_ERROR((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0, "Socket creation error");

    logEntry(INFO, "Naming Server socket created");

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(8888);  // Naming Server listening port
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    // Bind the socket
    CHECK_SOCKET_ERROR(bind(serverSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0, "Binding error");
    logEntry(INFO, "Naming Server socket bound");

    // Listen for incoming connections
    CHECK_SOCKET_ERROR(listen(serverSocket, MAX_ACCEPTED_CONNECTIONS) < 0, "Listening error");
    logEntry(INFO, "Naming Server listening for connections");

    while (1) {
        // Accept incoming connections

        checkForFailures();
        int clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddr, &addrLen);

        if (clientSocket > 0) {
            printf("Connection accepted from %s:%d\n", inet_ntoa(clientAddr.sin_addr), ntohs(clientAddr.sin_port));

            // Receive message header to determine type
            char header[25];  // Adjust the size as needed
            recv(clientSocket, header, sizeof(header), 0);

            if (strcmp(header, "STORAGE_SERVER_INFO") == 0) {
                // It's server information
                handleStorageServerInfo(clientSocket);
            } else if((strcmp(header, "CLIENT_INFO") == 0)) {
                // Handle other cases as needed
                handleClientInfo(clientSocket,clientAddr);
            }

            // Close the client socket
            close(clientSocket);
        }

        sleep(HEARTBEAT_INTERVAL);

    }

    // Close the server socket (Note: This part will not be reached in this example)
    close(serverSocket);
}

void displayStorageServers() {
    printf("Accessible Paths on Naming Server:\n");
    for (int i = 0; i < numRegisteredStorageServers; ++i) {
        printf("Storage Server %d: Port %d, Client Port %d, Accessible Path %s\n",
               storageServerInfo[i].serverID, storageServerInfo[i].port, storageServerInfo[i].clientPort, storageServerInfo[i].accessiblePath);
    }
}

int main() {
    initializeNamingServer();
    // Note: This part will not be reached in this example
    displayStorageServers();
    return 0;
}