#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>

#define MAX_STDIN_INPUT 1000

int sock; // Client's UDP socket
struct sockaddr_in dest; // Server's address

void SIGINT_handler(int sig){
    // Send quit to server
    sendto(sock, "/quit", 5, 0, (struct sockaddr*)&dest, sizeof(dest));
    // Clean up
    close(sock);
    exit(1);
}

int main(int argc, char *argv[])
{
    // Setting up signal handler for Ctrl+C
    signal(SIGINT, SIGINT_handler);

    // Parse command
    int opt;
    bool debug_mode = false;
    char *server_address;
    int server_port;

    while((opt = getopt(argc, argv, "v")) != -1)  
    {  
        switch(opt)  
        {
            case 'v':
                debug_mode = true;

            default:
                break;
        }  
    }

    // User did not pass in server address
    if(optind == argc){
        fprintf(stderr, "Hexin Jiang | bjiang1\n");
        exit(1);
    }

    // Parse server address and port
    server_address = argv[optind];
    char *colon = strstr(server_address, ":");
    char server_ip[colon - server_address + 1];
    bzero(server_ip, sizeof(server_ip));
    strncpy(server_ip, server_address, colon-server_address);
    server_port = atoi(colon + 1);

    if(debug_mode) printf("Server address: %s\nDebug_mode: %d\nServer ip: %s\nServer port: %d\n", server_address, debug_mode, server_ip, server_port);

    // Creating the datagram socket
    sock = socket(PF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        fprintf(stderr, "Cannot open socket (%s)\n", strerror(errno));
        exit(1);
    }

    // Destination for writing to server
    bzero(&dest, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &(dest.sin_addr));

    // Source for reading from server
    struct sockaddr_in src;
    socklen_t srclen = sizeof(src);

    // Prep for select(): create a fd_set
    fd_set read_fds, current_fds;
    FD_ZERO(&read_fds);
    FD_SET(STDIN_FILENO, &read_fds);
    FD_SET(sock, &read_fds);

    // Listen from the command line and server using select()
    while(true){
        current_fds = read_fds;
        if (select(sock + 1, &current_fds, NULL, NULL, NULL) <= 0){
            fprintf(stderr, "Select returned an error, errno: %d\n", errno);
            exit(1);
        }

        for(int i = 0; i < sock + 1; i++){
            if(FD_ISSET(i, &current_fds) == 0){
                continue;
            }

            // Stdin available to read
            if(i == STDIN_FILENO){
                // Reading input into a buffer
                char stdin_buffer[MAX_STDIN_INPUT];
                bzero(stdin_buffer, MAX_STDIN_INPUT);
                read(i, stdin_buffer, MAX_STDIN_INPUT);
                if(debug_mode) printf("Stdin input: %s\n", stdin_buffer);

                // Sending message to server
                sendto(sock, stdin_buffer, strlen(stdin_buffer) - 1, 0, (struct sockaddr*)&dest, sizeof(dest));

                // Check if /quit
                if(strcmp(stdin_buffer, "/quit\n") == 0){
                    close(sock);
                    exit(0);
                }

            }
            // Server socket available to read
            else if(i == sock){
                char buf[MAX_STDIN_INPUT];
                struct sockaddr_in src;
                socklen_t srcSize = sizeof(src);
                int rlen = recvfrom(sock, buf, sizeof(buf), 0, (struct sockaddr*)&src, &srcSize);
                buf[rlen] = 0;
                printf("%s\n", buf);
            }
        }
    }

    return 0;
}  