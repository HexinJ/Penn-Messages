#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <ctime>
#include <sys/time.h>
#include <map>
#include <vector>
#include <algorithm>
#include <iostream>
#include <sstream>

#define UNORDERED 0
#define FIFO 1
#define TOTAL 2

#define MAX_SERVERS 10
#define MAX_CLIENT_INPUT 1000
#define MAX_ROOMS 10
#define MAX_NICKNAME_LENGTH 50
#define MAX_CLIENTS 500
#define HASH_SIZE 32

using namespace std;

char* forwarding_ips[MAX_SERVERS];
char* binding_ips[MAX_SERVERS];
int forwarding_ports[MAX_SERVERS];
int binding_ports[MAX_SERVERS];

int file_idx; // Server's index (1-indexed)
int listen_socket; // Server's UDP socket

// Client identifier
typedef struct client{
    char ip[INET_ADDRSTRLEN]; // Client's IP address
    int port = -1; // Client's port
    int room = -1; // Room that the client joined
    char nick_name[MAX_NICKNAME_LENGTH]; // Client's nickname
    int sequence_numbers[MAX_ROOMS + 1]; // Local sequence number for FIFO
}client;

// List of active clients
client active_clients[MAX_CLIENTS];

// Data structures for FIFO
typedef struct client_tuple{
    char ip[INET_ADDRSTRLEN]; // Client's IP address
    int port = -1; // Client's port
    int room = -1; // Room that the client joined
}client_tuple;

typedef struct message{
    int sequence_number;
    int group_number;
    string message;
}message;

map <client_tuple, int> FIFO_remote_sequence; // Remote clients : last seen sequence number
map <client_tuple, vector <message> > FIFO_holdback_queues; // Remote clients : messages in holdback queue

bool operator<(const client_tuple& t1, const client_tuple& t2){
    return (t1.port < t2.port);
}

bool operator==(const client_tuple& t1, const client_tuple& t2){
    return (t1.port == t2.port);
}

bool operator<(const message& t1, const message& t2){
    return (t1.sequence_number < t2.sequence_number);
}

bool operator==(const message& t1, const message& t2){
    return (t1.sequence_number == t2.sequence_number);
}

// End of data structures for FIFO

// Data structures for TOTAL

typedef struct total_m{
    int num_responses = 0;
    int sequence_responses[MAX_SERVERS]; // 0-indexed
    string message;
    int room_number;
    int final_sequence = -1;
    int final_node_id = -1;
    string message_hash; // string of timestamp, client ip, and client port
    bool deliverable = false;
}total_m;

int num_servers; // Number of servers in the server_adresses file
int highest_proposed_sequence[MAX_ROOMS + 1];
int highest_agreed_sequence[MAX_ROOMS + 1];
map<string, total_m> TOTAL_holdback_queue; // Hash string to identify message : message

bool operator<(const total_m& t1, const total_m& t2){
    return (t1.final_sequence < t2.final_sequence);
}

bool operator==(const total_m& t1, const total_m& t2){
    return (t1.final_sequence == t2.final_sequence);
}
// End of data structures for TOTAL

void SIGINT_handler(int sig){
    // Clean up
    for(int i = 0; i < MAX_SERVERS; i++){
        if(forwarding_ips[i] != NULL){
            free(forwarding_ips[i]);
            free(binding_ips[i]);
        }
    }
    close(listen_socket);
    exit(1);
}

void B_deliver(int room, const char* message){
    struct sockaddr_in recvaddr;
    bzero(&recvaddr, sizeof(recvaddr));
    recvaddr.sin_family = AF_INET;
    for(int o = 0; o < MAX_CLIENTS; o++){
        if(active_clients[o].port != -1 && active_clients[o].room == room){
            recvaddr.sin_port = htons(active_clients[o].port);
            inet_pton(AF_INET, active_clients[o].ip, &(recvaddr.sin_addr));
            sendto(listen_socket, message, strlen(message), 0, (struct sockaddr*)&recvaddr, sizeof(recvaddr));
        }
    }
}

void B_multicast(char* message){
    struct sockaddr_in recvaddr;
    bzero(&recvaddr, sizeof(recvaddr));
    recvaddr.sin_family = AF_INET;
    for(int o = 0; o < MAX_SERVERS; o++){
        if(forwarding_ips[o] != NULL && o != file_idx - 1){
            recvaddr.sin_port = htons(forwarding_ports[o]);
            inet_pton(AF_INET, forwarding_ips[o], &(recvaddr.sin_addr));
            sendto(listen_socket, message, strlen(message), 0, (struct sockaddr*)&recvaddr, sizeof(recvaddr));
        }
    }
}

void FIFO_cleanup(client_tuple source_client){
    // Clean up all data structures for this client after delivering all messages
    char *to_deliver;
    int last_seen_sequence = FIFO_remote_sequence[source_client];
    while(!FIFO_holdback_queues[source_client].empty()){
        for(auto &cur_message:FIFO_holdback_queues[source_client]){
            if(cur_message.sequence_number == last_seen_sequence){
                B_deliver(cur_message.group_number, cur_message.message.c_str());
                FIFO_holdback_queues[source_client].erase(remove(FIFO_holdback_queues[source_client].begin(), FIFO_holdback_queues[source_client].end(), cur_message), FIFO_holdback_queues[source_client].end());
            }
        }
        last_seen_sequence++;
    }

    FIFO_remote_sequence.erase(source_client);
}

int main(int argc, char *argv[])
{
    // Setting up signal handler for Ctrl+C
    signal(SIGINT, SIGINT_handler);

    // Parse command
    int opt;
    bool debug_mode = false;
    int ordering_mode = UNORDERED;
    char *input_ordering = NULL;
    
    while((opt = getopt(argc, argv, "vo:")) != -1)  
    {  
        switch(opt)  
        {
            case 'v':
                debug_mode = true;
                break;

            case 'o':
                input_ordering = optarg;
                break;

            case ':': 
                fprintf(stderr, "Option needs a value\n");
                exit(1);

            default:
                break;
        }  
    }

    // User did not pass in server address
    if(optind == argc){
        fprintf(stderr, "Hexin Jiang | bjiang1\n");
        exit(1);
    }

    // Parse ordering
    if(input_ordering != NULL){
        if(strcmp(input_ordering, "unordered") == 0){
            ordering_mode = UNORDERED;
        }
        else if(strcmp(input_ordering, "fifo") == 0){
            ordering_mode = FIFO;
        }
        else if(strcmp(input_ordering, "total") == 0){
            ordering_mode = TOTAL;
        }
    }
    
    char *file_name = argv[optind];
    file_idx = atoi(argv[optind + 1]);

    if(file_idx == 0){
        fprintf(stderr, "Bad command line arguments\n");
        exit(1);
    }

    if(debug_mode) fprintf(stderr, "[S]: Debug mode enabled. Ordering mode: %d\n", ordering_mode);

    // Parse address file (proxy or no proxy)
    bzero(forwarding_ips, sizeof(forwarding_ips));
    bzero(binding_ips, sizeof(binding_ips));

    FILE *address_file = fopen(file_name, "r");
    char *line = NULL;
    size_t length = 0;
    ssize_t read;
    int counter = 0;
    
    while((read = getline(&line, &length, address_file)) != -1){
        char *comma = strstr(line, ",");
        if(comma){
            char forwarding[comma - line];
            char binding[strlen(comma)];
            strncpy(forwarding, line, comma-line);
            strncpy(binding, comma + 1, strlen(comma)-1);

            char *colon = strstr(forwarding, ":");
            char *forwarding_ip = (char *)malloc(colon-forwarding);
            strncpy(forwarding_ip, forwarding, colon-forwarding);
            forwarding_ips[counter] = forwarding_ip;
            forwarding_ports[counter] = atoi(colon + 1);

            colon = strstr(binding, ":");
            char *binding_ip = (char *)malloc(colon-binding);
            strncpy(binding_ip, binding, colon-binding);
            binding_ips[counter] = binding_ip;
            binding_ports[counter] = atoi(colon + 1);

            if(debug_mode) fprintf(stderr, "[S]: Forwarding: %d | Binding: %d\n", forwarding_ports[counter], binding_ports[counter]);
        }
        else{
            char *colon = strstr(line, ":");
            char *ip = (char *)malloc(colon-line);
            char *ip_2 = (char *)malloc(colon-line);
            strncpy(ip, line, colon-line);
            strncpy(ip_2, line, colon-line);
            forwarding_ips[counter] = ip;
            forwarding_ports[counter] = atoi(colon + 1);
            binding_ips[counter] = ip_2;
            binding_ports[counter] = atoi(colon + 1);
        }
        counter ++;
    }

    num_servers = counter;
    fclose(address_file);
    if(line) free(line);

    // Initialize TOTAL data structures
    bzero(highest_agreed_sequence, sizeof(highest_agreed_sequence));
    bzero(highest_proposed_sequence, sizeof(highest_proposed_sequence));
    
    // Listen for all messages on the bind address, creating a socket
    listen_socket = socket(PF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in servaddr;
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(binding_ports[file_idx-1]);
    inet_pton(AF_INET, binding_ips[file_idx-1], &(servaddr.sin_addr));
    bind(listen_socket, (struct sockaddr*)&servaddr, sizeof(servaddr));

    // Receive messages
    while (true) {
        // Address of sender
        struct sockaddr_in src;
        socklen_t srclen = sizeof(src);

        // Receive messages
        char buf[MAX_CLIENT_INPUT + MAX_NICKNAME_LENGTH + 3];
        int rlen = recvfrom(listen_socket, buf, sizeof(buf)-1, 0, (struct sockaddr*)&src, &srclen);
        buf[rlen] = 0;

        // Get time for debugging
        struct timeval tv;
        time_t now = time(NULL);
        tm *utc_now = gmtime(&now);
        gettimeofday(&tv, NULL);
        
        // Determine if the message is from a client or from another server
        int src_port = ntohs(src.sin_port); // Port of the source
        char src_ip[INET_ADDRSTRLEN]; // IP of source

        inet_ntop(AF_INET, &src.sin_addr, src_ip, INET_ADDRSTRLEN);

        bool is_client = true;
        for(int i = 0; i < MAX_SERVERS; i++){
            if(forwarding_ports[i] == src_port && strcmp(forwarding_ips[i], src_ip) == 0){
                is_client = false;
                break;
            }
        }

        // Message is from client. Update client status or broadcast message to all servers
        if(is_client){
            // If client is not in the list of active clients, add the client
            // Record client index
            int client_idx = -1;
            bool new_client = true;
            for(int i = 0; i < MAX_CLIENTS; i++){
                if(active_clients[i].port == src_port && strcmp(active_clients[i].ip, src_ip) == 0){
                    new_client = false;
                    client_idx = i;
                    break;
                }
            }

            if(new_client){
                for(int i = 0; i < MAX_CLIENTS; i++){
                    if(active_clients[i].port == -1){
                        active_clients[i].port = src_port;
                        strcpy(active_clients[i].ip, src_ip);
                        client_idx = i;
                        snprintf(active_clients[i].nick_name, MAX_NICKNAME_LENGTH, "%s:%d", src_ip, src_port);
                        if(debug_mode) fprintf(stderr, "[S]: New client connection from %s:%d\n", active_clients[i].ip, active_clients[i].port);
                        break;
                    }
                }
            }

            // Check for commands
            // JOIN
            if(strncmp(buf, "/join ", 6) == 0){
                char which_room[10];
                strcpy(which_room, buf+6);
                int room = atoi(which_room);

                // Check if room is valid
                if(room > MAX_ROOMS || room <= 0){
                    if(debug_mode) fprintf(stderr, "[S]: Bad room number from client %s:%d\n", inet_ntoa(src.sin_addr), src_port);
                    sendto(listen_socket, "-ERR Bad chat room number", 25, 0, (struct sockaddr*)&src, sizeof(src));
                }
                // Check if client is already in a room
                else if(active_clients[client_idx].room != -1){
                    char to_write[100];
                    int len = snprintf(to_write, 100, "-ERR You are already in room #%d", active_clients[client_idx].room);
                    if(debug_mode) fprintf(stderr, "[S]: Client %s:%d is already in room #%d, cannot join another room\n", inet_ntoa(src.sin_addr), src_port, active_clients[client_idx].room);
                    sendto(listen_socket, to_write, len, 0, (struct sockaddr*)&src, sizeof(src));
                }
                else{
                    // Set room for client
                    active_clients[client_idx].room = room;

                    // Update data structure if FIFO
                    if(ordering_mode == FIFO){
                        active_clients[client_idx].sequence_numbers[room] = 0;
                    }
                    
                    // Write response to client
                    char to_write[100];
                    int len = snprintf(to_write, 100, "+OK You are now in room #%d", active_clients[client_idx].room);
                    if(debug_mode) fprintf(stderr, "[S]: Client %s:%d successfully joined room #%d\n", inet_ntoa(src.sin_addr), src_port, active_clients[client_idx].room);
                    sendto(listen_socket, to_write, len, 0, (struct sockaddr*)&src, sizeof(src));
                }
            }
            // PART
            else if(strcmp(buf, "/part") == 0){
                // Check if client is not in a room
                if(active_clients[client_idx].room == -1){
                    if(debug_mode) fprintf(stderr, "[S]: Client %s:%d is not in any rooms, cannot part\n", inet_ntoa(src.sin_addr), src_port);
                    sendto(listen_socket, "-ERR You are not in any chat rooms", 34, 0, (struct sockaddr*)&src, sizeof(src));
                }
                else{
                    // Write response
                    char to_write[100];
                    int len = snprintf(to_write, 100, "+OK You have left room #%d", active_clients[client_idx].room);
                    if(debug_mode) fprintf(stderr, "[S]: Client %s:%d successfully left room #%d\n", inet_ntoa(src.sin_addr), src_port, active_clients[client_idx].room);

                    // Set room = -1
                    active_clients[client_idx].room = -1;

                    sendto(listen_socket, to_write, len, 0, (struct sockaddr*)&src, sizeof(src));
                }
            }
            // NICK
            else if(strncmp(buf, "/nick ", 6) == 0){
                strcpy(active_clients[client_idx].nick_name, buf+6);
                if(debug_mode) fprintf(stderr, "[S]: Client %s:%d successfully changed nickname to '%s'\n", inet_ntoa(src.sin_addr), src_port, active_clients[client_idx].nick_name);
                char to_write[150];
                int len = snprintf(to_write, 150, "+OK Nickname set to '%s'", active_clients[client_idx].nick_name);
                sendto(listen_socket, to_write, len, 0, (struct sockaddr*)&src, sizeof(src));
            }
            // QUIT
            else if(strcmp(buf, "/quit") == 0){
                if(ordering_mode == FIFO){
                    // Clean up client's local data structures and deliver all messages
                    client current_client = active_clients[client_idx];
                    client_tuple source_client;
                    strcpy(source_client.ip, current_client.ip);
                    source_client.port = current_client.port;
                    source_client.room = current_client.room;

                    FIFO_cleanup(source_client);
                    // C_printList(FIFO_local_sequence[current_client.room]);

                    // Tell other servers to clean up after delivering all messages
                    char server_broadcast[MAX_CLIENT_INPUT + MAX_NICKNAME_LENGTH + INET_ADDRSTRLEN + 5]; // String to broadcast to other servers
                    int len = snprintf(server_broadcast, sizeof(server_broadcast), "%d|%s|%d|%d|%s", current_client.room, current_client.ip, current_client.port, -1, "FIFO CLEANUP");
                    B_multicast(server_broadcast);
                }

                // Remove client from the active_client list
                active_clients[client_idx].port = -1;
                active_clients[client_idx].room = -1;
                bzero(active_clients[client_idx].ip, INET_ADDRSTRLEN);
                bzero(active_clients[client_idx].nick_name, MAX_NICKNAME_LENGTH);
                if(debug_mode) fprintf(stderr, "[S]: Client %s:%d quits, cleaning up local client\n", inet_ntoa(src.sin_addr), src_port);
            }
            // Invalid command
            else if(strncmp(buf, "/", 1) == 0){
                sendto(listen_socket, "-ERR Invalid command", 20, 0, (struct sockaddr*)&src, sizeof(src));
            }
            // Not a command -> a message to broadcast to other servers
            else{
                // Check if the client is in a room
                if(active_clients[client_idx].room == -1){
                    if(debug_mode) fprintf(stderr, "[S]: Client %s:%d is not in any rooms, cannot broadcast message\n", inet_ntoa(src.sin_addr), src_port);
                    sendto(listen_socket, "-ERR You are not in any chat rooms, cannot broadcast message", 60, 0, (struct sockaddr*)&src, sizeof(src));
                    continue;
                }

                if(debug_mode) printf("%02d:%02d:%02d.%06ld  S%02d Client %s:%d posts '%s' to chat room #%d\n", utc_now->tm_hour, utc_now->tm_min, utc_now->tm_sec, tv.tv_usec, file_idx, active_clients[client_idx].ip, active_clients[client_idx].port, buf, active_clients[client_idx].room);

                // Broadcast message: UNORDERED
                if(ordering_mode == UNORDERED){
                    // Send all clients in the same room on this server the message
                    int room = active_clients[client_idx].room;
                    char local_broadcast[MAX_CLIENT_INPUT + MAX_NICKNAME_LENGTH]; // String to broadcast locally
                    int len = snprintf(local_broadcast, sizeof(local_broadcast), "<%s> %s", active_clients[client_idx].nick_name, buf);

                    B_deliver(room, local_broadcast);

                    // Broadcast the message to other servers
                    char server_broadcast[MAX_CLIENT_INPUT + MAX_NICKNAME_LENGTH + 3]; // String to broadcast to other servers
                    len = snprintf(server_broadcast, sizeof(server_broadcast), "%d|<%s> %s", room, active_clients[client_idx].nick_name, buf);

                    B_multicast(server_broadcast);
                }
                // Broadcast message: FIFO
                else if(ordering_mode == FIFO){
                    int room = active_clients[client_idx].room;
                    client current_client = active_clients[client_idx];

                    // Deliver the message locally to all clients in the same group
                    char local_broadcast[MAX_CLIENT_INPUT + MAX_NICKNAME_LENGTH]; // String to broadcast locally
                    int len = snprintf(local_broadcast, sizeof(local_broadcast), "<%s> %s", current_client.nick_name, buf);

                    B_deliver(room, local_broadcast);

                    // Increase local client's sequence number by 1
                    active_clients[client_idx].sequence_numbers[room] ++;
                    int incremented_sequence = active_clients[client_idx].sequence_numbers[room];

                    if(incremented_sequence == -1){
                        fprintf(stderr, "Error, client not found when incrementing serquence\n");
                        exit(1);
                    }

                    // Attach client information and sequence number to message and broadcast to other servers
                    char server_broadcast[MAX_CLIENT_INPUT + MAX_NICKNAME_LENGTH + INET_ADDRSTRLEN + 5]; // String to broadcast to other servers
                    len = snprintf(server_broadcast, sizeof(server_broadcast), "%d|%s|%d|%d|<%s> %s", room, current_client.ip, current_client.port, incremented_sequence, current_client.nick_name, buf);

                    B_multicast(server_broadcast);
                }
                // Broadcast message: TOTAL
                else if(ordering_mode == TOTAL){
                    client current_client = active_clients[client_idx];

                    // Constructing message with response of this server
                    total_m message;
                    string message_body = buf;
                    string nick_name = current_client.nick_name;
                    string full_message = "<" + nick_name + "> " + message_body;
                    message.message = full_message;
                    message.room_number = current_client.room;

                    // Incrementing counter and proposed sequence
                    message.num_responses++;
                    highest_proposed_sequence[current_client.room] = max(highest_agreed_sequence[current_client.room], highest_proposed_sequence[current_client.room]) + 1;
                    message.sequence_responses[file_idx - 1] = highest_proposed_sequence[current_client.room];
                    message.final_node_id = file_idx;
                    message.final_sequence = highest_proposed_sequence[current_client.room];

                    // Compute unique identifier of this message
                    stringstream time; 
                    time << utc_now->tm_hour << utc_now->tm_min << utc_now->tm_sec << tv.tv_usec;
                    string ip = current_client.ip;
                    string hash = time.str() + ip + to_string(current_client.port) + to_string(current_client.room);
                    message.message_hash = hash;

                    // Add this message to local holdback queue
                    TOTAL_holdback_queue[hash] = message;

                    char server_broadcast[2 * MAX_CLIENT_INPUT]; // Message to broadcast
                    // We need room number, server number, sequence number, the hash, full message body
                    int len = snprintf(server_broadcast, sizeof(server_broadcast), "FRESH MESSAGE|%d|%d|%d|%s|<%s> %s", message.room_number, file_idx, highest_proposed_sequence[current_client.room], hash.c_str(), current_client.nick_name, buf);
                    B_multicast(server_broadcast);
                }
            }         
        }
        // Message is from another server. Broadcast to all clients on this server
        else{
            // Broadcast message: UNORDERED
            if(ordering_mode == UNORDERED){
                // Prase which room to broadcast to
                char room_buf[10];
                bzero(room_buf, 10);
                int room;
                char *bar = strstr(buf, "|");
                room = atoi(strncpy(room_buf, buf, bar - buf));
                if(debug_mode) printf("%02d:%02d:%02d.%06ld  S%02d Message '%s' delivered to chat room #%d\n", utc_now->tm_hour, utc_now->tm_min, utc_now->tm_sec, tv.tv_usec, file_idx, bar+1, room);

                // Send all clients in the same room on this server the message
                B_deliver(room, bar+1);
            }
            // Broadcast message: FIFO
            else if(ordering_mode == FIFO){
                /* Parse the message:
                1. room
                2. client ip
                3. client port
                4. incremented sequence number
                */ 
                char room_buf[10];
                bzero(room_buf, 10);
                int room; // Room
                char *bar_1 = strstr(buf, "|");
                room = atoi(strncpy(room_buf, buf, bar_1 - buf));

                char client_ip[INET_ADDRSTRLEN]; // Client ip
                bzero(client_ip, INET_ADDRSTRLEN);
                char *bar_2 = strstr(bar_1 + 1, "|");
                strncpy(client_ip, bar_1 + 1, bar_2 - bar_1 - 1);

                char client_port_buf[10];
                bzero(client_port_buf, 10);
                int client_port; // Client port
                char *bar_3 = strstr(bar_2 + 1, "|");
                client_port = atoi(strncpy(client_port_buf, bar_2 + 1, bar_3 - bar_2 - 1));

                char sequence_buf[10];
                bzero(sequence_buf, 10);
                int sequence_number; // Message's sequence number
                char *bar_4 = strstr(bar_3 + 1, "|");
                sequence_number = atoi(strncpy(sequence_buf, bar_3 + 1, bar_4 - bar_3 - 1));

                // if(debug_mode) fprintf(stderr, "[S]: Received FIFO message:\nRoom: %d\nClient ip: %s\nClient port: %d\nSequence number: %d\nMessage: %s\n", room, client_ip, client_port, sequence_number, bar_4+1);

                // Construct source client
                client_tuple source_client;
                strcpy(source_client.ip, client_ip);
                source_client.port = client_port;
                source_client.room = room;

                // Check whether message is clean up
                if(sequence_number == -1 && strcmp(bar_4 + 1, "FIFO CLEANUP") == 0){
                    // Clean up the remote client's data structures and deliver all messages
                    FIFO_cleanup(source_client);
                    if(debug_mode) fprintf(stderr, "[S]: Client %s:%d quits, cleaning up remote client\n", client_ip, client_port);
                    continue;
                }

                // Update remote clients if necessary, and get client's latest delivered sequence number
                if(FIFO_remote_sequence.find(source_client) == FIFO_remote_sequence.end()){
                    FIFO_remote_sequence[source_client] = 0;
                }
                
                // Put message into holdback queue
                message to_insert;
                to_insert.group_number = room;
                to_insert.sequence_number = sequence_number;
                to_insert.message = bar_4 + 1;

                if(FIFO_holdback_queues.find(source_client) == FIFO_holdback_queues.end()){
                    vector <message> messages;
                    FIFO_holdback_queues[source_client] = messages;
                }

                FIFO_holdback_queues[source_client].push_back(to_insert);

                // Deliver all messages in holdback queue in consecutive sequential order from 
                char *to_deliver;
                while(true){
                    int last_seen_sequence = FIFO_remote_sequence[source_client];
                    bool found = false;
                    for(auto &cur_message:FIFO_holdback_queues[source_client]){
                        if(cur_message.sequence_number == last_seen_sequence + 1){
                            found = true;
                            FIFO_remote_sequence[source_client]++;
                            if(debug_mode) printf("%02d:%02d:%02d.%06ld  S%02d Message '%s' delivered to chat room #%d\n", utc_now->tm_hour, utc_now->tm_min, utc_now->tm_sec, tv.tv_usec, file_idx, cur_message.message.c_str(), source_client.room);
                            B_deliver(cur_message.group_number, cur_message.message.c_str());
                            FIFO_holdback_queues[source_client].erase(remove(FIFO_holdback_queues[source_client].begin(), FIFO_holdback_queues[source_client].end(), cur_message), FIFO_holdback_queues[source_client].end());
                            break;
                        }
                    }
                    if(found == false){
                        break;
                    }
                }
            }
            else if(ordering_mode == TOTAL){
                // Parse message, split by '|'
                string received_message = buf;
                stringstream temp(received_message);
                string segment;
                vector <string> seglist;

                while(getline(temp, segment, '|')){
                    seglist.push_back(segment);
                }

                // See if this is a message being broadcasted for the first time
                if(seglist[0] == "FRESH MESSAGE"){
                    int source_server_idx = stoi(seglist[2]);
                    // Creating the total_m struct
                    // We need room number, server number, sequence number, the hash, full message body
                    total_m message;
                    message.room_number = stoi(seglist[1]);
                    message.message_hash = seglist[4];
                    message.message = seglist[5];

                    // Calculate my new highest_proposed_sequence and update my responses
                    highest_proposed_sequence[message.room_number] = max(highest_proposed_sequence[message.room_number], highest_agreed_sequence[message.room_number]) + 1;
                    message.final_node_id = file_idx;
                    message.final_sequence = highest_proposed_sequence[message.room_number];

                    // Append the message to the holdback queue
                    TOTAL_holdback_queue[message.message_hash] = message;

                    // Respond to the source server
                    struct sockaddr_in recvaddr;
                    bzero(&recvaddr, sizeof(recvaddr));
                    recvaddr.sin_family = AF_INET;
                    recvaddr.sin_port = htons(forwarding_ports[source_server_idx-1]);
                    inet_pton(AF_INET, forwarding_ips[source_server_idx-1], &(recvaddr.sin_addr));

                    // Construct message and send
                    char response[MAX_CLIENT_INPUT]; // Response to source server
                    // We need the new proposed sequence, our server_id, and message hash

                    int len = snprintf(response, sizeof(response), "RESPONSE|%d|%d|%s", highest_proposed_sequence[message.room_number], file_idx, message.message_hash.c_str());
                    sendto(listen_socket, response, strlen(response), 0, (struct sockaddr*)&recvaddr, sizeof(recvaddr));
                }
                // Received response from initial multicast
                else if(seglist[0] == "RESPONSE"){
                    // printf("Received response: %s\n", buf);
                    // Use message hash to find message in holdback queue and update fields
                    int proposed_sequence = stoi(seglist[1]);
                    int source_server_idx = stoi(seglist[2]);
                    string message_hash = seglist[3];
                    total_m *message = &TOTAL_holdback_queue[message_hash];

                    // Store the proposed sequence numbers into the array
                    message->num_responses++;
                    message->sequence_responses[source_server_idx-1] = proposed_sequence;

                    // Check if received final response
                    if(TOTAL_holdback_queue[message_hash].num_responses == num_servers){
                        // Pick max proposed sequence and max ID
                        int max_proposed_sequence = 0;
                        int max_server_idx = 0;
                        for(int o = 0; o < MAX_SERVERS; o++){
                            if(message->sequence_responses[o] > max_proposed_sequence){
                                max_proposed_sequence = message->sequence_responses[o];
                                max_server_idx = o+1;
                            }
                            else if(message->sequence_responses[o] == max_proposed_sequence && o + 1 > max_server_idx){
                                max_server_idx = o+1;
                            }
                        }

                        // printf("Max proposed sequence: %d, max server idx: %d\n", max_proposed_sequence, max_server_idx);

                        // Update final sequence number and server idx
                        message->final_node_id = max_server_idx;
                        message->final_sequence = max_proposed_sequence;
                        message->deliverable = true;

                        // Try to deliver this message locally
                        int room = message->room_number;
                        string lowest_message_hash;

                        // Multicast this information to other servers
                        char deliver[MAX_CLIENT_INPUT]; // Tell other servers to deliver a message
                        // We need final proposed sequence number, final node id, and message hash
                        int len = snprintf(deliver, sizeof(deliver), "DELIVER|%d|%d|%s", message->final_sequence, message->final_node_id, message->message_hash.c_str());
                        B_multicast(deliver);

                        // Set highest_agreed_sequence
                        highest_agreed_sequence[room] = max(highest_agreed_sequence[room], max_proposed_sequence);

                        // Get lowest deliverable message and deliver iteratively, if any
                        while(true){
                            int lowest_deliverable_sequence = 100000;
                            int lowest_node_id = 100000;
                            for(auto &x : TOTAL_holdback_queue){
                                total_m cur_message = x.second;
                                if(cur_message.room_number != room){
                                    continue;
                                }

                                if(cur_message.final_sequence < lowest_deliverable_sequence){
                                    lowest_deliverable_sequence = cur_message.final_sequence;
                                    lowest_node_id = cur_message.final_node_id;
                                    lowest_message_hash = cur_message.message_hash;
                                }
                                else if(cur_message.final_sequence == lowest_deliverable_sequence && cur_message.final_node_id < lowest_node_id){
                                    lowest_node_id = cur_message.final_node_id;
                                    lowest_message_hash = cur_message.message_hash;
                                }
                            }

                            // Break or deliver and delete message from map
                            if(TOTAL_holdback_queue.find(lowest_message_hash) == TOTAL_holdback_queue.end()){
                                break;
                            }
                            else{
                                total_m to_deliver = TOTAL_holdback_queue[lowest_message_hash];
                                if(to_deliver.deliverable == false){
                                    break;
                                }
                                if(debug_mode) printf("%02d:%02d:%02d.%06ld  S%02d Message '%s' delivered to chat room #%d\n", utc_now->tm_hour, utc_now->tm_min, utc_now->tm_sec, tv.tv_usec, file_idx, message->message.c_str(), message->room_number);
                                B_deliver(room, to_deliver.message.c_str());
                                TOTAL_holdback_queue.erase(lowest_message_hash);
                            }
                        }
                    }
                }
                else if(seglist[0] == "DELIVER"){
                    // printf("Received deliver instruction\n");
                    int final_proposed_sequence = stoi(seglist[1]);
                    int final_node_id = stoi(seglist[2]);
                    string message_hash = seglist[3];

                    // Update the message with the final values
                    total_m *to_deliver = &TOTAL_holdback_queue[message_hash];
                    to_deliver->final_sequence = final_proposed_sequence;
                    to_deliver->final_node_id = final_node_id;
                    to_deliver->deliverable = true;
                    
                    // Get lowest deliverable message and deliver iteratively, if any
                    int room = to_deliver->room_number;
                    string lowest_message_hash;

                    // Set highest_agreed_sequence
                    highest_agreed_sequence[room] = max(highest_agreed_sequence[room], final_proposed_sequence);

                    while(true){
                        int lowest_deliverable_sequence = 100000;
                        int lowest_node_id = 100000;
                        for(auto &x : TOTAL_holdback_queue){
                            total_m cur_message = x.second;
                            if(cur_message.room_number != room){
                                continue;
                            }

                            if(cur_message.final_sequence < lowest_deliverable_sequence){
                                lowest_deliverable_sequence = cur_message.final_sequence;
                                lowest_node_id = cur_message.final_node_id;
                                lowest_message_hash = cur_message.message_hash;
                            }
                            else if(cur_message.final_sequence == lowest_deliverable_sequence && cur_message.final_node_id < lowest_node_id){
                                lowest_node_id = cur_message.final_node_id;
                                lowest_message_hash = cur_message.message_hash;
                            }
                        }

                        // Break or deliver and delete message from map
                        if(TOTAL_holdback_queue.find(lowest_message_hash) == TOTAL_holdback_queue.end()){
                            break;
                        }
                        else{
                            total_m to_deliver = TOTAL_holdback_queue[lowest_message_hash];
                            if(to_deliver.deliverable == false){
                                break;
                            }
                            if(debug_mode) printf("%02d:%02d:%02d.%06ld  S%02d Message '%s' delivered to chat room #%d\n", utc_now->tm_hour, utc_now->tm_min, utc_now->tm_sec, tv.tv_usec, file_idx, to_deliver.message.c_str(), to_deliver.room_number);
                            B_deliver(room, to_deliver.message.c_str());
                            TOTAL_holdback_queue.erase(lowest_message_hash);
                        }
                    }
                }
            }
        }
    }
    return 0;
}  