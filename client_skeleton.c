#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "chatroom.h"

#define MAX 8192   // max buffer size
#define PORT 6789  // port number

static int sockfd;
static int peer_listener_fd;
static int peer_listen_port;
static char username[C_NAME_LEN + 1];
static pthread_t recv_server_msg_thread;
static pthread_t recv_peer_msg_thread;
static pthread_mutex_t server_resp_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t server_resp_cv = PTHREAD_COND_INITIALIZER;
static int server_connected = 1;
static int waiting_dir = 0;
static int dir_ready = 0;
static char dir_response[MAX];
static int waiting_offline = 0;
static int offline_ready = 0;
static char offline_response[MAX];

typedef struct peer_info {
    char username[C_NAME_LEN + 1];
    char status[8]; /* "online" or "offline" */
    char ip[INET_ADDRSTRLEN];
    int port;
} peer_info_t;

/* RDT 3.0 protocol constants */
#define RDT_TIMEOUT_SEC 0
#define RDT_TIMEOUT_USEC 500000
#define RDT_MAX_RETRIES 10
#define RDT_DATA_SIZE 1024
#define RDT_REASM_MAX (MAX * 64) /* max reassembly buffer: 64KB */

void generate_menu()
{
    printf("Hello dear user pls select one of the following options:\n");
    printf("EXIT\t-\t Send exit message to server - unregister ourselves from server\n");
    printf("DIR\t-\t Get all registered users with online/offline status\n");
    printf("#<user>: <msg>\t-\t Send direct P2P message (auto-saves offline if user unavailable)\n");
    printf("Or input messages sending to everyone in the chatroom via server broadcast.\n");
}

static int is_dir_payload(const char *msg)
{
    return strncmp(msg, "=== User List ===", 17) == 0;
}

static int is_offline_response(const char *msg)
{
    return strncmp(msg, "Message saved for ", 18) == 0 ||
           strncmp(msg, "Failed to store offline message.", 32) == 0 ||
           strncmp(msg, "Destination user is not registered.", 35) == 0;
}

static void *server_message_thread(void *arg)
{
    char buffer[MAX + 1];
    ssize_t nbytes;
    (void)arg;

    for (;;) {
        int handled = 0;
        memset(buffer, 0, sizeof(buffer));
        nbytes = recv(sockfd, buffer, MAX, 0);
        if (nbytes <= 0) {
            pthread_mutex_lock(&server_resp_mutex);
            server_connected = 0;
            pthread_cond_broadcast(&server_resp_cv);
            pthread_mutex_unlock(&server_resp_mutex);
            break;
        }
        buffer[nbytes] = '\0';

        pthread_mutex_lock(&server_resp_mutex);
        if (waiting_dir && is_dir_payload(buffer)) {
            strncpy(dir_response, buffer, sizeof(dir_response) - 1);
            dir_response[sizeof(dir_response) - 1] = '\0';
            dir_ready = 1;
            waiting_dir = 0;
            pthread_cond_broadcast(&server_resp_cv);
            handled = 1;
        } else if (waiting_offline && is_offline_response(buffer)) {
            strncpy(offline_response, buffer, sizeof(offline_response) - 1);
            offline_response[sizeof(offline_response) - 1] = '\0';
            offline_ready = 1;
            waiting_offline = 0;
            pthread_cond_broadcast(&server_resp_cv);
            handled = 1;
        }
        pthread_mutex_unlock(&server_resp_mutex);

        if (!handled) {
            printf("%s", buffer);
        }
        fflush(stdout);
    }

    return NULL;
}

static void *peer_message_thread(void *arg)
{
    char packet[RDT_DATA_SIZE + 128];
    char ackbuf[64];
    char reasm[RDT_REASM_MAX + 1];
    struct sockaddr_in sender_addr;
    socklen_t sender_len = sizeof(sender_addr);
    ssize_t nbytes;
    size_t reasm_len = 0;
    int expected_seq = 0;
    (void)arg;

    memset(reasm, 0, sizeof(reasm));

    for (;;) {
        int seq = -1;
        int is_last = 0;
        size_t chunk_length = 0;
        int header_length = 0;

        memset(packet, 0, sizeof(packet));
        memset(&sender_addr, 0, sizeof(sender_addr));
        sender_len = sizeof(sender_addr);

        nbytes = recvfrom(peer_listener_fd, packet, sizeof(packet) - 1, 0,
                          (struct sockaddr *)&sender_addr, &sender_len);
        if (nbytes < 0) {
            if (errno == EINTR) continue;   // just an interrupted syscall, continue for now
            perror("recvfrom peer");
            break;
        }

        packet[nbytes] = '\0';
        if (sscanf(packet, "RDT3|%d|%d|%zu|%n", &seq, &is_last, &chunk_length, &header_length) != 3) continue;

        snprintf(ackbuf, sizeof(ackbuf), "ACK|%d", seq);
        sendto(peer_listener_fd, ackbuf, strlen(ackbuf), 0,
               (struct sockaddr *)&sender_addr, sender_len);

        if (seq != expected_seq) continue;  // wrong ACK
        if ((size_t) header_length > (size_t) nbytes) continue;
        if (chunk_length > (size_t)nbytes - (size_t)header_length) {
            chunk_length = (size_t)nbytes - (size_t)header_length;    // adjust chunk_len if sender was wrong?
        }
        if (reasm_len + chunk_length >= sizeof(reasm)) {
            // reassembly buffer overflow, gg, just reset
            reasm_len = 0;
            expected_seq = 0;
            continue;
        }

        memcpy(reasm + reasm_len, packet + header_length, chunk_length);
        reasm_len += chunk_length; // shift reassembly buffer end forward
        if (is_last) {
            reasm[reasm_len] = '\0';
            printf("%s\n", reasm);
            fflush(stdout);
            memset(reasm, 0, sizeof(reasm));
            reasm_len = 0;
            expected_seq = 0;
        } else {
            expected_seq++;
        }
    }

    return NULL;
}

/**********************************************************/
/* Send DIR command
/**********************************************************/
int request_dir(peer_info_t peers[], int max_peers) {
    char buffer[MAX + 1];
    int peer_count = 0;

    pthread_mutex_lock(&server_resp_mutex);
    waiting_dir = 1;
    dir_ready = 0;
    dir_response[0] = '\0';
    pthread_mutex_unlock(&server_resp_mutex);

    if (send(sockfd, "DIR", 3, 0) == -1) {
        perror("send DIR");
        pthread_mutex_lock(&server_resp_mutex);
        waiting_dir = 0;
        pthread_mutex_unlock(&server_resp_mutex);
        return -1;
    }

    pthread_mutex_lock(&server_resp_mutex);
    while (!dir_ready && server_connected) {
        pthread_cond_wait(&server_resp_cv, &server_resp_mutex);
    }
    if (!dir_ready) {
        waiting_dir = 0;
        pthread_mutex_unlock(&server_resp_mutex);
        return -1;
    }
    strncpy(buffer, dir_response, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';
    dir_ready = 0;
    pthread_mutex_unlock(&server_resp_mutex);

    char *line = strtok(buffer, "\n");
    while (line != NULL && peer_count < max_peers) {
        if (sscanf(line, "%24s %7s %15s %d", peers[peer_count].username,
                   peers[peer_count].status, peers[peer_count].ip,
                   &peers[peer_count].port) == 4) {
            // successfully parsed a peer line with all 4 fields
            peer_count++;
        // } else if (sscanf(line, "%24s [%7[^]]", peers[peer_count].username,
        //                   peers[peer_count].status) == 2) {
        //     // alternative parsing for previous server versions that only sends "username [status]"
        //     strncpy(peers[peer_count].ip, "127.0.0.1", sizeof(peers[peer_count].ip) - 1);
        //     peers[peer_count].ip[sizeof(peers[peer_count].ip) - 1] = '\0';
        //     peers[peer_count].port = 0;
        //     peer_count++;
        }
        line = strtok(NULL, "\n");
    }
    return peer_count;
}

/**********************************************************
 * send_to_peer - Send a message to a peer using UDP + RDT 3.0 with fragmentation.
/**********************************************************/
int send_to_peer(const char *ip, int port, const char *message) {
    int udpfd;
    struct sockaddr_in peer_addr;
    char packet[RDT_DATA_SIZE + 128];
    char ackbuf[128];
    size_t msg_length;
    size_t offset = 0;
    int seq = 0;

    if (ip == NULL || message == NULL) {
        char buffer[256];
        snprintf(buffer, sizeof(buffer), "invalid arguments to send_to_peer: ip: %s, port: %d", ip, port);
        perror(buffer);
        return -1;  // invalid inputs check
    }

    udpfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpfd < 0) {
        perror("socket UDP");
        return -1;
    }

    memset(&peer_addr, 0, sizeof(peer_addr));
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &peer_addr.sin_addr) != 1) {
        perror("inet_pton");
        close(udpfd);
        return -1;
    }

    msg_length = strlen(message);
    while (offset < msg_length) {
        size_t chunk_length = msg_length - offset;
        int is_last;
        int sent_ok = 0;
        int retry;
        if (chunk_length > RDT_DATA_SIZE) {
            chunk_length = RDT_DATA_SIZE;   // max chunk size for RDT packet
        }
        is_last = (offset + chunk_length >= msg_length) ? 1 : 0;

        for (retry = 0; retry < RDT_MAX_RETRIES; retry++) {
            struct timeval tv;
            ssize_t sent_bytes;
            ssize_t recv_bytes;
            int ack_seq = -1;

            memset(packet, 0, sizeof(packet));
            snprintf(packet, sizeof(packet), "RDT3|%d|%d|%zu|", seq, is_last, chunk_length);
            {
                // construct & send packet with header + chunk
                size_t header_length = strlen(packet);
                if (header_length + chunk_length >= sizeof(packet)) {
                    close(udpfd);
                    perror("message chunk too long for RDT packet? Messed up somehow");
                    return -1;
                }
                memcpy(packet + header_length, message + offset, chunk_length);
                sent_bytes = sendto(udpfd, packet, header_length + chunk_length, 0,
                                    (struct sockaddr *)&peer_addr, sizeof(peer_addr));
            }

            if (sent_bytes < 0) {
                perror("sendto");
                close(udpfd);
                return -1;
            }

            tv.tv_sec = RDT_TIMEOUT_SEC;
            tv.tv_usec = RDT_TIMEOUT_USEC;
            if (setsockopt(udpfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
                perror("setsockopt");
                close(udpfd);
                return -1;
            }

            memset(ackbuf, 0, sizeof(ackbuf));
            recv_bytes = recvfrom(udpfd, ackbuf, sizeof(ackbuf) - 1, 0, NULL, NULL);
            if (recv_bytes < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    continue;   // retry
                }
                perror("recvfrom");
                close(udpfd);
                return -1;
            }

            if (sscanf(ackbuf, "ACK|%d", &ack_seq) == 1 && ack_seq == seq) {
                // Got the ACK
                sent_ok = 1;
                break;
            }
        }

        if (!sent_ok) {
            close(udpfd);
            perror("failed to send message after retries");
            return 1;
        }

        offset += chunk_length;
        seq += 1;
    }

    close(udpfd);
    return 0;
}

/**********************************************************
 * send_offline_to_server - Send an offline message to the server for storage.
 *
 * Constructs the internal command "OFFLINE <dest>\t<fullmsg>" and sends it
 * over the existing TCP connection (sockfd). The server will store the message
 * in the target user's mailbox file for later delivery.
/**********************************************************/
int send_offline_to_server(const char *destname, const char *fullmsg) {
    char buffer[MAX];

    if (destname == NULL || fullmsg == NULL || destname[0] == '\0') {
        return -1;
    }

    memset(buffer, 0, sizeof(buffer));
    snprintf(buffer, sizeof(buffer), "OFFLINE %s\t%s", destname, fullmsg);

    pthread_mutex_lock(&server_resp_mutex);
    waiting_offline = 1;
    offline_ready = 0;
    offline_response[0] = '\0';
    pthread_mutex_unlock(&server_resp_mutex);

    if (send(sockfd, buffer, strlen(buffer), 0) == -1) {
        perror("send OFFLINE");
        pthread_mutex_lock(&server_resp_mutex);
        waiting_offline = 0;
        pthread_mutex_unlock(&server_resp_mutex);
        return -1;
    }

    pthread_mutex_lock(&server_resp_mutex);
    while (!offline_ready && server_connected) {
        pthread_cond_wait(&server_resp_cv, &server_resp_mutex);
    }
    if (!offline_ready) {
        waiting_offline = 0;
        pthread_mutex_unlock(&server_resp_mutex);
        return -1;
    }
    strncpy(buffer, offline_response, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';
    offline_ready = 0;
    pthread_mutex_unlock(&server_resp_mutex);

    printf("%s", buffer);
    fflush(stdout);
    return 0;
}

/**********************************************************
 * send_direct_p2p - Handle direct messaging with offline fallback.
 *
 * Flow:
 *   1. Parse "#<user>: <msg>" from buffer
 *   2. Construct wire message: "<sender> to you: <msg>"
 *   3. DIR query to check target status
 *   4. If target offline  → send_offline_to_server() immediately
 *   5. If target online   → try send_to_peer()
 *      - Returns 0  → success
 *      - Returns 1  → ACK timeout, auto-fallback to offline storage
 *      - Returns -1 → local error, just report failure
 *   6. If target not found → "not registered"
 *
 * Returns: 0 on success (P2P or offline stored), -1 on failure.
/**********************************************************/
int send_direct_p2p(char *buffer)
{
    peer_info_t peers[50];
    int peer_count;
    char destname[C_NAME_LEN + 1];
    char msg[MAX];
    char wiremsg[MAX];
    char *start = buffer + 1;   // skip leading '#'
    char *colon = strchr(buffer, ':');
    int i;
    bzero(destname, sizeof(destname));
    bzero(msg, sizeof(msg));
    bzero(wiremsg, sizeof(wiremsg));
    if (colon == NULL) {
        perror("invalid direct message format");
        return -1;
    }
    // Build sender name & message parts
    {
        int namelen = colon - start;
        strncpy(destname, start, namelen);
        destname[namelen] = '\0';
    }
    strcpy(msg, colon + 1);
    if (msg[0] == ' ') memmove(msg, msg + 1, sizeof(msg));

    /* Build the full display message: "<sender> to you: <msg>" */
    snprintf(wiremsg, sizeof(wiremsg), "%s to you: %s", username, msg);
    peer_count = request_dir(peers, 50);
    if (peer_count >= 0) {
        for (i = 0; i < peer_count; i++) {
            if (strcmp(peers[i].username, destname) == 0) {
                if (strcmp(peers[i].status, "online") != 0) {
                    /* Target is offline — store via server */
                    printf("User %s is offline. Saving offline message...\n", destname);
                    if (send_offline_to_server(destname, wiremsg) == 0) {
                        return 0;
                    }
                    printf("Failed to save offline message for %s.\n", destname);
                    return -1;
                }

                /* Target is online — try P2P first */
                int p2p_result = send_to_peer(peers[i].ip, peers[i].port, wiremsg);
                if (p2p_result == 0) {
                    return 0;
                }
                if (p2p_result == 1) {
                    /* ACK timeout — target unreachable, fallback to offline storage */
                    printf("P2P delivery failed. Saving offline message for %s...\n", destname);
                    if (send_offline_to_server(destname, wiremsg) == 0) {
                        return 0;
                    }
                    printf("Failed to save offline message for %s.\n", destname);
                    return -1;
                }
                /* p2p_result == -1: local error, no fallback */
                printf("Failed to send to %s (local error).\n", destname);
                // perror("send_to_peer");
                return -1;
            }
        }
    }
    printf("User %s is not registered.\n", destname);
    return -1;
}

int main()
{
    int n;
    int nbytes;
    struct sockaddr_in server_addr;
    struct sockaddr_in peer_addr;
    char buffer[MAX];
    char register_msg[MAX];
    char welcome[MAX];
    char username_input[C_NAME_LEN + 1];
    socklen_t peer_addr_len;

    /**********************************************************/
    /*create peer listener and thread to handle peer messages */
    /**********************************************************/
    peer_listener_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (peer_listener_fd < 0) {
        perror("socket peer listener");
        return 1;
    }
    memset(&peer_addr, 0, sizeof(peer_addr));
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    peer_addr.sin_port = htons(0);
    if (bind(peer_listener_fd, (struct sockaddr *)&peer_addr, sizeof(peer_addr)) < 0) {
        perror("bind peer listener");
        close(peer_listener_fd);
        return 1;
    }
    peer_addr_len = sizeof(peer_addr);
    if (getsockname(peer_listener_fd, (struct sockaddr *)&peer_addr, &peer_addr_len) < 0) {
        perror("getsockname peer listener");
        close(peer_listener_fd);
        return 1;
    }
    peer_listen_port = ntohs(peer_addr.sin_port);
    if (pthread_create(&recv_peer_msg_thread, NULL, peer_message_thread, NULL) != 0) {
        perror("pthread_create peer");
        close(peer_listener_fd);
        return 1;
    }

    /**********************************************************/
    /*connect to the server */
    /**********************************************************/
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket server");
        return 1;
    }
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr) != 1) {
        perror("inet_pton server");
        close(sockfd);
        return 1;
    }
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect server");
        close(sockfd);
        return 1;
    }

    generate_menu();

    bzero(buffer, sizeof(buffer));
    if ((nbytes = recv(sockfd, buffer, sizeof(buffer) - 1, 0)) == -1) {
        perror("recv");
    } else if (nbytes >= 0) {
        buffer[nbytes] = '\0';
    }
    printf("%s", buffer);
    fflush(stdout);

    /**********************************************************/
    /*input the username to register or login */
    /**********************************************************/
    memset(username_input, 0, sizeof(username_input));
    if (fgets(username_input, sizeof(username_input), stdin) == NULL) {
        fprintf(stderr, "failed to read username\n");
        close(sockfd);
        close(peer_listener_fd);
        return 1;
    }
    username_input[strcspn(username_input, "\n")] = '\0';
    strncpy(username, username_input, sizeof(username) - 1);
    username[sizeof(username) - 1] = '\0';

    snprintf(register_msg, sizeof(register_msg), "REGISTER %s 127.0.0.1 %d", username,
             peer_listen_port);
    if (send(sockfd, register_msg, strlen(register_msg), 0) < 0) {
        perror("send REGISTER");
        close(sockfd);
        close(peer_listener_fd);
        return 1;
    }

    bzero(buffer, sizeof(buffer));
    if ((nbytes = recv(sockfd, buffer, sizeof(buffer) - 1, 0)) == -1) {
        perror("recv");
    } else if (nbytes >= 0) {
        buffer[nbytes] = '\0';
    }
    printf("%s", buffer);
    printf("Your P2P listener is on 127.0.0.1:%d\n", peer_listen_port);
    fflush(stdout);

    /**********************************************************/
    /*create thread to handle server messages */
    /**********************************************************/
    if (pthread_create(&recv_server_msg_thread, NULL, server_message_thread, NULL) != 0) {
        perror("pthread_create server");
        close(sockfd);
        close(peer_listener_fd);
        return 1;
    }

    for (;;) {
        bzero(buffer, sizeof(buffer));
        n = 0;
        while ((buffer[n++] = getchar()) != '\n');
        if ((strncmp(buffer, "EXIT", 4)) == 0) {
            /**********************************************************/
            /*handle exit command */
            /**********************************************************/
            if (send(sockfd, "EXIT", 4, 0) < 0) {
                perror("send EXIT");
            }
            break;

        } else if (strncmp(buffer, "DIR", 3) == 0) {
            /**********************************************************/
            /*handle DIR command */
            /**********************************************************/
            peer_info_t peers[50];
            int peer_count = request_dir(peers, 50);
            if (peer_count >= 0) {
                int k;
                // Relocated display formatting to here instead of inside request_dir
                printf("=== User List ===\n");
                for (k = 0; k < peer_count; k++) {
                    printf("%-24s [%s]\n", peers[k].username, peers[k].status);
                }
                printf("=================\n");
            } else {
                printf("Failed to get user list.\n");
            }
        } else if (strncmp(buffer, "#", 1) == 0) {
            if (send_direct_p2p(buffer) < 0) {
                printf("Sending direct message failed...\n");
            }
        } else {
            /**********************************************************/
            /*handle broadcast messages */
            /**********************************************************/
            if (send(sockfd, buffer, strlen(buffer), 0) == -1) {
                perror("send broadcast");
            }
        }
    }

    pthread_cancel(recv_server_msg_thread);
    pthread_cancel(recv_peer_msg_thread);
    close(sockfd);
    close(peer_listener_fd);
    return 0;
}
