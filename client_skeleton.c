#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
    printf(
        "#<user>: <msg>\t-\t Send direct P2P message (auto-saves offline if user unavailable)\n");
    printf("Or input messages sending to everyone in the chatroom via server broadcast.\n");
}

/**********************************************************/
/* Send DIR command
/**********************************************************/
int request_dir(peer_info_t peers[], int max_peers) {}

/**********************************************************
 * send_to_peer - Send a message to a peer using UDP + RDT 3.0 with fragmentation.
/**********************************************************/
int send_to_peer(const char *ip, int port, const char *message) {}

/**********************************************************
 * send_offline_to_server - Send an offline message to the server for storage.
 *
 * Constructs the internal command "OFFLINE <dest>\t<fullmsg>" and sends it
 * over the existing TCP connection (sockfd). The server will store the message
 * in the target user's mailbox file for later delivery.
/**********************************************************/
int send_offline_to_server(const char *destname, const char *fullmsg) {}

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
    char *start = buffer + 1;
    char *colon = strchr(buffer, ':');
    int i;
    bzero(destname, sizeof(destname));
    bzero(msg, sizeof(msg));
    bzero(wiremsg, sizeof(wiremsg));
    if (colon == NULL) {
        return -1;
    }
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
    char buffer[MAX];
    char register_msg[MAX];

    /**********************************************************/
    /*create peer listener and thread to handle peer messages */
    /**********************************************************/

    /**********************************************************/
    /*connect to the server */
    /**********************************************************/

    generate_menu();

    bzero(buffer, sizeof(buffer));
    if ((nbytes = recv(sockfd, buffer, sizeof(buffer), MSG_WAITALL)) == -1) {
        perror("recv");
    }
    printf("%s", buffer);
    fflush(stdout);

    /**********************************************************/
    /*input the username to register or login */
    /**********************************************************/

    bzero(buffer, sizeof(buffer));
    if (recv(sockfd, buffer, sizeof(buffer), MSG_WAITALL) == -1) {
        perror("recv");
    }
    printf("%s", buffer);
    printf("Your P2P listener is on 127.0.0.1:%d\n", peer_listen_port);
    fflush(stdout);

    /**********************************************************/
    /*create thread to handle server messages */
    /**********************************************************/

    for (;;) {
        bzero(buffer, sizeof(buffer));
        n = 0;
        while ((buffer[n++] = getchar()) != '\n');
        if ((strncmp(buffer, "EXIT", 4)) == 0) {
            /**********************************************************/
            /*handle exit command */
            /**********************************************************/

        } else if (strncmp(buffer, "DIR", 3) == 0) {
            /**********************************************************/
            /*handle DIR command */
            /**********************************************************/
            peer_info_t peers[50];
            int peer_count = request_dir(peers, 50);
            if (peer_count >= 0) {
                int k;
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
        }
    }
    return 0;
}
