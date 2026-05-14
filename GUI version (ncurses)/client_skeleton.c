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
#include <ncurses.h>
#include <time.h>

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

/* Message queue for thread-safe UI updates */
#define MSG_QUEUE_SIZE 256
typedef struct {
    char messages[MSG_QUEUE_SIZE][MAX];
    int head;
    int tail;
    pthread_mutex_t lock;
} msg_queue_t;

static msg_queue_t msg_queue = {{}, 0, 0, PTHREAD_MUTEX_INITIALIZER};

/* ncurses window pointers */
WINDOW *win_friends;
WINDOW *win_messages;
WINDOW *win_input;
int max_x, max_y;

/* Friend list cache for display */
static peer_info_t cached_peers[50];
static int cached_peer_count = 0;
static time_t last_dir_refresh = 0;
static pthread_mutex_t peer_cache_lock = PTHREAD_MUTEX_INITIALIZER;

static void ui_print_message(const char *msg)
{
    pthread_mutex_lock(&msg_queue.lock);
    int next_head = (msg_queue.head + 1) % MSG_QUEUE_SIZE;
    if (next_head != msg_queue.tail) {
        strncpy(msg_queue.messages[msg_queue.head], msg, MAX - 1);
        msg_queue.messages[msg_queue.head][MAX - 1] = '\0';
        msg_queue.head = next_head;
    }
    pthread_mutex_unlock(&msg_queue.lock);
}

static void ui_init_ncurses(void)
{
    initscr();
    cbreak();
    noecho();
    nodelay(stdscr, TRUE);
    getmaxyx(stdscr, max_y, max_x);

    /* Create three windows: friends (left), messages (main), input (bottom) */
    win_friends = newwin(max_y - 3, 25, 0, 0);
    win_messages = newwin(max_y - 3, max_x - 25, 0, 25);
    win_input = newwin(3, max_x, max_y - 3, 0);

    box(win_friends, 0, 0);
    box(win_messages, 0, 0);
    box(win_input, 0, 0);

    waddstr(win_friends, "=== Online Friends ===\n");
    waddstr(win_messages, "=== Messages ===\n");
    waddstr(win_input, "Input: ");

    wrefresh(win_friends);
    wrefresh(win_messages);
    wrefresh(win_input);
}

static void ui_cleanup_ncurses(void)
{
    if (win_friends) delwin(win_friends);
    if (win_messages) delwin(win_messages);
    if (win_input) delwin(win_input);
    endwin();
}

static void ui_draw_messages(void)
{
    wclear(win_messages);
    box(win_messages, 0, 0);
    waddstr(win_messages, "=== Messages ===\n");

    pthread_mutex_lock(&msg_queue.lock);
    int idx = msg_queue.tail;
    int row = 2;
    while (idx != msg_queue.head && row < max_y - 5) {
        mvwaddstr(win_messages, row++, 1, msg_queue.messages[idx]);
        idx = (idx + 1) % MSG_QUEUE_SIZE;
    }
    pthread_mutex_unlock(&msg_queue.lock);

    wrefresh(win_messages);
}

static void ui_draw_friends(void)
{
    wclear(win_friends);
    box(win_friends, 0, 0);
    mvwaddstr(win_friends, 0, 1, "=== Online ===");

    pthread_mutex_lock(&peer_cache_lock);
    int row = 2;
    for (int i = 0; i < cached_peer_count && row < max_y - 3; i++) {
        if (strcmp(cached_peers[i].status, "online") == 0) {
            mvwaddstr(win_friends, row++, 1, cached_peers[i].username);
        }
    }
    pthread_mutex_unlock(&peer_cache_lock);

    wrefresh(win_friends);
}

static void ui_refresh_if_needed(void)
{
    time_t now = time(NULL);
    if (now - last_dir_refresh > 3) {
        peer_info_t peers[50];
        int peer_count = request_dir(peers, 50);
        if (peer_count >= 0) {
            pthread_mutex_lock(&peer_cache_lock);
            cached_peer_count = peer_count;
            memcpy(cached_peers, peers, peer_count * sizeof(peer_info_t));
            pthread_mutex_unlock(&peer_cache_lock);
            last_dir_refresh = now;
        }
    }
}

void generate_menu(void)
{
    /* Menu is now displayed via ncurses in UI */
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
            ui_print_message(buffer);
        }
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
            ui_print_message(reasm);
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

    ui_print_message(buffer);
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
                    char msg[128];
                    snprintf(msg, sizeof(msg), "User %s is offline. Saving offline message...", destname);
                    ui_print_message(msg);
                    if (send_offline_to_server(destname, wiremsg) == 0) {
                        return 0;
                    }
                    snprintf(msg, sizeof(msg), "Failed to save offline message for %s.", destname);
                    ui_print_message(msg);
                    return -1;
                }

                /* Target is online — try P2P first */
                int p2p_result = send_to_peer(peers[i].ip, peers[i].port, wiremsg);
                if (p2p_result == 0) {
                    return 0;
                }
                if (p2p_result == 1) {
                    /* ACK timeout — target unreachable, fallback to offline storage */
                    char msg[128];
                    snprintf(msg, sizeof(msg), "P2P delivery failed. Saving offline message for %s...", destname);
                    ui_print_message(msg);
                    if (send_offline_to_server(destname, wiremsg) == 0) {
                        return 0;
                    }
                    snprintf(msg, sizeof(msg), "Failed to save offline message for %s.", destname);
                    ui_print_message(msg);
                    return -1;
                }
                /* p2p_result == -1: local error, no fallback */
                printf("Failed to send to %s (local error).\n", destname);
                // perror("send_to_peer");
                return -1;
            }
        }
    }
    char not_found_msg[64];
    snprintf(not_found_msg, sizeof(not_found_msg), "User %s is not registered.", destname);
    ui_print_message(not_found_msg);
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

    /**********************************************************/
    /*initialize ncurses UI */
    /**********************************************************/
    ui_init_ncurses();
    ui_print_message("Chat room connected. Commands: DIR, #user: msg, or broadcast");

    for (;;) {
        int ch = wgetch(win_input);
        
        /* Refresh UI periodically */
        ui_refresh_if_needed();
        ui_draw_messages();
        ui_draw_friends();

        if (ch == ERR) {
            /* No input available, just refresh */
            usleep(50000);
            continue;
        }

        if (ch == '\n' || ch == '\r') {
            /* Process the command in buffer */
            if (strlen(buffer) == 0) {
                /* Empty line, just redraw */
                wmove(win_input, 1, 7);
                wclrtoeol(win_input);
                waddstr(win_input, "Input: ");
                wrefresh(win_input);
                continue;
            }

            if (strncmp(buffer, "EXIT", 4) == 0) {
                if (send(sockfd, "EXIT", 4, 0) < 0) {
                    perror("send EXIT");
                }
                break;
            } else if (strncmp(buffer, "DIR", 3) == 0) {
                peer_info_t peers[50];
                int peer_count = request_dir(peers, 50);
                if (peer_count >= 0) {
                    pthread_mutex_lock(&peer_cache_lock);
                    cached_peer_count = peer_count;
                    memcpy(cached_peers, peers, peer_count * sizeof(peer_info_t));
                    last_dir_refresh = time(NULL);
                    pthread_mutex_unlock(&peer_cache_lock);
                    ui_draw_friends();
                } else {
                    ui_print_message("Failed to get user list.");
                }
            } else if (strncmp(buffer, "#", 1) == 0) {
                ui_print_message(buffer);
                if (send_direct_p2p(buffer) < 0) {
                    ui_print_message("Sending direct message failed...");
                }
            } else if (strlen(buffer) > 0) {
                /* Broadcast message */
                ui_print_message(buffer);
                if (send(sockfd, buffer, strlen(buffer), 0) == -1) {
                    perror("send broadcast");
                }
            }

            /* Clear input buffer and redraw input pane */
            bzero(buffer, sizeof(buffer));
            wclear(win_input);
            box(win_input, 0, 0);
            waddstr(win_input, "Input: ");
            wrefresh(win_input);
            n = 0;
        } else if (ch == KEY_BACKSPACE || ch == 127) {
            /* Handle backspace */
            if (n > 0) {
                buffer[--n] = '\0';
                wmove(win_input, 1, 7 + n);
                wdelch(win_input);
            }
        } else if (ch >= 32 && ch < 127 && n < MAX - 1) {
            /* Regular character */
            buffer[n++] = ch;
            buffer[n] = '\0';
            waddch(win_input, ch);
            wrefresh(win_input);
        }
    }

    pthread_cancel(recv_server_msg_thread);
    pthread_cancel(recv_peer_msg_thread);
    ui_cleanup_ncurses();
    close(sockfd);
    close(peer_listener_fd);
    return 0;
}
