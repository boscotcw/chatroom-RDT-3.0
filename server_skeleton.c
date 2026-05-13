#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "chatroom.h"

#define MAX 8192                      // max buffer size
#define PORT 6789                     // server port number
#define MAX_USERS 50                  // max number of users
#define LISTEN_BACKLOG 10             // backlog for listen()
static unsigned int users_count = 0;  // number of registered users

static user_info_t *listOfUsers[MAX_USERS] = {0};  // list of users

/* Add user to userList */
void user_add(user_info_t *user);
/* Get user name from userList */
char *get_username(int sockfd);
/* Get user sockfd by name */
int get_sockfd(char *name);

/* Add user to userList */
void user_add(user_info_t *user)
{
    if (users_count == MAX_USERS) {
        printf("sorry the system is full, please try again later\n");
        return;
    }
    /***************************/
    /* add the user to the list */
    /**************************/
    listOfUsers[users_count] = user;
    users_count++;
    printf("Client %s successfully registered.\n", listOfUsers[users_count - 1]->username);
}

/* Determine whether the user has been registered  */
int isNewUser(char *name)
{
    int i;
    int flag = -1;
    /*******************************************/
    /* Compare the name with existing usernames */
    /*******************************************/
    for (i = 0; i < users_count; i++) {
        if (strcmp(listOfUsers[i]->username, name) == 0) {
            flag = i;
            break;
        }
    }

    return flag;
}

/* Get user name from userList */
char *get_username(int ss)
{
    int i;
    static char uname[MAX];
    /* Return the username only if the socket matches an online user.
     * This avoids returning stale names when file descriptor numbers are
     * recycled by the OS after sockets are closed. If not found, return
     * an empty string. */
    uname[0] = '\0';
    for (i = 0; i < users_count; i++) {
        if (listOfUsers[i]->sockfd == ss) {
            strcpy(uname, listOfUsers[i]->username);
            break;
        }
    }
    return uname;
}

/* Get user sockfd by name */
int get_sockfd(char *name)
{
    int i;
    int sock;
    /*******************************************/
    /* Get the user sockfd by the user name */
    /*******************************************/
    sock = -1;
    for (i = 0; i < users_count; i++) {
        if (strcmp(listOfUsers[i]->username, name) == 0) {
            sock = listOfUsers[i]->sockfd;
            break;
        }
    }

    return sock;
}
// The following two functions are defined for poll()
// Add a new file descriptor to the set
void add_to_pfds(struct pollfd *pfds[], int newfd, int *fd_count, int *fd_size)
{
    // If we don't have room, add more space in the pfds array
    if (*fd_count == *fd_size) {
        *fd_size *= 2;  // Double it

        *pfds = realloc(*pfds, sizeof(**pfds) * (*fd_size));
    }

    (*pfds)[*fd_count].fd = newfd;
    (*pfds)[*fd_count].events = POLLIN;  // Check ready-to-read

    (*fd_count)++;
}
// Remove an index from the set
void del_from_pfds(struct pollfd pfds[], int i, int *fd_count)
{
    // Copy the one from the end over this one
    pfds[i] = pfds[*fd_count - 1];

    (*fd_count)--;
}


/*
 * user_exists - Check whether a user is registered in the system.
 *
 * Returns the index in listOfUsers if found, -1 otherwise.
 * Reuses the isNewUser() logic but with a clearer name for mailbox context.
 */
int user_exists(const char *name) { return isNewUser((char *)name); }

static int send_text(int fd, const char *text)
{
    char out[MAX];
    bzero(out, sizeof(out));
    snprintf(out, sizeof(out), "%s", text == NULL ? "" : text);
    return send(fd, out, sizeof(out), 0);
}

static void mailbox_path(const char *username, char *path, size_t path_size)
{
    snprintf(path, path_size, "mailbox_%s.txt", username);
}

static int append_offline_message(const char *dest, const char *message)
{
    char path[128];
    FILE *fp;

    mailbox_path(dest, path, sizeof(path));
    fp = fopen(path, "a");
    if (fp == NULL) {
        return -1;
    }
    fprintf(fp, "%s\n", message);
    fclose(fp);
    return 0;
}

static void deliver_offline_messages(user_info_t *user)
{
    char path[128];
    char line[MAX];
    char prefixed_msg[MAX]; 
    FILE *fp;
    FILE *truncate_fp;

    mailbox_path(user->username, path, sizeof(path));
    fp = fopen(path, "r");
    if (fp == NULL) {
        return;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        memset(prefixed_msg, 0, sizeof(prefixed_msg));
        snprintf(prefixed_msg, sizeof(prefixed_msg), "[Offline] %s", line);
        send_text(user->sockfd, prefixed_msg);
    }
    fclose(fp);

    truncate_fp = fopen(path, "w");
    if (truncate_fp != NULL) {
        fclose(truncate_fp);
    }
}

static void broadcast_to_online(int exclude_fd, const char *msg)
{
    unsigned int idx;
    for (idx = 0; idx < users_count; idx++) {
        if (listOfUsers[idx]->state == 1 && listOfUsers[idx]->sockfd != exclude_fd) {
            send_text(listOfUsers[idx]->sockfd, msg);
        }
    }
}




/* ==================== End Offline Message Box Helpers ===================== */

int main()
{
    int listener;   // listening socket descriptor
    int newfd;      // newly accept()ed socket descriptor
    int addr_size;  // length of client addr
    struct sockaddr_in server_addr, client_addr;

    char buffer[MAX];  // buffer for client data
    int nbytes;
    int fd_count = 0;
    int fd_size = 5;
    struct pollfd *pfds = malloc(sizeof *pfds * fd_size);

    int yes = 1;  // for setsockopt() SO_REUSEADDR, below
    int i, j, u;

    /**********************************************************/
    /*create the listener socket and bind it with server_addr*/
    /**********************************************************/
    listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        perror("socket");
        return 1;
    }
    if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
        perror("setsockopt");
        return 1;
    }
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);
    if (bind(listener, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        return 1;
    }
    

    /**********************************************************/
    /*start listening for connections */
    /**********************************************************/
    if (listen(listener, LISTEN_BACKLOG) < 0) {
        perror("listen");
        return 1;
    }

    add_to_pfds(&pfds, listener, &fd_count, &fd_size);

    // main loop
    for (;;) {
        /***************************************/
        /* use poll function */
        /**************************************/

        if (poll(pfds, fd_count, -1) == -1) {
            perror("poll");
            continue;
        }

        // run through the existing connections looking for data to read
        for (i = 0; i < fd_count; i++) {
            if (pfds[i].revents & POLLIN) {  // we got one!!
                if (pfds[i].fd == listener) {
                    /**************************/
                    /* It is the listener and needs to handle new connections from clients */
                    /****************************/
                    addr_size = sizeof(client_addr);
                    newfd = accept(listener, (struct sockaddr *)&client_addr, &addr_size);
                    if (newfd == -1) {
                        perror("accept");
                        continue;
                    }
                    add_to_pfds(&pfds, newfd, &fd_count, &fd_size);
                    send_text(newfd, "Please input your username for registration/login:\n");
                } else {
                    // handle data from a client
                    bzero(buffer, sizeof(buffer));
                    if ((nbytes = recv(pfds[i].fd, buffer, sizeof(buffer), 0)) <= 0) {
                        // got error or connection closed by client
                        if (nbytes == 0) {
                            // connection closed
                            printf("pollserver: socket %d hung up\n", pfds[i].fd);
                        } else {
                            perror("recv");
                        }
                        for (u = 0; u < users_count; u++) {
                            if (listOfUsers[u]->sockfd == pfds[i].fd) {
                                listOfUsers[u]->state = 0;
                                listOfUsers[u]->sockfd = -1; /* clear stored fd to avoid future false matches */
                                break;
                            }
                        }
                        close(pfds[i].fd);  // Bye!
                        del_from_pfds(pfds, i, &fd_count);
                        i--;    // so that we don't skip the fd we just moved to replace this deleted fd
                    } else {
                        // we got some data from a client
                        if (strncmp(buffer, "REGISTER", 8) == 0) {
                            /********************************/
                            /* Get the user name and add the user to the userlist*/
                            /**********************************/
                            char name[C_NAME_LEN + 1];
                            char peer_ip[INET_ADDRSTRLEN];
                            int peer_port = 0;
                            bzero(name, sizeof(name));
                            bzero(peer_ip, sizeof(peer_ip));
                            if (sscanf(buffer, "REGISTER %24s %15s %d", name, peer_ip, &peer_port) <
                                1) {
                                continue;   // invalid registration message, ignore
                            }
                            if (peer_ip[0] == '\0') {
                                strcpy(peer_ip, "127.0.0.1");
                            }
                            if (isNewUser(name) == -1) {
                                /********************************/
                                /* it is a new user and we need to handle the registration*/
                                /**********************************/
                                user_info_t *new_user = malloc(sizeof(user_info_t));
                                bzero(new_user, sizeof(user_info_t));
                                strcpy(new_user->username, name);
                                strcpy(new_user->ip, peer_ip);
                                new_user->port = peer_port;
                                new_user->sockfd = pfds[i].fd;
                                new_user->state = 1;
                                user_add(new_user);

                                /*****************************/
                                /* Broadcast the welcome message*/
                                /*****************************/
                                bzero(buffer, sizeof(buffer));
                                snprintf(buffer, sizeof(buffer), "Welcome %s to join the chat room!\n", name);
                                broadcast_to_online(pfds[i].fd, buffer);

                                /*****************************/
                                /* send registration success message to the new user*/
                                /*****************************/
                                bzero(buffer, sizeof(buffer));
                                snprintf(buffer, sizeof(buffer), "Welcome %s! A new account has been created.\n", name);
                                send_text(pfds[i].fd, buffer);

                            } else {
                                /********************************/
                                /* it's an existing user and we need to handle the login. Note the
                                 * state of user,*/
                                /**********************************/
                                int idx = isNewUser(name);
                                listOfUsers[idx]->sockfd = pfds[i].fd;
                                listOfUsers[idx]->state = 1;
                                strcpy(listOfUsers[idx]->ip, peer_ip);
                                listOfUsers[idx]->port = peer_port;

                                /*****************************/
                                /* Broadcast the welcome message 
                                /* and Deliver any stored offline messages after re-login
                                /*****************************/
                                bzero(buffer, sizeof(buffer));
                                snprintf(buffer, sizeof(buffer), "Welcome %s to join the chat room!\n", name);
                                broadcast_to_online(pfds[i].fd, buffer);
                                send_text(pfds[i].fd, "Welcome back!\n");
                                deliver_offline_messages(listOfUsers[idx]);
                            }
                        } else if (strncmp(buffer, "OFFLINE", 7) == 0) {
                            char dest[C_NAME_LEN + 1];
                            char msg[MAX];
                            int idx;

                            bzero(dest, sizeof(dest));
                            bzero(msg, sizeof(msg));
                            if (sscanf(buffer, "OFFLINE %24s\t%[^\n]", dest, msg) < 2) {
                                send_text(pfds[i].fd, "Failed to store offline message.\n");
                                continue;
                            }

                            idx = user_exists(dest);
                            if (idx == -1) {
                                send_text(pfds[i].fd, "Destination user is not registered.\n");
                                continue;
                            }

                            if (append_offline_message(dest, msg) == 0) {
                                bzero(buffer, sizeof(buffer));
                                snprintf(buffer, sizeof(buffer), "Message saved for %s.\n", dest);
                                send_text(pfds[i].fd, buffer);
                            } else {
                                send_text(pfds[i].fd, "Failed to store offline message.\n");
                            }
                        } else if (strncmp(buffer, "EXIT", 4) == 0) {
                            printf("Got exit message. Removing user from system\n");
                            // send leave message to the other members
                            bzero(buffer, sizeof(buffer));
                            strcpy(buffer, get_username(pfds[i].fd));
                            strcat(buffer, " has left the chatroom\n");
                            /*********************************/
                            /* Broadcast the leave message to the other users in the group*/
                            /**********************************/
                            broadcast_to_online(pfds[i].fd, buffer);

                            /*********************************/
                            /* Change the state of this user to offline*/
                            /**********************************/
                            for (u = 0; u < users_count; u++) {
                                if (listOfUsers[u]->sockfd == pfds[i].fd) {
                                    listOfUsers[u]->state = 0;
                                    listOfUsers[u]->sockfd = -1; /* clear stored fd to avoid future false matches */
                                    break;
                                }
                            }
                            close(pfds[i].fd);
                            del_from_pfds(pfds, i, &fd_count);
                            i--;

                        } else if (strncmp(buffer, "DIR", 3) == 0) {
                            /*********************************/
                            /* Build DIR response: all registered users with online/offline status
                             * and set to client*/
                            /**********************************/
                            int wrote = 0;
                            bzero(buffer, sizeof(buffer));
                            wrote += snprintf(buffer + wrote, sizeof(buffer) - wrote, "=== User List ===\n");
                            wrote += snprintf(buffer + wrote, sizeof(buffer) - wrote, "%d user(s) total:\n", users_count);
                            for (u = 0; u < users_count; u++) {
                                if (listOfUsers[u]->sockfd == pfds[i].fd) {
                                    continue;
                                }
                                wrote += snprintf(buffer + wrote, sizeof(buffer) - wrote,
                                                  "%-24s %7s %15s %d\n", listOfUsers[u]->username,
                                                  listOfUsers[u]->state ? "online" : "offline",
                                                  listOfUsers[u]->ip, listOfUsers[u]->port);
                                if (wrote >= (int)sizeof(buffer) - 1) {
                                    break;
                                }
                            }
                            wrote += snprintf(buffer + wrote, sizeof(buffer) - wrote, "=================\n");
                            send_text(pfds[i].fd, buffer);



                            printf("Client %s successfully requested DIR.\n",
                                   get_username(pfds[i].fd));
                        } else {
                            /*********************************/
                            /* Handle broadcast messages */
                            /**********************************/
                            
                            char broadcast_msg[MAX];
                            bzero(broadcast_msg, sizeof(broadcast_msg));
                            snprintf(broadcast_msg, sizeof(broadcast_msg), 
                                     "%s: %s", get_username(pfds[i].fd), buffer);
                            broadcast_to_online(pfds[i].fd, broadcast_msg);
                            
                            printf("Client %s successfully broadcasted a message.\n",
                                   get_username(pfds[i].fd));
                        }
                    }
                }  // end handle data from client
            }  // end got new incoming connection
        }  // end looping through file descriptors
    }  // end for(;;)

    return 0;
}
