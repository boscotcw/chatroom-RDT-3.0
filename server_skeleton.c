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
    /*******************************************/
    /* Get the user name by the user's sock fd */
    /*******************************************/
    for (i = 0; i < users_count; i++) {
        if (listOfUsers[i]->sockfd == ss) {
            strcpy(uname, listOfUsers[i]->username);
            break;
        }
    }

    // printf("get user name: %s\n", uname);
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
    

    /**********************************************************/
    /*start listening for connections */
    /**********************************************************/

    // main loop
    for (;;) {
        /***************************************/
        /* use poll function */
        /**************************************/

        // run through the existing connections looking for data to read
        for (i = 0; i < fd_count; i++) {
            if (pfds[i].revents & POLLIN) {  // we got one!!
                if (pfds[i].fd == listener) {
                    /**************************/
                    /* It is the listener and needs to handle new connections from clients */
                    /****************************/
                    
                } else {
                    // handle data from a client
                    bzero(buffer, sizeof(buffer));
                    if ((nbytes = recv(pfds[i].fd, buffer, sizeof(buffer), MSG_WAITALL)) <= 0) {
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
                                break;
                            }
                        }
                        close(pfds[i].fd);  // Bye!
                        del_from_pfds(pfds, i, &fd_count);
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
                                continue;
                            }
                            if (peer_ip[0] == '\0') {
                                strcpy(peer_ip, "127.0.0.1");
                            }
                            if (isNewUser(name) == -1) {
                                /********************************/
                                /* it is a new user and we need to handle the registration*/
                                /**********************************/
                                
                                /*****************************/
                                /* Broadcast the welcome message*/
                                /*****************************/
                                

                                /*****************************/
                                /* send registration success message to the new user*/
                                /*****************************/

                            } else {
                                /********************************/
                                /* it's an existing user and we need to handle the login. Note the
                                 * state of user,*/
                                /**********************************/
                                
                                /*****************************/
                                /* Broadcast the welcome message 
                                /* and Deliver any stored offline messages after re-login
                                /*****************************/
                                
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


                            /*********************************/
                            /* Change the state of this user to offline*/
                            /**********************************/

                        } else if (strncmp(buffer, "DIR", 3) == 0) {
                            /*********************************/
                            /* Build DIR response: all registered users with online/offline status
                             * and set to client*/
                            /**********************************/
                            



                            printf("Client %s successfully requested DIR.\n",
                                   get_username(pfds[i].fd));
                        } else {
                            /*********************************/
                            /* Handle broadcast messages */
                            /**********************************/
                            

                            
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
