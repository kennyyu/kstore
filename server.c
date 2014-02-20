#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <string.h>
#include <unistd.h>
#include "include/threadpool.h"

#define PORT 5000
#define BACKLOG 16

// boolean to tell the server to keep looping
static bool volatile keep_running = true;

static
void
sigint_handler(int sig)
{
    (void) sig;
    keep_running = false;
    printf("Caught shutdown signal, shutting down...\n");
}

int
main(void)
{
    struct threadpool *tpool = threadpool_create(5);
    assert(tpool != NULL);
    threadpool_destroy(tpool);
    printf("hello server\n");

    int result;
    int listenfd, acceptfd;
    struct sockaddr_in serv_addr;

    // create a file descriptor to listen for connections
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd == -1) {
        perror("socket");
        result = listenfd;
        goto done;
    }

    // bind the file descriptor to a port
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(PORT);
    result = bind(listenfd, (const struct sockaddr *) &serv_addr,
                  sizeof(serv_addr));
    if (result == -1) {
        perror("bind");
        goto done;
    }

    // put the socket in listening mode
    result = listen(listenfd, BACKLOG);
    if (result == -1) {
        perror("listen");
        goto done;
    }

    // install a SIGINT handler for graceful shutdown
    if (signal(SIGINT, sigint_handler) == SIG_ERR) {
        perror("signal");
        result = -1;
        goto done;
    }

    // accept in a loop, waiting for more connections
    char buf[1024];
    while (keep_running) {
        acceptfd = accept(listenfd, NULL, NULL);
        if (acceptfd == -1) {
            perror("accept");
            goto done;
        }

        // TODO: add a job to the threadpool to handle acceptfd
        sprintf(buf, "hello from server");
        write(acceptfd, buf, sizeof(buf));
        close(acceptfd);
    }

    printf("shutdown");

  done:
    return result;
}
