#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <unistd.h>
#include "src/common/include/io.h"
#include "src/common/include/threadpool.h"
#include "src/common/include/db_message.h"

#define PORT "5000"
#define BACKLOG 16
#define NTHREADS 16

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

struct server_job_args {
    int fd;
};

// TODO: need a struct server context
// array of (name, select ids)
// array of file names -> file descriptors
// need file descriptor -> parse CSV
// column names -> file descriptor for unsorted data
// if type != unsorted-->build sorted projection in new file
//
// create:
//   create new file
// load:
//   parse CSV
//   write unsorted data into columns
//   for each column, build sorted projection if needed
// select:
//   perform select, store ids in bitmap
//   if ASSIGn, map varname -> ID
// fetch:
//   find varname -> lookup by ID

static
void
server_routine(void *arg)
{
    assert(arg != NULL);
    struct server_job_args *sarg = (struct server_job_args *) arg;
    int clientfd = sarg->fd;
    free(sarg);
    int result;
    int copyfd;
    int currfileno = 0;
    char buf[1024];

    char *payload;
    while (1) {
        struct db_message msg;
        result = dbm_read(clientfd, &msg);
        if (result) {
            goto done;
        }
        switch (msg.dbm_type) {
        case DB_MESSAGE_QUERY:
            payload = malloc(sizeof(char) * msg.dbm_len); // includes null byte
            if (payload == NULL) {
                goto done;
            }
            result = io_read(clientfd, payload, msg.dbm_len);
            if (result) {
                free(payload);
                goto done;
            }
            printf("got query: [%s]\n", payload);
            free(payload);
            break;
        case DB_MESSAGE_FILE:
            sprintf(buf, "%d.tmp", currfileno);
            copyfd = open(buf, O_CREAT | O_RDWR | O_TRUNC, S_IRWXU);
            if (copyfd == -1) {
                result = -1;
                goto done;
            }
            result = io_copy(clientfd, copyfd, msg.dbm_len);
            if (result) {
                assert(close(copyfd) == 0);
                goto done;
            }
            printf("got file: %s\n", buf);
            assert(close(copyfd) == 0);
            break;
        default:
            assert(0);
            break;
        }
    }
    // TODO send result/error
  done:
    assert(close(clientfd) == 0);
}

int
main(void)
{
    printf("hello server\n");

    int result;
    int listenfd, acceptfd;
    struct addrinfo hints, *servinfo;
    int yes = 1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    result = getaddrinfo(NULL, PORT, &hints, &servinfo);
    if (result != 0) {
        perror("getaddrinfo");
        result = -1;
        goto done;
    }

    // create a socket for listening for connections
    listenfd = socket(servinfo->ai_family, servinfo->ai_socktype,
                      servinfo->ai_protocol);
    if (listenfd == -1) {
        perror("socket");
        result = listenfd;
        goto done;
    }

    // make the socket reusable
    result = setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
    if (result == -1) {
        perror("setsockopt");
        goto cleanup_listenfd;
    }

    // bind the file descriptor to a port
    result = bind(listenfd, servinfo->ai_addr, servinfo->ai_addrlen);
    if (result == -1) {
        perror("bind");
        goto cleanup_listenfd;
    }
    freeaddrinfo(servinfo);

    // put the socket in listening mode
    result = listen(listenfd, BACKLOG);
    if (result == -1) {
        perror("listen");
        goto cleanup_listenfd;
    }

    // install a SIGINT handler for graceful shutdown
    struct sigaction sig;
    sig.sa_handler = sigint_handler;
    sig.sa_flags = 0;
    sigemptyset( &sig.sa_mask );
    result = sigaction( SIGINT, &sig, NULL );
    if (result == -1) {
        perror("sigaction");
        goto done;
    }

    // create a threadpool to handle the connections
    struct threadpool *tpool = threadpool_create(NTHREADS);
    if (tpool == NULL) {
        perror("threadpool");
        goto cleanup_listenfd;
    }

    // accept in a loop, waiting for more connections
    while (keep_running) {
        acceptfd = accept(listenfd, NULL, NULL);
        if (acceptfd == -1) {
            perror("accept");
            goto shutdown;
        }

        // add the file descriptor as a job to the thread pool
        // if we can't add it, clean up the file descriptor.
        // if we are successful, the threadpool will handle
        // cleaning up the file descriptor
        struct server_job_args *sjob = malloc(sizeof(struct server_job_args));
        if (sjob == NULL) {
            goto cleanup_acceptfd;
        }
        sjob->fd = acceptfd;

        struct job job;
        job.j_arg = (void *) sjob;
        job.j_routine = server_routine;
        result = threadpool_add_job(tpool, &job);
        if (result == -1) {
            goto cleanup_sjob;
        }
        continue;

      cleanup_sjob:
        free(sjob);
      cleanup_acceptfd:
        assert(close(acceptfd));
    }

  shutdown:
    threadpool_destroy(tpool);
    printf("done.\n");
    result = 0;
  cleanup_listenfd:
    assert(close(listenfd) == 0);
  done:
    return result;
}
