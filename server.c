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
#include "src/common/include/array.h"
#include "src/server/include/storage.h"

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

// tuple of (variable, column_id) to store the
// result of selects
struct vartuple {
    char vt_var[128];
    struct column_id *vt_column_ids;
};

struct filetuple {
    char ft_name[128];
    int fd;
};

DECLARRAY(vartuple);
DEFARRAY(vartuple, /* no inline */);
DECLARRAY(filetuple);
DEFARRAY(filetuple, /* no inline */);
DECLARRAY(columntuple);
DEFARRAY(columntuple, /* no inline */);

struct server_job_context {
    int sj_fd;
    unsigned sj_jobid;
    struct columnarray *sj_cols;
    struct vartuplearray *sj_env;
    struct filetuplearray *sj_files;
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
//   if ASSIGN, map varname -> ID
// fetch:
//   find varname -> lookup by ID

static
void
server_routine(void *arg)
{
    assert(arg != NULL);
    struct server_job_context *sarg = (struct server_job_context *) arg;
    int clientfd = sarg->sj_fd;
    unsigned jobid = sarg->sj_jobid;
    // TODO: support multiple loads from a single client
    free(sarg);
    int result;

    while (1) {
        struct op *op;
        int copyfd;
        result = dbm_read_query(clientfd, &op);
        if (result) {
            goto done;
        }
        if (op->op_type == OP_LOAD) {
            result = dbm_read_file(clientfd, jobid, &copyfd);
            if (result) {
                goto cleanup_op;
            }
        }
        continue;
      cleanup_op:
        free(op);
        goto done;
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
    unsigned jobid = 0;
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
        struct server_job_context *sjob = malloc(sizeof(struct server_job_context));
        if (sjob == NULL) {
            goto cleanup_acceptfd;
        }
        sjob->sj_fd = acceptfd;
        sjob->sj_jobid = jobid++;

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
