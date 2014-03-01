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
#include "src/common/include/csv.h"
#include "src/server/include/storage.h"

#define PORT "5000"
#define BACKLOG 16
#define NTHREADS 16
#define DBDIR "db"

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
    struct column_ids *vt_column_ids;
};

// (file name) -> file descriptor to CSV file
struct filetuple {
    char ft_name[128];
    int ft_fd;
};

DECLARRAY(vartuple);
DEFARRAY(vartuple, /* no inline */);
DECLARRAY(filetuple);
DEFARRAY(filetuple, /* no inline */);

struct server_jobctx {
    int sj_fd;
    unsigned sj_jobid;
    struct storage *sj_storage;
    struct vartuplearray *sj_env;
    struct filetuplearray *sj_files;
};

static
struct server_jobctx *
server_jobctx_create(int fd, unsigned jobid, struct storage *storage)
{
    struct server_jobctx *jobctx = malloc(sizeof(struct server_jobctx));
    if (jobctx == NULL) {
        goto done;
    }
    jobctx->sj_env = vartuplearray_create();
    if (jobctx->sj_env == NULL) {
        goto cleanup_malloc;
    }
    jobctx->sj_files = filetuplearray_create();
    if (jobctx->sj_files == NULL) {
        goto cleanup_vartuple;
    }
    jobctx->sj_fd = fd;
    jobctx->sj_jobid = jobid;
    jobctx->sj_storage = storage;
    goto done;
  cleanup_vartuple:
    vartuplearray_destroy(jobctx->sj_env);
  cleanup_malloc:
    free(jobctx);
    jobctx = NULL;
  done:
    return jobctx;
}

static
void
server_jobctx_destroy(struct server_jobctx *jobctx)
{
    assert(jobctx != NULL);
    while (vartuplearray_num(jobctx->sj_env) > 0) {
        struct vartuple *v = vartuplearray_get(jobctx->sj_env, 0);
        free(v);
        vartuplearray_remove(jobctx->sj_env, 0);
    }
    vartuplearray_destroy(jobctx->sj_env);
    while (filetuplearray_num(jobctx->sj_files) > 0) {
        struct filetuple *f = filetuplearray_get(jobctx->sj_files, 0);
        free(f);
        filetuplearray_remove(jobctx->sj_files, 0);
    }
    filetuplearray_destroy(jobctx->sj_files);
    // jobctx->sj_fd should be closed in load handler
    free(jobctx);
}

static
int
server_eval_select(struct server_jobctx *jobctx, struct op *op)
{
    return 0;
}

static
int
server_eval_load(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_LOAD);

    // find the csv file descriptor for the load file name
    int csvfd = -1;
    for (unsigned i = 0; i < filetuplearray_num(jobctx->sj_files); i++) {
        struct filetuple *ftuple = filetuplearray_get(jobctx->sj_files, i);
        if (strcmp(ftuple->ft_name, op->op_load.op_load_file) == 0) {
            csvfd = ftuple->ft_fd;
            break;
        }
    }
    assert(csvfd != -1);
    int result;

    // parse the csv
    struct csv_resultarray *results = csv_parse(csvfd);
    if (results == NULL) {
        result = -1;
        goto done;
    }

    // for each column, load the data into that column
    for (unsigned i = 0; i < csv_resultarray_num(results); i++) {
        struct csv_result *csvheader = csv_resultarray_get(results, i);
        struct column *col =
                column_open(jobctx->sj_storage, csvheader->csv_colname);
        if (col == NULL) {
            goto cleanup_csv;
        }
        result = column_load(col, (int *) csvheader->csv_vals->arr.v,
                             intarray_num(csvheader->csv_vals));
        if (result) {
            fprintf(stderr, "column load failed\n");
            goto cleanup_column;
        }
        column_close(col);
        continue;

      cleanup_column:
        column_close(col);
        goto cleanup_csv;
    }

  cleanup_csv:
    csv_destroy(results);
  done:
    return result;
}

static
int
server_eval_fetch(struct server_jobctx *jobctx, struct op *op)
{
    return 0;
}

static
int
server_eval_insert(struct server_jobctx *jobctx, struct op *op)
{
    return 0;
}

static
int
server_eval_create(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_CREATE);
    printf("eval create %s\n", op->op_create.op_create_col);
    return storage_add_column(jobctx->sj_storage, op->op_create.op_create_col,
                              op->op_create.op_create_stype);
}

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
int
server_eval(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    switch (op->op_type) {
    case OP_SELECT_ALL_ASSIGN:
    case OP_SELECT_RANGE_ASSIGN:
    case OP_SELECT_VALUE_ASSIGN:
    case OP_SELECT_ALL:
    case OP_SELECT_RANGE:
    case OP_SELECT_VALUE:
        return server_eval_select(jobctx, op);
    case OP_FETCH:
        return server_eval_fetch(jobctx, op);
    case OP_CREATE:
        return server_eval_create(jobctx, op);
    case OP_LOAD:
        return server_eval_load(jobctx, op);
    case OP_INSERT:
        return server_eval_insert(jobctx, op);
    default:
        assert(0);
        return -1;
    }
}

static
void
server_routine(void *arg)
{
    assert(arg != NULL);
    struct server_jobctx *sarg = (struct server_jobctx *) arg;
    int clientfd = sarg->sj_fd;
    unsigned jobid = sarg->sj_jobid;
    // TODO: support multiple loads from a single client
    // append a file num
    int result;

    while (1) {
        struct op *op;
        result = dbm_read_query(clientfd, &op);
        if (result) {
            goto done;
        }
        if (op->op_type == OP_LOAD) {
            int copyfd;
            result = dbm_read_file(clientfd, jobid, &copyfd);
            if (result) {
                goto cleanup_op;
            }
            struct filetuple *ftuple = malloc(sizeof(struct filetuple));
            if (ftuple == NULL) {
                goto cleanup_op;
            }
            // TODO: file names are larger than ftuple char buf
            strcpy(ftuple->ft_name, op->op_load.op_load_file);
            ftuple->ft_fd = copyfd;
            result = filetuplearray_add(sarg->sj_files, ftuple, NULL);
            if (result) {
                free(ftuple);
                goto cleanup_op;
            }
        }
        result = server_eval(sarg, op);
        if (result) {
            goto cleanup_op;
        }
        continue;
      cleanup_op:
        free(op);
        goto done;
    }
    // TODO send result/error
  done:
    server_jobctx_destroy(sarg);
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

    // init the storage directory
    struct storage *storage = storage_init(DBDIR);
    if (storage == NULL) {
        fprintf(stderr, "storage failed\n");
        goto cleanup_listenfd;
    }

    // create a threadpool to handle the connections
    struct threadpool *tpool = threadpool_create(NTHREADS);
    if (tpool == NULL) {
        perror("threadpool");
        goto cleanup_storage;
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
        struct server_jobctx *sjob =
                server_jobctx_create(acceptfd, jobid++, storage);
        if (sjob == NULL) {
            goto cleanup_acceptfd;
        }

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
  cleanup_storage:
    storage_close(storage);
  cleanup_listenfd:
    assert(close(listenfd) == 0);
  done:
    return result;
}
