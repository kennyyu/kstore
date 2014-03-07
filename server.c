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
#include "src/common/include/rpc.h"
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

enum vartuple_type {
    VAR_IDS,
    VAR_VALS,
};

// tuple of (variable, column_id) to store the
// result of selects and fetches
struct vartuple {
    char vt_var[128];
    enum vartuple_type vt_type;
    union {
        struct column_ids *vt_column_ids;
        struct column_vals *vt_column_vals;
    };
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
        switch (v->vt_type) {
        case VAR_IDS:
            column_ids_destroy(v->vt_column_ids);
            break;
        case VAR_VALS:
            column_vals_destroy(v->vt_column_vals);
            break;
        default:
            assert(0);
            break;
        }
        free(v);
        vartuplearray_remove(jobctx->sj_env, 0);
    }
    vartuplearray_destroy(jobctx->sj_env);
    while (filetuplearray_num(jobctx->sj_files) > 0) {
        struct filetuple *f = filetuplearray_get(jobctx->sj_files, 0);
        // load file descriptor closed in load handler
        free(f);
        filetuplearray_remove(jobctx->sj_files, 0);
    }
    filetuplearray_destroy(jobctx->sj_files);
    free(jobctx);
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
    unsigned ix = -1;
    for (unsigned i = 0; i < filetuplearray_num(jobctx->sj_files); i++) {
        struct filetuple *ftuple = filetuplearray_get(jobctx->sj_files, i);
        if (strcmp(ftuple->ft_name, op->op_load.op_load_file) == 0) {
            ix = i;
            csvfd = ftuple->ft_fd;
            break;
        }
    }
    assert(csvfd != -1);
    assert(ix != -1);
    filetuplearray_remove(jobctx->sj_files, ix);
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
            column_close(col);
            goto cleanup_csv;
        }
        column_close(col);
    }

  cleanup_csv:
    csv_destroy(results);
  done:
    return result;
}

static
struct vartuple *
server_eval_get_var(struct vartuplearray *env, char *varname)
{
    for (unsigned i = 0; i < vartuplearray_num(env); i++) {
        struct vartuple *v = vartuplearray_get(env, i);
        if (strcmp(v->vt_var, varname) == 0) {
            return v;
        }
    }
    return NULL;
}

static
int
server_add_var(struct vartuplearray *env, char *varname,
               enum vartuple_type type,
               struct column_ids *ids,
               struct column_vals *vals)
{
    assert(env != NULL);
    assert(varname != NULL);
    switch (type) {
    case VAR_IDS:
        assert(ids != NULL);
        assert(vals == NULL);
        break;
    case VAR_VALS:
        assert(vals != NULL);
        assert(ids == NULL);
        break;
    default:
        assert(0);
        break;
    }

    int result;
    bool should_cleanup_vtuple_on_err = false;
    struct vartuple *vtuple = server_eval_get_var(env, varname);
    if (vtuple == NULL) {
        // we couldn't find the variable in the environment, so we
        // need to create it
        vtuple = malloc(sizeof(struct vartuple));
        if (vtuple == NULL) {
            result = -1;
            goto done;
        }
        should_cleanup_vtuple_on_err = true;
        strcpy(vtuple->vt_var, varname);
        result = vartuplearray_add(env, vtuple, NULL);
        if (result) {
            goto cleanup_vartuple;
        }
    } else {
        // need to destroy the old assignment before reusing
        switch (vtuple->vt_type) {
        case VAR_IDS:
            column_ids_destroy(vtuple->vt_column_ids);
            vtuple->vt_column_ids = NULL;
            break;
        case VAR_VALS:
            column_vals_destroy(vtuple->vt_column_vals);
            vtuple->vt_column_vals = NULL;
            break;
        default:
            assert(0);
            break;
        }
    }
    vtuple->vt_type = type;
    switch (type) {
    case VAR_IDS:
        vtuple->vt_column_ids = ids;
        break;
    case VAR_VALS:
        vtuple->vt_column_vals = vals;
        break;
    default:
        assert(0);
        break;
    }

    // success
    result = 0;
    goto done;
  cleanup_vartuple:
    if (should_cleanup_vtuple_on_err) {
        free(vtuple);
    }
  done:
    return result;
}

static
int
server_eval_select(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type >= OP_SELECT_ALL_ASSIGN
           && op->op_type <= OP_SELECT_RANGE);
    int result;
    struct column *col =
            column_open(jobctx->sj_storage, op->op_select.op_sel_col);
    if (col == NULL) {
        result = -1;
        goto done;
    }
    struct column_ids *ids = column_select(col, op);
    if (ids == NULL) {
        result = -1;
        goto cleanup_col;
    }

    // If we have an assignment,
    // find the variable in the vartuple array if it already exists
    // otherwise create a new variable tuple
    switch (op->op_type) {
    case OP_SELECT_ALL_ASSIGN:
    case OP_SELECT_RANGE_ASSIGN:
    case OP_SELECT_VALUE_ASSIGN:
        break;
    case OP_SELECT_ALL:
    case OP_SELECT_RANGE:
    case OP_SELECT_VALUE:
        goto cleanup_col;
    default:
        assert(0);
        break;
    }
    result = server_add_var(jobctx->sj_env, op->op_select.op_sel_var,
                            VAR_IDS, ids, NULL);
    if (result) {
        goto cleanup_col;
    }

    // success
    result = 0;
    goto cleanup_col;

  cleanup_col:
    column_close(col);
  done:
    return result;
}

static
int
server_eval_fetch(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_FETCH || op->op_type == OP_FETCH_ASSIGN);
    int result;
    struct column *col =
            column_open(jobctx->sj_storage, op->op_fetch.op_fetch_col);
    if (col == NULL) {
        result = -1;
        goto done;
    }
    // find the variable representing the positions
    struct vartuple *v =
            server_eval_get_var(jobctx->sj_env, op->op_fetch.op_fetch_pos);
    if (v == NULL) {
        result = -1; // couldn't find var
        goto cleanup_col;
    }
    assert(v->vt_type == VAR_IDS);
    // now that we have the ids, let's fetch the values for those ids
    struct column_vals *vals = column_fetch(col, v->vt_column_ids);
    if (vals == NULL) {
        result = -1;
        goto cleanup_col;
    }

    switch (op->op_type) {
    case OP_FETCH:
        // now write the results back to the client
        result = rpc_write_fetch_result(jobctx->sj_fd, vals);
        if (result) {
            goto cleanup_vals;
        }
        break;
    case OP_FETCH_ASSIGN:
        result = server_add_var(jobctx->sj_env, op->op_fetch.op_fetch_var,
                                VAR_VALS, NULL, vals);
        if (result) {
            goto cleanup_vals;
        }
        result = 0;
        goto cleanup_col;  // don't destroy results
    default:
        break;
    }

    // success
    result = 0;
    goto cleanup_vals;
  cleanup_vals:
    column_vals_destroy(vals);
  cleanup_col:
    column_close(col);
  done:
    return result;
}

static
int
server_eval_insert(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_INSERT);

    int result;
    struct column *col =
            column_open(jobctx->sj_storage, op->op_insert.op_insert_col);
    if (col == NULL) {
        result = -1;
        goto done;
    }
    result = column_insert(col, op->op_insert.op_insert_val);
    if (result) {
        goto done;
    }

    // success
    result = 0;
    goto cleanup_col;
  cleanup_col:
    column_close(col);
  done:
    return result;
}

static
int
server_eval_create(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_CREATE);
    int result = storage_add_column(jobctx->sj_storage, op->op_create.op_create_col,
                                    op->op_create.op_create_stype);
    if (result) {
        fprintf(stderr, "storage_add_column failed\n");
    }
    return result;
}

DECLARRAY(column_vals);
DEFARRAY(column_vals, /* no inline */);

static
int
server_eval_tuple(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_TUPLE);

    int result;
    struct column_valsarray *tuples = column_valsarray_create();
    if (tuples == NULL) {
        result = -1;
        goto done;
    }
    const char *delimiters = ",";
    char *pch;
    char *saveptr;

    // Find the variable in the environment
    pch = strtok_r((char *) op->op_tuple.op_tuple_vars, delimiters, &saveptr);
    while (pch != NULL) {
        struct vartuple *v = server_eval_get_var(jobctx->sj_env, pch);
        if (v == NULL) {
            result = -1;
            goto cleanup_valsarray;
        }
        assert(v->vt_type == VAR_VALS);
        result = column_valsarray_add(tuples, v->vt_column_vals, NULL);
        if (result) {
            goto cleanup_valsarray;
        }
        pch = strtok_r(NULL, delimiters, &saveptr);
    }
    result = rpc_write_tuple_result(jobctx->sj_fd,
                                    (struct column_vals **) tuples->arr.v,
                                    tuples->arr.num);
    if (result) {
        goto cleanup_valsarray;
    }

    result = 0;
    goto cleanup_valsarray;
  cleanup_valsarray:
    while (column_valsarray_num(tuples) > 0) {
        column_valsarray_remove(tuples, 0);
    }
    column_valsarray_destroy(tuples);
  done:
    return result;
}

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
    case OP_FETCH_ASSIGN:
        return server_eval_fetch(jobctx, op);
    case OP_CREATE:
        return server_eval_create(jobctx, op);
    case OP_LOAD:
        return server_eval_load(jobctx, op);
    case OP_INSERT:
        return server_eval_insert(jobctx, op);
    case OP_TUPLE:
        return server_eval_tuple(jobctx, op);
    default:
        assert(0);
        return -1;
    }
}

static
void
server_routine(void *arg, unsigned threadnum)
{
    assert(arg != NULL);
    struct server_jobctx *sarg = (struct server_jobctx *) arg;
    int clientfd = sarg->sj_fd;
    unsigned jobid = sarg->sj_jobid;
    unsigned loadid = 0;
    int result;

    while (1) {
        struct op *op;
        struct rpc_header msg;
        struct rpc_header loadmsg;
        result = rpc_read_header(clientfd, &msg);
        if (result) {
            goto done;
        }
        switch (msg.rpc_type) {
        case RPC_TERMINATE:
            printf("[Thread %u] received TERMINATE\n", threadnum);
            assert(rpc_write_terminate(clientfd) == 0);
            goto done;
        case RPC_QUERY:
            // handle RPC_FILE here as well
            result = rpc_read_query(clientfd, &msg, &op);
            if (result) {
                goto done;
            }
            if (op->op_type == OP_LOAD) {
                int copyfd;
                char filenamebuf[128];
                sprintf(filenamebuf, "%s/jobid-%d.loadid-%d.tmp",
                        sarg->sj_storage->st_dbdir, jobid, loadid);
                loadid++;
                result = rpc_read_header(clientfd, &loadmsg);
                if (result) {
                    goto cleanup_op;
                }
                assert(loadmsg.rpc_type = RPC_FILE);
                result = rpc_read_file(clientfd, &loadmsg, filenamebuf, &copyfd);
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
            break;
        default:
            assert(0);
            break;
        }
        continue;

      cleanup_op:
        free(op);
        goto done;
    }
    // TODO send result/error
  done:
    if (result) {
        (void) rpc_write_error(sarg->sj_fd, "server error");
        (void) rpc_write_terminate(sarg->sj_fd);
    }
    server_jobctx_destroy(sarg);
}

int
main(void)
{
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
