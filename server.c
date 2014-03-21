#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
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
#include "src/common/include/search.h"
#include "src/common/include/dberror.h"
#include "src/common/include/try.h"
#include "src/server/include/storage.h"
#include "src/server/include/aggregate.h"

#define PORT 5000
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
    int result;
    struct server_jobctx *jobctx;
    TRYNULL(result, DBENOMEM, jobctx, malloc(sizeof(struct server_jobctx)), done);
    TRYNULL(result, DBENOMEM, jobctx->sj_env, vartuplearray_create(), cleanup_malloc);
    TRYNULL(result, DBENOMEM, jobctx->sj_files, filetuplearray_create(), cleanup_vartuple);
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
    struct csv_resultarray *results = NULL;
    TRYNULL(result, DBECSV, results, csv_parse(csvfd), done);

    // for each column, load the data into that column
    for (unsigned i = 0; i < csv_resultarray_num(results); i++) {
        struct csv_result *csvheader = csv_resultarray_get(results, i);
        struct column *col;
        TRYNULL(result, DBECOLOPEN, col,
                column_open(jobctx->sj_storage, csvheader->csv_colname),
                cleanup_csv);
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
        TRYNULL(result, DBENOMEM, vtuple, malloc(sizeof(struct vartuple)), done);
        should_cleanup_vtuple_on_err = true;
        strcpy(vtuple->vt_var, varname);
        TRY(result, vartuplearray_add(env, vtuple, NULL), cleanup_vartuple);
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
    struct column *col;
    TRYNULL(result, DBECOLOPEN, col,
            column_open(jobctx->sj_storage, op->op_select.op_sel_col),
            done);
    struct column_ids *ids;
    TRYNULL(result, DBECOLSELECT, ids, column_select(col, op), cleanup_col);

    // If we have an assignment,
    // find the variable in the vartuple array if it already exists
    // otherwise create a new variable tuple
    switch (op->op_type) {
    case OP_SELECT_ALL_ASSIGN:
    case OP_SELECT_RANGE_ASSIGN:
    case OP_SELECT_VALUE_ASSIGN:
        TRY(result, server_add_var(jobctx->sj_env, op->op_select.op_sel_var,
                                   VAR_IDS, ids, NULL), cleanup_ids);
        result = 0;
        goto cleanup_col; // don't destroy ids
    case OP_SELECT_ALL:
    case OP_SELECT_RANGE:
    case OP_SELECT_VALUE:
        TRY(result, rpc_write_select_result(jobctx->sj_fd, ids), cleanup_ids);
        result = 0;
        goto cleanup_ids; // destroy the ids
    default:
        assert(0);
        break;
    }

  cleanup_ids:
    column_ids_destroy(ids);
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
    struct column *col;
    TRYNULL(result, DBECOLOPEN, col,
            column_open(jobctx->sj_storage, op->op_fetch.op_fetch_col),
            done);
    // find the variable representing the positions
    struct vartuple *v;
    TRYNULL(result, DBENOVAR, v,
            server_eval_get_var(jobctx->sj_env, op->op_fetch.op_fetch_pos),
            cleanup_col);
    if (v->vt_type != VAR_IDS) {
        result = DBEVARTYPE;
        DBLOG(result);
        goto cleanup_col;
    }
    // now that we have the ids, let's fetch the values for those ids
    struct column_vals *vals;
    TRYNULL(result, DBECOLFETCH, vals,
            column_fetch(col, v->vt_column_ids), cleanup_col);

    switch (op->op_type) {
    case OP_FETCH:
        // now write the results back to the client
        // sort the values before returning
        qsort(vals->cval_vals, vals->cval_len, sizeof(int), int_compare);
        TRY(result, rpc_write_fetch_result(jobctx->sj_fd, vals), cleanup_vals);
        break;
    case OP_FETCH_ASSIGN:
        TRY(result, server_add_var(jobctx->sj_env, op->op_fetch.op_fetch_var,
                                   VAR_VALS, NULL, vals), cleanup_vals);
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
server_eval_agg(struct server_jobctx *jobctx, struct op *op) {
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_AGG);

    int result;

    // Try to find the column intermediate
    struct vartuple *v;
    TRYNULL(result, DBENOVAR, v,
            server_eval_get_var(jobctx->sj_env, op->op_agg.op_agg_col),
            done);
    if (v->vt_type != VAR_VALS) {
        result = DBEVARTYPE;
        DBLOG(result);
        goto done;
    }

    // Perform the aggregation
    agg_func_t aggf = agg_func(op->op_agg.op_agg_atype);
    struct column_vals *aggval;
    TRY(result, column_agg(v->vt_column_vals, aggf, &aggval), done);
    assert(aggval != NULL);

    // If this is an assignment, add it to the environment
    if (op->op_agg.op_agg_assign) {
        TRY(result, server_add_var(jobctx->sj_env, op->op_agg.op_agg_var,
                                   VAR_VALS, NULL, aggval), cleanup_aggval);
        result = 0;
        goto done; // don't destroy aggval
    } else {
        TRY(result, rpc_write_fetch_result(jobctx->sj_fd, aggval), cleanup_aggval);
        result = 0;
        goto cleanup_aggval; // destroy the intermediate
    }

  cleanup_aggval:
    column_vals_destroy(aggval);
  done:
    return result;
}

static
int
server_eval_math(struct server_jobctx *jobctx, struct op *op) {
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_MATH);

    int result;

    // Try to find the column intermediates
    struct vartuple *vleft, *vright;
    TRYNULL(result, DBENOVAR, vleft,
            server_eval_get_var(jobctx->sj_env, op->op_math.op_math_col1),
            done);
    TRYNULL(result, DBENOVAR, vright,
            server_eval_get_var(jobctx->sj_env, op->op_math.op_math_col2),
            done);
    if (vleft->vt_type != VAR_VALS || vright->vt_type != VAR_VALS) {
        result = DBEVARTYPE;
        DBLOG(result);
        goto done;
    }

    // Perform the aggregation
    math_func_t mathf = math_func(op->op_math.op_math_mtype);
    struct column_vals *mathvals;
    TRY(result, column_math(vleft->vt_column_vals,
                            vright->vt_column_vals, mathf, &mathvals), done);
    assert(mathvals != NULL);

    // If this is an assignment, add it to the environment
    if (op->op_math.op_math_assign) {
        TRY(result, server_add_var(jobctx->sj_env, op->op_math.op_math_var,
                                   VAR_VALS, NULL, mathvals), cleanup_mathval);
        result = 0;
        goto done; // don't destroy vals
    } else {
        qsort(mathvals->cval_vals, mathvals->cval_len, sizeof(int), int_compare);
        TRY(result, rpc_write_fetch_result(jobctx->sj_fd, mathvals), cleanup_mathval);
        result = 0;
        goto cleanup_mathval; // destroy the intermediate
    }

  cleanup_mathval:
    column_vals_destroy(mathvals);
  done:
    return result;
}

static
int
server_eval_print(struct server_jobctx *jobctx, struct op *op)
{
    assert(jobctx != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_PRINT);

    int result;
    struct vartuple *v;
    TRYNULL(result, DBENOVAR, v,
            server_eval_get_var(jobctx->sj_env, op->op_print.op_print_var),
            done);
    switch (v->vt_type) {
    case VAR_VALS:
        TRY(result, rpc_write_fetch_result(jobctx->sj_fd, v->vt_column_vals), done);
        break;
    case VAR_IDS:
        TRY(result, rpc_write_select_result(jobctx->sj_fd, v->vt_column_ids), done);
        break;
    default:
        assert(0);
        break;
    }

    result = 0;
    goto done;
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
    struct column *col;
    TRYNULL(result, DBECOLOPEN, col,
            column_open(jobctx->sj_storage, op->op_insert.op_insert_col),
            done);
    TRY(result, column_insert(col, op->op_insert.op_insert_val), done);

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
    int result;
    TRY(result, storage_add_column(jobctx->sj_storage, op->op_create.op_create_col,
                                   op->op_create.op_create_stype), done);
    result = 0;
    goto done;
  done:
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
    struct column_valsarray *tuples;
    TRYNULL(result, DBENOMEM, tuples, column_valsarray_create(), done);
    const char *delimiters = ",";
    char *pch;
    char *saveptr;

    // Find the variable in the environment
    pch = strtok_r((char *) op->op_tuple.op_tuple_vars, delimiters, &saveptr);
    while (pch != NULL) {
        struct vartuple *v;
        TRYNULL(result, DBENOVAR, v,
                server_eval_get_var(jobctx->sj_env, pch), cleanup_valsarray);
        if (v->vt_type != VAR_VALS) {
            result = DBEVARTYPE;
            DBLOG(result);
            goto cleanup_valsarray;
        }
        TRY(result, column_valsarray_add(tuples, v->vt_column_vals, NULL), cleanup_valsarray);
        pch = strtok_r(NULL, delimiters, &saveptr);
    }
    TRY(result, rpc_write_tuple_result(jobctx->sj_fd,
                                    (struct column_vals **) tuples->arr.v,
                                    tuples->arr.num), cleanup_valsarray);

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
    case OP_AGG:
        return server_eval_agg(jobctx, op);
    case OP_MATH:
        return server_eval_math(jobctx, op);
    case OP_PRINT:
        return server_eval_print(jobctx, op);
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
        TRY(result, rpc_read_header(clientfd, &msg), recover);
        switch (msg.rpc_type) {
        case RPC_TERMINATE:
            printf("[Thread %u] received TERMINATE\n", threadnum);
            assert(rpc_write_terminate(clientfd) == 0);
            result = DBECLIENTTERM;
            goto recover;
        case RPC_QUERY:
            // handle RPC_FILE here as well
            TRY(result, rpc_read_query(clientfd, &msg, &op), recover);
            if (op->op_type == OP_LOAD) {
                int copyfd;
                char filenamebuf[128];
                sprintf(filenamebuf, "%s/jobid-%d.loadid-%d.tmp",
                        sarg->sj_storage->st_dbdir, jobid, loadid);
                loadid++;
                TRY(result, rpc_read_header(clientfd, &loadmsg), cleanup_op);
                assert(loadmsg.rpc_type = RPC_FILE);
                TRY(result, rpc_read_file(clientfd, &loadmsg, filenamebuf, &copyfd), cleanup_op);
                struct filetuple *ftuple;
                TRYNULL(result, DBENOMEM, ftuple, malloc(sizeof(struct filetuple)), cleanup_op);
                // TODO: file names are larger than ftuple char buf
                strcpy(ftuple->ft_name, op->op_load.op_load_file);
                ftuple->ft_fd = copyfd;
                result = filetuplearray_add(sarg->sj_files, ftuple, NULL);
                if (result) {
                    free(ftuple);
                    goto cleanup_op;
                }
            }
            TRY(result, server_eval(sarg, op), cleanup_op);
            break;
        default:
            assert(0);
            break;
        }
        continue;

      cleanup_op:
        free(op);
      recover:
        if (result != DBECLIENTTERM) {
            (void) rpc_write_error(clientfd, (char *) dberror_string(result));
        }
        if (dberror_server_is_fatal(result)) {
            goto done;
        }
        continue;
    }
    // TODO send result/error
  done:
    if (result && result != DBECLIENTTERM) {
        (void) rpc_write_terminate(clientfd);
    }
    server_jobctx_destroy(sarg);
}

static struct {
    int sopt_port;
    int sopt_backlog;
    int sopt_nthreads;
    char sopt_dbdir[128];
} server_options = {
    .sopt_port = PORT,
    .sopt_backlog = BACKLOG,
    .sopt_nthreads = NTHREADS,
    .sopt_dbdir = DBDIR,
};

const char *short_options = "h";

const struct option long_options[] = {
    {"help", no_argument, NULL, 'h'},
    {"port", required_argument, &server_options.sopt_port, 0},
    {"backlog", required_argument, &server_options.sopt_backlog, 0},
    {"nthreads", required_argument,  &server_options.sopt_nthreads, 0},
    {"dbdir", required_argument, NULL, 0},
    {NULL, 0, NULL, 0}
};

int
main(int argc, char **argv)
{
    while (1) {
        int option_index;
        int c = getopt_long(argc, argv, short_options,
                           long_options, &option_index);
        if (c == -1) {
            break;
        }
        switch (c) {
        case 0:
            if (optarg) {
                if (strcmp(long_options[option_index].name, "dbdir") == 0) {
                    strcpy(server_options.sopt_dbdir, optarg);
                } else {
                    *(long_options[option_index].flag) = atoi(optarg);
                }
            }
            break;
        case 'h':
            printf("Usage: %s\n", argv[0]);
            printf("--help\n");
            printf("--port P        [default=5000]\n");
            printf("--backlog B     [default=16]\n");
            printf("--nthreads T    [default=16]\n");
            printf("--dbdir dir     [default=db]\n");
            return 0;
        }
    }
    printf("port: %d, backlog: %d, nthreads: %d, dbdir: %s\n",
            server_options.sopt_port, server_options.sopt_backlog,
            server_options.sopt_nthreads, server_options.sopt_dbdir);

    int result;
    int listenfd, acceptfd;
    struct addrinfo hints, *servinfo;
    int yes = 1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    char portbuf[16];
    sprintf(portbuf, "%d", server_options.sopt_port);
    result = getaddrinfo(NULL, portbuf, &hints, &servinfo);
    if (result != 0) {
        result = DBEGETADDRINFO;
        DBLOG(result);
        goto done;
    }

    // create a socket for listening for connections
    listenfd = socket(servinfo->ai_family, servinfo->ai_socktype,
                      servinfo->ai_protocol);
    if (listenfd == -1) {
        result = DBESOCKET;
        DBLOG(result);
        goto done;
    }

    // make the socket reusable
    result = setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
    if (result == -1) {
        result = DBESETSOCKOPT;
        DBLOG(result);
        goto cleanup_listenfd;
    }

    // bind the file descriptor to a port
    result = bind(listenfd, servinfo->ai_addr, servinfo->ai_addrlen);
    if (result == -1) {
        result = DBEBIND;
        DBLOG(result);
        goto cleanup_listenfd;
    }
    freeaddrinfo(servinfo);

    // put the socket in listening mode
    result = listen(listenfd, server_options.sopt_backlog);
    if (result == -1) {
        result = DBELISTEN;
        DBLOG(result);
        goto cleanup_listenfd;
    }

    // install a SIGINT handler for graceful shutdown
    struct sigaction sig;
    sig.sa_handler = sigint_handler;
    sig.sa_flags = 0;
    sigemptyset( &sig.sa_mask );
    result = sigaction( SIGINT, &sig, NULL );
    if (result == -1) {
        result = DBESIGACTION;
        DBLOG(result);
        goto done;
    }

    // init the storage directory
    struct storage *storage = storage_init(server_options.sopt_dbdir);
    if (storage == NULL) {
        fprintf(stderr, "storage failed\n");
        goto cleanup_listenfd;
    }

    // create a threadpool to handle the connections
    struct threadpool *tpool = threadpool_create(server_options.sopt_nthreads);
    if (tpool == NULL) {
        perror("threadpool");
        goto cleanup_storage;
    }

    // accept in a loop, waiting for more connections
    unsigned jobid = 0;
    while (keep_running) {
        acceptfd = accept(listenfd, NULL, NULL);
        if (acceptfd == -1) {
            result = DBEACCEPT;
            DBLOG(result);
            goto shutdown;
        }

        // add the file descriptor as a job to the thread pool
        // if we can't add it, clean up the file descriptor.
        // if we are successful, the threadpool will handle
        // cleaning up the file descriptor
        struct server_jobctx *sjob;
        TRYNULL(result, DBENOMEM, sjob,
                server_jobctx_create(acceptfd, jobid++, storage),
                cleanup_acceptfd);

        struct job job;
        job.j_arg = (void *) sjob;
        job.j_routine = server_routine;
        TRY(result, threadpool_add_job(tpool, &job), cleanup_sjob);
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
