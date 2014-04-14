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
#include "../common/include/io.h"
#include "../common/include/threadpool.h"
#include "../common/include/rpc.h"
#include "../common/include/array.h"
#include "../common/include/csv.h"
#include "../common/include/search.h"
#include "../common/include/dberror.h"
#include "../common/include/try.h"
#include "../common/include/results.h"
#include "../server/include/storage.h"
#include "include/aggregate.h"
#include "include/join.h"
#include "include/server.h"

static
void
sigint_handler(int sig)
{
    (void) sig;
    printf("Caught shutdown signal, shutting down...\n");
}

struct server {
    struct server_options s_opt;
    int s_listenfd;
    struct storage *s_storage;
    struct threadpool *s_threadpool;
};

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

struct session {
    int ses_fd;
    unsigned ses_jobid;
    struct storage *ses_storage;
    struct vartuplearray *ses_env;
    struct filetuplearray *ses_files;
};

static
struct session *
session_create(int fd, unsigned jobid, struct storage *storage)
{
    int result;
    struct session *session;
    TRYNULL(result, DBENOMEM, session, malloc(sizeof(struct session)), done);
    TRYNULL(result, DBENOMEM, session->ses_env, vartuplearray_create(), cleanup_malloc);
    TRYNULL(result, DBENOMEM, session->ses_files, filetuplearray_create(), cleanup_vartuple);
    session->ses_fd = fd;
    session->ses_jobid = jobid;
    session->ses_storage = storage;
    goto done;
  cleanup_vartuple:
    vartuplearray_destroy(session->ses_env);
  cleanup_malloc:
    free(session);
    session = NULL;
  done:
    return session;
}

static
void
session_destroy(struct session *session)
{
    assert(session != NULL);
    while (vartuplearray_num(session->ses_env) > 0) {
        struct vartuple *v = vartuplearray_get(session->ses_env, 0);
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
        vartuplearray_remove(session->ses_env, 0);
    }
    vartuplearray_destroy(session->ses_env);
    while (filetuplearray_num(session->ses_files) > 0) {
        struct filetuple *f = filetuplearray_get(session->ses_files, 0);
        // load file descriptor closed in load handler
        free(f);
        filetuplearray_remove(session->ses_files, 0);
    }
    filetuplearray_destroy(session->ses_files);
    free(session);
}

static
int
server_eval_load(struct session *session, struct op *op)
{
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_LOAD);

    // find the csv file descriptor for the load file name
    int csvfd = -1;
    unsigned ix = -1;
    for (unsigned i = 0; i < filetuplearray_num(session->ses_files); i++) {
        struct filetuple *ftuple = filetuplearray_get(session->ses_files, i);
        if (strcmp(ftuple->ft_name, op->op_load.op_load_file) == 0) {
            ix = i;
            csvfd = ftuple->ft_fd;
            break;
        }
    }
    assert(csvfd != -1);
    assert(ix != -1);
    filetuplearray_remove(session->ses_files, ix);
    int result;

    // parse the csv
    struct csv_resultarray *results = NULL;
    TRYNULL(result, DBECSV, results, csv_parse(csvfd), done);

    // for each column, load the data into that column
    for (unsigned i = 0; i < csv_resultarray_num(results); i++) {
        struct csv_result *csvheader = csv_resultarray_get(results, i);
        struct column *col;
        TRY(result, column_open(session->ses_storage, csvheader->csv_colname, &col),
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
server_eval_select(struct session *session, struct op *op)
{
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type >= OP_SELECT_ALL_ASSIGN
           && op->op_type <= OP_SELECT_RANGE);
    int result;
    struct column *col;
    TRY(result, column_open(session->ses_storage, op->op_select.op_sel_col, &col), done);
    struct column_ids *ids;
    TRYNULL(result, DBECOLSELECT, ids, column_select(col, op), cleanup_col);

    // If we have an assignment,
    // find the variable in the vartuple array if it already exists
    // otherwise create a new variable tuple
    switch (op->op_type) {
    case OP_SELECT_ALL_ASSIGN:
    case OP_SELECT_RANGE_ASSIGN:
    case OP_SELECT_VALUE_ASSIGN:
        TRY(result, server_add_var(session->ses_env, op->op_select.op_sel_var,
                                   VAR_IDS, ids, NULL), cleanup_ids);
        result = 0;
        goto cleanup_col; // don't destroy ids
    case OP_SELECT_ALL:
    case OP_SELECT_RANGE:
    case OP_SELECT_VALUE:
        TRY(result, rpc_write_select_result(session->ses_fd, ids), cleanup_ids);
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
server_eval_fetch(struct session *session, struct op *op)
{
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_FETCH || op->op_type == OP_FETCH_ASSIGN);
    int result;
    struct column *col;
    TRY(result, column_open(session->ses_storage, op->op_fetch.op_fetch_col, &col), done);
    // find the variable representing the positions
    struct vartuple *v;
    TRYNULL(result, DBENOVAR, v,
            server_eval_get_var(session->ses_env, op->op_fetch.op_fetch_pos),
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
        TRY(result, rpc_write_fetch_result(session->ses_fd, vals), cleanup_vals);
        break;
    case OP_FETCH_ASSIGN:
        TRY(result, server_add_var(session->ses_env, op->op_fetch.op_fetch_var,
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
server_eval_agg(struct session *session, struct op *op) {
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_AGG);

    int result;

    // Try to find the column intermediate
    struct vartuple *v;
    TRYNULL(result, DBENOVAR, v,
            server_eval_get_var(session->ses_env, op->op_agg.op_agg_col),
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
        TRY(result, server_add_var(session->ses_env, op->op_agg.op_agg_var,
                                   VAR_VALS, NULL, aggval), cleanup_aggval);
        result = 0;
        goto done; // don't destroy aggval
    } else {
        TRY(result, rpc_write_fetch_result(session->ses_fd, aggval), cleanup_aggval);
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
server_eval_math(struct session *session, struct op *op) {
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_MATH);

    int result;

    // Try to find the column intermediates
    struct vartuple *vleft, *vright;
    TRYNULL(result, DBENOVAR, vleft,
            server_eval_get_var(session->ses_env, op->op_math.op_math_col1),
            done);
    TRYNULL(result, DBENOVAR, vright,
            server_eval_get_var(session->ses_env, op->op_math.op_math_col2),
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
        TRY(result, server_add_var(session->ses_env, op->op_math.op_math_var,
                                   VAR_VALS, NULL, mathvals), cleanup_mathval);
        result = 0;
        goto done; // don't destroy vals
    } else {
        TRY(result, rpc_write_fetch_result(session->ses_fd, mathvals), cleanup_mathval);
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
server_eval_print(struct session *session, struct op *op)
{
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_PRINT);

    int result;
    struct vartuple *v;
    TRYNULL(result, DBENOVAR, v,
            server_eval_get_var(session->ses_env, op->op_print.op_print_var),
            done);
    switch (v->vt_type) {
    case VAR_VALS:
        TRY(result, rpc_write_fetch_result(session->ses_fd, v->vt_column_vals), done);
        break;
    case VAR_IDS:
        TRY(result, rpc_write_select_result(session->ses_fd, v->vt_column_ids), done);
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
server_eval_insert(struct session *session, struct op *op)
{
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_INSERT);

    int result;
    struct column *col;
    TRY(result, column_open(session->ses_storage, op->op_insert.op_insert_col, &col), done);
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
server_eval_create(struct session *session, struct op *op)
{
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_CREATE);
    int result;
    TRY(result, storage_add_column(session->ses_storage, op->op_create.op_create_col,
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
server_eval_tuple(struct session *session, struct op *op)
{
    assert(session != NULL);
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
                server_eval_get_var(session->ses_env, pch), cleanup_valsarray);
        if (v->vt_type != VAR_VALS) {
            result = DBEVARTYPE;
            DBLOG(result);
            goto cleanup_valsarray;
        }
        TRY(result, column_valsarray_add(tuples, v->vt_column_vals, NULL), cleanup_valsarray);
        pch = strtok_r(NULL, delimiters, &saveptr);
    }
    TRY(result, rpc_write_tuple_result(session->ses_fd,
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
server_eval_join(struct session *session, struct op *op)
{
    assert(session != NULL);
    assert(op != NULL);
    assert(op->op_type == OP_JOIN);

    int result;

    // Try to find the column intermediates
    struct vartuple *inputL, *inputR;
    TRYNULL(result, DBENOVAR, inputL,
            server_eval_get_var(session->ses_env, op->op_join.op_join_inputL),
            done);
    TRYNULL(result, DBENOVAR, inputR,
            server_eval_get_var(session->ses_env, op->op_join.op_join_inputR),
            done);
    if (inputL->vt_type != VAR_VALS
        || inputR->vt_type != VAR_VALS
        || inputL->vt_column_vals->cval_ids == NULL
        || inputR->vt_column_vals->cval_ids == NULL) {
        result = DBEVARTYPE;
        DBLOG(result);
        goto done;
    }

    struct column_ids *idsL, *idsR;
    TRY(result, column_join(op->op_join.op_join_jtype,
                            session->ses_storage,
                            inputL->vt_column_vals,
                            inputR->vt_column_vals,
                            &idsL, &idsR), done);

    // Don't allow these to fail
    result = server_add_var(session->ses_env, op->op_join.op_join_varL,
                            VAR_IDS, idsL, NULL);
    assert(result == 0);
    result = server_add_var(session->ses_env, op->op_join.op_join_varR,
                            VAR_IDS, idsR, NULL);
    assert(result == 0);

    // success
    result = 0;
    goto done;
  done:
    return result;
}

static
int
server_eval(struct session *session, struct op *op)
{
    assert(session != NULL);
    assert(op != NULL);
    switch (op->op_type) {
    case OP_SELECT_ALL_ASSIGN:
    case OP_SELECT_RANGE_ASSIGN:
    case OP_SELECT_VALUE_ASSIGN:
    case OP_SELECT_ALL:
    case OP_SELECT_RANGE:
    case OP_SELECT_VALUE:
        return server_eval_select(session, op);
    case OP_FETCH:
    case OP_FETCH_ASSIGN:
        return server_eval_fetch(session, op);
    case OP_CREATE:
        return server_eval_create(session, op);
    case OP_LOAD:
        return server_eval_load(session, op);
    case OP_INSERT:
        return server_eval_insert(session, op);
    case OP_TUPLE:
        return server_eval_tuple(session, op);
    case OP_AGG:
        return server_eval_agg(session, op);
    case OP_MATH:
        return server_eval_math(session, op);
    case OP_PRINT:
        return server_eval_print(session, op);
    case OP_JOIN:
        return server_eval_join(session, op);
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
    struct session *sarg = (struct session *) arg;
    int clientfd = sarg->ses_fd;
    unsigned jobid = sarg->ses_jobid;
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
                        sarg->ses_storage->st_dbdir, jobid, loadid);
                loadid++;
                TRY(result, rpc_read_header(clientfd, &loadmsg), cleanup_op);
                assert(loadmsg.rpc_type = RPC_FILE);
                TRY(result, rpc_read_file(clientfd, &loadmsg, filenamebuf, &copyfd), cleanup_op);
                struct filetuple *ftuple;
                TRYNULL(result, DBENOMEM, ftuple, malloc(sizeof(struct filetuple)), cleanup_op);
                // TODO: file names are larger than ftuple char buf
                strcpy(ftuple->ft_name, op->op_load.op_load_file);
                ftuple->ft_fd = copyfd;
                result = filetuplearray_add(sarg->ses_files, ftuple, NULL);
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
        assert(result == 0);
        result = rpc_write_ok(clientfd);
        if (result) {
            goto cleanup_op;
        }
        free(op);
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
    session_destroy(sarg);
}

struct server *
server_create(struct server_options *options)
{
    assert(options != NULL);
    int result;
    struct server *s = NULL;
    TRYNULL(result, DBENOMEM, s, malloc(sizeof(struct server)), done);
    memcpy(&s->s_opt, options, sizeof(struct server_options));

    int listenfd;
    struct addrinfo hints, *servinfo;
    int yes = 1;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    char portbuf[16];
    sprintf(portbuf, "%d", s->s_opt.sopt_port);
    result = getaddrinfo(NULL, portbuf, &hints, &servinfo);
    if (result != 0) {
        result = DBEGETADDRINFO;
        DBLOG(result);
        goto cleanup_malloc;
    }

    // create a socket for listening for connections
    listenfd = socket(servinfo->ai_family, servinfo->ai_socktype,
                      servinfo->ai_protocol);
    if (listenfd == -1) {
        result = DBESOCKET;
        DBLOG(result);
        goto cleanup_malloc;
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
    result = listen(listenfd, s->s_opt.sopt_backlog);
    if (result == -1) {
        result = DBELISTEN;
        DBLOG(result);
        goto cleanup_listenfd;
    }
    s->s_listenfd = listenfd;

    // install a SIGINT handler for graceful shutdown
    struct sigaction sig;
    sig.sa_handler = sigint_handler;
    sig.sa_flags = 0;
    sigemptyset(&sig.sa_mask);
    result = sigaction( SIGINT, &sig, NULL );
    if (result == -1) {
        result = DBESIGACTION;
        DBLOG(result);
        goto done;
    }

    // init the storage directory
    TRYNULL(result, DBENOMEM, s->s_storage, storage_init(s->s_opt.sopt_dbdir),
            cleanup_listenfd);

    // create a threadpool to handle the connections
    TRYNULL(result, DBENOMEM, s->s_threadpool,
            threadpool_create(s->s_opt.sopt_nthreads), cleanup_storage);
    result = 0;
    goto done;

  cleanup_storage:
    storage_close(s->s_storage);
  cleanup_listenfd:
    assert(close(listenfd) == 0);
  cleanup_malloc:
    free(s);
    s = NULL;
  done:
    return s;
}

int
server_start(struct server *s)
{
    assert(s != NULL);
    int result;
    int listenfd = s->s_listenfd;

    // accept in a loop, waiting for more connections
    unsigned jobid = 0;
    while (errno != EINTR) {
        int acceptfd = accept(listenfd, NULL, NULL);
        if (acceptfd == -1) {
            result = DBEACCEPT;
            DBLOG(result);
            goto done;
        }

        // add the file descriptor as a job to the thread pool
        // if we can't add it, clean up the file descriptor.
        // if we are successful, the threadpool will handle
        // cleaning up the file descriptor
        struct session *sjob;
        TRYNULL(result, DBENOMEM, sjob,
                session_create(acceptfd, jobid++, s->s_storage),
                cleanup_acceptfd);

        struct job job;
        job.j_arg = (void *) sjob;
        job.j_routine = server_routine;
        TRY(result, threadpool_add_job(s->s_threadpool, &job), cleanup_sjob);
        continue;

      cleanup_sjob:
        free(sjob);
      cleanup_acceptfd:
        assert(close(acceptfd));
    }
  done:
    return result;
}

void
server_destroy(struct server *s)
{
    assert(s != NULL);
    threadpool_destroy(s->s_threadpool);
    storage_close(s->s_storage);
    assert(close(s->s_listenfd) == 0);
    free(s);
}
