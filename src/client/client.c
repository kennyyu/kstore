#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <unistd.h>
#include "../common/include/try.h"
#include "../common/include/dberror.h"
#include "../common/include/rpc.h"
#include "../common/include/operators.h"
#include "../common/include/parser.h"
#include "../common/include/io.h"
#include "../common/include/dberror.h"
#include "../common/include/try.h"
#include "include/client.h"
#include <readline/readline.h>
#include <readline/history.h>

struct client {
    struct client_options c_opt;
    int c_sockfd;
//    volatile bool c_keep_running;
};

static
void
sigint_handler(int sig)
{
    (void) sig;
    fprintf(stderr, "Caught shutdown signal, shutting down client...\n");
}

#define BUFSIZE 4096

static
int
client_handle_fetch(int sockfd, struct rpc_header *msg)
{
    assert(msg->rpc_type == RPC_FETCH_RESULT);
    int *vals = NULL;
    unsigned nvals;
    int result;
    TRY(result, rpc_read_fetch_result(sockfd, msg, &vals, &nvals), done);
    assert(vals != NULL);
    for (unsigned i = 0; i < nvals; i++) {
        printf("%d\n", vals[i]);
    }
    free(vals);

    result = 0;
    goto done;

  done:
    return result;
}

static
int
client_handle_select(int sockfd, struct rpc_header *msg)
{
    assert(msg->rpc_type == RPC_SELECT_RESULT);
    unsigned *ids = NULL;
    unsigned nids;
    int result;
    TRY(result, rpc_read_select_result(sockfd, msg, &ids, &nids), done);
    assert(ids != NULL);
    for (unsigned i = 0; i < nids; i++) {
        printf("%d\n", ids[i]);
    }
    free(ids);

    result = 0;
    goto done;

  done:
    return result;
}

static
int
client_handle_error(int sockfd, struct rpc_header *msg)
{
    assert(msg->rpc_type == RPC_ERROR);
    char *error = NULL;
    int result;
    TRY(result, rpc_read_error(sockfd, msg, &error), done);
    assert(error != NULL);
    printf("[SERVER ERROR: %s]\n", error);
    free(error);

    result = 0;
    goto done;
  done:
    return result;
}

static
int
client_handle_tuple(int sockfd, struct rpc_header *msg)
{
    assert(msg->rpc_type == RPC_TUPLE_RESULT);
    int *tuple = NULL;
    unsigned len;
    int result;
    TRY(result, rpc_read_tuple_result(sockfd, msg, &tuple, &len), done);
    assert(tuple != NULL);
    printf("(");
    for (unsigned i = 0; i < len - 1; i++) {
        printf("%d,", tuple[i]);
    }
    printf("%d)\n", tuple[len - 1]);
    free(tuple);

    result = 0;
    goto done;

  done:
    return result;
}

static
int
parse_sockfd(struct client *c)
{
    int result;
    int sockfd = c->c_sockfd;
    struct rpc_header msg;

    // We keep looping until we get an OK, ERROR, or TERMINATE message
    while (1) {
        bzero(&msg, sizeof(struct rpc_header));
        TRY(result, rpc_read_header(sockfd, &msg), done);
        switch (msg.rpc_type) {
        case RPC_OK:
            result = 0;
            goto done;
        case RPC_ERROR:
            result = client_handle_error(sockfd, &msg);
            goto done;
        case RPC_TERMINATE:
            if (c->c_opt.copt_interactive) {
                fprintf(stderr, "Received TERMINATE from server\n");
            }
            result = DBESERVERTERM;
            break;
        case RPC_FETCH_RESULT:
            result = client_handle_fetch(sockfd, &msg);
            break;
        case RPC_SELECT_RESULT:
            result = client_handle_select(sockfd, &msg);
            break;
        case RPC_TUPLE_RESULT:
            result = client_handle_tuple(sockfd, &msg);
            break;
        default:
            assert(0);
            break;
        }
        if (result) {
            goto done;
        }
    }

    // success
    result = 0;
    goto done;
  done:
    if (!dberror_client_is_fatal(result)) {
        result = DBSUCCESS;
    }
    return result;
}

static
int
parse_stdin_string(struct client *c, char *s)
{
    int result;
    struct oparray *ops;
    int sockfd = c->c_sockfd;
    TRYNULL(result, DBEPARSE, ops, parse_query(s), done);

    for (unsigned i = 0; i < oparray_num(ops); i++) {
        struct op *op = oparray_get(ops, i);
        TRY(result, rpc_write_query(sockfd, op), cleanup_ops);
        if (op->op_type == OP_LOAD) {
            char loadfilebuf[128];
            sprintf(loadfilebuf, "%s/%s", c->c_opt.copt_loaddir,
                    op->op_load.op_load_file);
            strcpy(op->op_load.op_load_file, loadfilebuf);
            TRY(result, rpc_write_file(sockfd, op), cleanup_ops);
        }
    }
    // success
    result = 0;
  cleanup_ops:
    parse_cleanup_ops(ops);
  done:
    return result;
}

static
int
parse_stdin_interactive(struct client *c)
{
    int result;
    char *prompt = ">>> ";
    char *input;

    TRYNULL(result, DBEIOEARLYEOF, input, readline(prompt), done);
    if (input[0] == '\0' || input[0] == '\n' || input[0] == '\r') {
        result = DBEPARSE;
        DBLOG(result);
        goto cleanup_input;
    }
    add_history(input);
    TRY(result, parse_stdin_string(c, input), cleanup_input);

    result = 0;
    goto cleanup_input;

  cleanup_input:
    free(input);
  done:
    return result;
}

static
int
client_interactive(struct client *c)
{
    int result;
    int sockfd = c->c_sockfd;
    rl_getc_function = getc;
    while (errno != EINTR) {
        result = parse_stdin_interactive(c);
        if (result) {
            DBLOG(result);
            if (!dberror_client_is_fatal(result)) {
                result = DBSUCCESS;
                continue;
            } else {
                (void) rpc_write_terminate(sockfd);
                goto done;
            }
        }
        TRY(result, parse_sockfd(c), done);
    }
    result = 0;
    goto done;
  done:
    return result;
}

static
int
parse_stdin_batch(struct client *c)
{
    int result;
    char buf[TUPLELEN];
    bzero(buf, TUPLELEN);
    result = 0;
    unsigned ix = 0;
    while (1) {
        result = read(STDIN_FILENO, buf + ix, 1);
        if (result == -1 || result == 0) {
            result = DBEIOCHECKERRNO;
            goto done;
        }
        ix++;
        if (buf[ix-1] == '\n' || buf[ix-1] == '\0') {
            break;
        }
    }
    TRY(result, parse_stdin_string(c, buf), done);

    result = 0;
    goto done;
  done:
    if (!dberror_client_is_fatal(result)) {
        result = DBSUCCESS;
    }
    return result;
}

static
int
client_batch(struct client *c)
{
    int result;
    int sockfd = c->c_sockfd;
    bool read_stdin = true;
    bool read_socket = true;
    while (errno != EINTR && (read_stdin || read_socket)) {
        fd_set readfds;
        FD_ZERO(&readfds);
        if (read_stdin) {
            FD_SET(STDIN_FILENO, &readfds);
        }
        if (read_socket) {
            FD_SET(sockfd, &readfds);
        }
        result = select(sockfd + 1, &readfds, NULL, NULL, NULL);
        if (result == -1) {
            result = DBESELECT;
            DBLOG(result);
            goto done;
        }

        // if we get something from stdin, parse it and write it to the socket
        if (read_stdin && FD_ISSET(STDIN_FILENO, &readfds)) {
            result = parse_stdin_batch(c);
            if (result) {
                // if stdin is done, send a connection termination message
                // to the server
                read_stdin = false;
                (void) rpc_write_terminate(sockfd);
            }
        }

        // if we get something from the socket, parse it and write it to stdout
        if (read_socket && FD_ISSET(sockfd, &readfds)) {
            result = parse_sockfd(c);
            if (result) {
                read_socket = false;
            }
        }
    }
    result = 0;
    goto done;
  done:
    return result;
}

struct client *
client_create(struct client_options *options)
{
    assert(options != NULL);

    int result;
    struct client *c = NULL;
    TRYNULL(result, DBENOMEM, c, malloc(sizeof(struct client)), done);
    memcpy(&c->c_opt, options, sizeof(struct client_options));

    int sockfd;
    struct addrinfo hints, *servinfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char portbuf[16];
    sprintf(portbuf, "%d", c->c_opt.copt_port);
    result = getaddrinfo(c->c_opt.copt_host, portbuf, &hints, &servinfo);
    if (result != 0) {
        result = DBEGETADDRINFO;
        DBLOG(result);
        goto cleanup_malloc;
    }

    // create a socket to connect to the server
    sockfd = socket(servinfo->ai_family, servinfo->ai_socktype,
                    servinfo->ai_protocol);
    if (sockfd == -1) {
        result = DBESOCKET;
        DBLOG(result);
        goto cleanup_malloc;
    }
    freeaddrinfo(servinfo);

    // wait for a connection to the server
    result = connect(sockfd, servinfo->ai_addr, servinfo->ai_addrlen);
    if (result == -1) {
        result = DBECONNECT;
        DBLOG(result);
        goto cleanup_sockfd;
    }
    c->c_sockfd = sockfd;

    // install a SIGINT handler for graceful shutdown
    struct sigaction sig;
    sig.sa_handler = sigint_handler;
    sig.sa_flags = 0;
    sigemptyset(&sig.sa_mask);
    result = sigaction( SIGINT, &sig, NULL );
    if (result == -1) {
        result = DBESIGACTION;
        DBLOG(result);
        goto cleanup_sockfd;
    }
    result = 0;
    goto done;

  cleanup_sockfd:
    (void) rpc_write_terminate(sockfd);
    assert(close(sockfd) == 0);
  cleanup_malloc:
    free(c);
    c = NULL;
  done:
    return c;
}

int
client_start(struct client *c)
{
    assert(c != NULL);
    // Start a client in batch or interactive mode
    int result;
    if (c->c_opt.copt_interactive) {
        result = client_interactive(c);
    } else {
        result = client_batch(c);
    }
    if (result) {
        DBLOG(result);
    }
    return result;
}

void
client_destroy(struct client *c)
{
    assert(c != NULL);
    (void) rpc_write_terminate(c->c_sockfd);
    assert(close(c->c_sockfd) == 0);
    free(c);
}
