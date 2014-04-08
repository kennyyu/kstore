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
#include "src/common/include/rpc.h"
#include "src/common/include/operators.h"
#include "src/common/include/parser.h"
#include "src/common/include/io.h"
#include "src/common/include/dberror.h"
#include "src/common/include/try.h"
#include <readline/readline.h>
#include <readline/history.h>

// boolean to tell the client to keep looping
static bool volatile keep_running = true;

static
void
sigint_handler(int sig)
{
    (void) sig;
    keep_running = false;
    fprintf(stderr, "Caught shutdown signal, shutting down client...\n");
}

#define PORT 5000
#define HOST "localhost"
#define LOADDIR "p2tests"

static struct {
    int copt_port;
    char copt_host[128];
    char copt_loaddir[128];
    int copt_interactive;
} client_options = {
    .copt_port = PORT,
    .copt_interactive = 0,
    .copt_host = HOST,
    .copt_loaddir = LOADDIR,
};

const char *short_options = "h";

const struct option long_options[] = {
    {"help", no_argument, NULL, 'h'},
    {"port", required_argument, &client_options.copt_port, 0},
    {"host", required_argument, NULL, 0},
    {"loaddir", required_argument,  NULL, 0},
    {"interactive", no_argument, &client_options.copt_interactive, 1},
    {NULL, 0, NULL, 0}
};

#define BUFSIZE 4096

static
int
parse_stdin_string(int sockfd, char *s)
{
    int result;
    struct oparray *ops;
    TRYNULL(result, DBEPARSE, ops, parse_query(s), done);

    for (unsigned i = 0; i < oparray_num(ops); i++) {
        struct op *op = oparray_get(ops, i);
        TRY(result, rpc_write_query(sockfd, op), cleanup_ops);
        if (op->op_type == OP_LOAD) {
            char loadfilebuf[128];
            sprintf(loadfilebuf, "%s/%s", client_options.copt_loaddir,
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
client_handle_fetch(int readfd, int writefd, struct rpc_header *msg)
{
    (void) writefd;
    assert(msg->rpc_type == RPC_FETCH_RESULT);
    int *vals = NULL;
    unsigned nvals;
    int result;
    TRY(result, rpc_read_fetch_result(readfd, msg, &vals, &nvals), done);
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
client_handle_select(int readfd, int writefd, struct rpc_header *msg)
{
    (void) writefd;
    assert(msg->rpc_type == RPC_SELECT_RESULT);
    unsigned *ids = NULL;
    unsigned nids;
    int result;
    TRY(result, rpc_read_select_result(readfd, msg, &ids, &nids), done);
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
client_handle_error(int readfd, int writefd, struct rpc_header *msg)
{
    (void) writefd;
    assert(msg->rpc_type == RPC_ERROR);
    char *error = NULL;
    int result;
    TRY(result, rpc_read_error(readfd, msg, &error), done);
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
client_handle_tuple(int readfd, int writefd, struct rpc_header *msg)
{
    (void) writefd;
    assert(msg->rpc_type == RPC_TUPLE_RESULT);
    int *tuple = NULL;
    unsigned len;
    int result;
    TRY(result, rpc_read_tuple_result(readfd, msg, &tuple, &len), done);
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
parse_sockfd(int readfd, int writefd)
{
    (void) writefd;
    int result;
    struct rpc_header msg;

    // We keep looping until we get an OK, ERROR, or TERMINATE message
    while (1) {
        bzero(&msg, sizeof(struct rpc_header));
        TRY(result, rpc_read_header(readfd, &msg), done);
        switch (msg.rpc_type) {
        case RPC_OK:
            result = 0;
            goto done;
        case RPC_ERROR:
            result = client_handle_error(readfd, writefd, &msg);
            break;
        case RPC_TERMINATE:
            if (client_options.copt_interactive) {
                fprintf(stderr, "Received TERMINATE from server\n");
            }
            result = DBESERVERTERM;
            break;
        case RPC_FETCH_RESULT:
            result = client_handle_fetch(readfd, writefd, &msg);
            break;
        case RPC_SELECT_RESULT:
            result = client_handle_select(readfd, writefd, &msg);
            break;
        case RPC_TUPLE_RESULT:
            result = client_handle_tuple(readfd, writefd, &msg);
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
parse_stdin_interactive(int sockfd)
{
    int result;
    char *prompt = ">>> ";
    char *input;

    TRYNULL(result, DBEIOEARLYEOF, input, readline(prompt), done);
    if (*input) {
        add_history(input);
        TRY(result, parse_stdin_string(sockfd, input), cleanup_input);
    }

    result = 0;
    goto cleanup_input;

  cleanup_input:
    free(input);
  done:
    return result;
}

static
int
client_interactive(int sockfd)
{
    int result;
    while (keep_running) {
        result = parse_stdin_interactive(sockfd);
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
        // wait for response from server
        //printf("\r     \r");
        //fflush(stdout);
        TRY(result, parse_sockfd(sockfd, STDOUT_FILENO), done);
    }
    result = 0;
    goto done;
  done:
    return result;
}

static
int
parse_stdin_batch(int sockfd)
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
    TRY(result, parse_stdin_string(sockfd, buf), done);

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
client_batch(int sockfd)
{
    int result;
    bool read_stdin = true;
    bool read_socket = true;
    while (keep_running && (read_stdin || read_socket)) {
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
            result = parse_stdin_batch(sockfd);
            if (result) {
                // if stdin is done, send a connection termination message
                // to the server
                read_stdin = false;
                (void) rpc_write_terminate(sockfd);
            }
        }

        // if we get something from the socket, parse it and write it to stdout
        if (read_socket && FD_ISSET(sockfd, &readfds)) {
            result = parse_sockfd(sockfd, STDOUT_FILENO);
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
                if (strcmp(long_options[option_index].name, "host") == 0) {
                    strcpy(client_options.copt_host, optarg);
                } else if (strcmp(long_options[option_index].name, "loaddir") == 0) {
                    strcpy(client_options.copt_loaddir, optarg);
                } else {
                    *(long_options[option_index].flag) = atoi(optarg);
                }
            }
            break;
        case 'h':
            printf("Usage: %s\n", argv[0]);
            printf("--help\n");
            printf("--port P        [default=5000]\n");
            printf("--host H        [default=localhost]\n");
            printf("--loaddir dir    [default=p2tests]\n");
            printf("--interactive\n");
            return 0;
        }
    }
    if (client_options.copt_interactive) {
        printf("port: %d, host: %s, loaddir: %s, interactive: %d\n",
               client_options.copt_port, client_options.copt_host,
               client_options.copt_loaddir, client_options.copt_interactive);
    }

    int result;
    int sockfd;
    struct addrinfo hints, *servinfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char portbuf[16];
    sprintf(portbuf, "%d", client_options.copt_port);
    result = getaddrinfo(client_options.copt_host, portbuf, &hints, &servinfo);
    if (result != 0) {
        result = DBEGETADDRINFO;
        DBLOG(result);
        goto done;
    }

    // create a socket to connect to the server
    sockfd = socket(servinfo->ai_family, servinfo->ai_socktype,
                    servinfo->ai_protocol);
    if (sockfd == -1) {
        result = DBESOCKET;
        DBLOG(result);
        goto done;
    }
    freeaddrinfo(servinfo);

    // wait for a connection to the server
    result = connect(sockfd, servinfo->ai_addr, servinfo->ai_addrlen);
    if (result == -1) {
        result = DBECONNECT;
        DBLOG(result);
        goto cleanup_sockfd;
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

    if (client_options.copt_interactive) {
        result = client_interactive(sockfd);
    } else {
        result = client_batch(sockfd);
    }

  cleanup_sockfd:
    (void) rpc_write_terminate(sockfd);
    assert(close(sockfd) == 0);
  done:
    return result;
}
