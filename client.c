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
parse_stdin(int readfd, int writefd)
{
    int result;
    char buf[TUPLELEN];
    bzero(buf, TUPLELEN);
    result = 0;
    unsigned ix = 0;
    while (1) {
        result = read(readfd, buf + ix, 1);
        if (result == -1 || result == 0) {
            result = DBEIOCHECKERRNO;
            goto done;
        }
        ix++;
        if (buf[ix-1] == '\n' || buf[ix-1] == '\0') {
            break;
        }
    }
    struct oparray *ops;
    TRYNULL(result, DBEPARSE, ops, parse_query(buf), done);

    for (unsigned i = 0; i < oparray_num(ops); i++) {
        struct op *op = oparray_get(ops, i);
        TRY(result, rpc_write_query(writefd, op), cleanup_ops);
        if (op->op_type == OP_LOAD) {
            char loadfilebuf[128];
            sprintf(loadfilebuf, "%s/%s", client_options.copt_loaddir,
                    op->op_load.op_load_file);
            strcpy(op->op_load.op_load_file, loadfilebuf);
            TRY(result, rpc_write_file(writefd, op), cleanup_ops);
        }
    }
    // success
    result = 0;
  cleanup_ops:
    parse_cleanup_ops(ops);
  done:
    if (!dberror_client_is_fatal(result)) {
        result = DBSUCCESS;
    }
    return result;
}

static
int
client_handle_fetch(int readfd, int writefd, struct rpc_header *msg)
{
    (void) writefd;
    assert(msg->rpc_type == RPC_FETCH_RESULT);
    int *vals = NULL;
    int nvals;
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
client_handle_error(int readfd, int writefd, struct rpc_header *msg)
{
    (void) writefd;
    assert(msg->rpc_type == RPC_ERROR);
    char *error = NULL;
    int result;
    TRY(result, rpc_read_error(readfd, msg, &error), done);
    assert(error != NULL);
    printf("ERROR: %s\n", error);
    free(error);

    result = 0;
    goto done;
  done:
    return result;
}

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
    TRY(result, rpc_read_header(readfd, &msg), done);
    switch (msg.rpc_type) {
    case RPC_TERMINATE:
        if (client_options.copt_interactive) {
            fprintf(stderr, "Received TERMINATE from server\n");
        }
        result = DBESERVERTERM;
        break;
    case RPC_FETCH_RESULT:
        result = client_handle_fetch(readfd, writefd, &msg);
        break;
    case RPC_TUPLE_RESULT:
        result = client_handle_tuple(readfd, writefd, &msg);
        break;
    case RPC_ERROR:
        result = client_handle_error(readfd, writefd, &msg);
        break;
    default:
        assert(0);
        break;
    }
    if (result) {
        goto done;
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

    bool read_stdin = true;
    bool read_socket = true;
    while (keep_running && (read_stdin || read_socket)) {
        fd_set readfds;
        FD_ZERO(&readfds);
        if (read_stdin) {
            FD_SET(STDIN_FILENO, &readfds);
            if (client_options.copt_interactive) {
                printf(">>> ");
                fflush(stdout);
            }
        }
        if (read_socket) {
            FD_SET(sockfd, &readfds);
        }
        result = select(sockfd + 1, &readfds, NULL, NULL, NULL);
        if (result == -1) {
            result = DBESELECT;
            DBLOG(result);
            goto cleanup_sockfd;
        }

        // if we get something from stdin, parse it and write it to the socket
        if (read_stdin && FD_ISSET(STDIN_FILENO, &readfds)) {
            result = parse_stdin(STDIN_FILENO, sockfd);
            if (result) {
                if (client_options.copt_interactive) {
                    fprintf(stderr, "ERROR: client error\n");
                }
                // if stdin is done, send a connection termination message
                // to the server
                read_stdin = false;
                (void) rpc_write_terminate(sockfd);
            }
        }

        // if we get something from the socket, parse it and write it to stdout
        if (read_socket && FD_ISSET(sockfd, &readfds)) {
            if (client_options.copt_interactive) {
                printf("\r     \r");
                fflush(stdout);
            }
            result = parse_sockfd(sockfd, STDOUT_FILENO);
            if (result) {
                read_socket = false;
            }
        }
    }

  cleanup_sockfd:
    (void) rpc_write_terminate(sockfd);
    assert(close(sockfd) == 0);
  done:
    return result;
}
