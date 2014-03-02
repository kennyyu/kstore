#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <unistd.h>
#include "src/common/include/db_message.h"
#include "src/common/include/operators.h"
#include "src/common/include/parser.h"
#include "src/common/include/io.h"

#define PORT "5000"
#define HOST "localhost"

#define BUFSIZE 4096

static
int
parse_stdin(int readfd, int writefd)
{
    int result;
    char buf[BUFSIZE];
    bzero(buf, BUFSIZE);
    result = read(readfd, buf, BUFSIZE); // read at most BUFSIZE
    if (result == -1 || result == 0) {
        result = -1;
        goto done;
    }
    struct oparray *ops = parse_query(buf);
    if (ops == NULL) {
        result = -1;
        goto done;
    }
    for (unsigned i = 0; i < oparray_num(ops); i++) {
        struct op *op = oparray_get(ops, i);
        result = rpc_write_query(writefd, op);
        if (result) {
            goto cleanup_ops;
        }
        if (op->op_type == OP_LOAD) {
            result = rpc_write_file(writefd, op);
            if (result) {
                goto cleanup_ops;
            }
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
client_handle_result(int readfd, int writefd, struct rpc_header *msg)
{
    (void) writefd;
    assert(msg->rpc_type == RPC_FETCH_RESULT);
    int *vals = NULL;
    int nvals;
    int result = rpc_read_fetch_result(readfd, msg, &vals, &nvals);
    if (result) {
        goto done;
    }
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
    int result = rpc_read_error(readfd, msg, &error);
    if (result) {
        goto done;
    }
    assert(error != NULL);
    printf("ERROR: %s\n", error);
    free(error);

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
    result = rpc_read_header(readfd, &msg);
    if (result) {
        goto done;
    }
    switch (msg.rpc_type) {
    case RPC_TERMINATE:
        result = -1;
        break;
    case RPC_FETCH_RESULT:
        result = client_handle_result(readfd, writefd, &msg);
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
    return result;
}

int
main(void)
{
    int result;
    int sockfd;
    struct addrinfo hints, *servinfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    result = getaddrinfo(HOST, PORT, &hints, &servinfo);
    if (result != 0) {
        perror("getaddrinfo");
        result = -1;
        goto done;
    }

    // create a socket to connect to the server
    sockfd = socket(servinfo->ai_family, servinfo->ai_socktype,
                    servinfo->ai_protocol);
    if (sockfd == -1) {
        perror("socket");
        result = -1;
        goto done;
    }
    freeaddrinfo(servinfo);

    // wait for a connection to the server
    result = connect(sockfd, servinfo->ai_addr, servinfo->ai_addrlen);
    if (result == -1) {
        perror("connect");
        goto cleanup_sockfd;
    }

    bool read_stdin = true;
    bool read_socket = true;
    while (read_stdin || read_socket) {
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
            perror("select");
            goto cleanup_sockfd;
        }

        // if we get something from stdin, parse it and write it to the socket
        if (read_stdin && FD_ISSET(STDIN_FILENO, &readfds)) {
            result = parse_stdin(STDIN_FILENO, sockfd);
            if (result) {
                // if stdin is done, send a connection termination message
                // to the server
                read_stdin = false;
                result = rpc_write_terminate(sockfd);
                if (result) {
                    goto cleanup_sockfd;
                }
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

  cleanup_sockfd:
    assert(close(sockfd) == 0);
  done:
    return result;
}
