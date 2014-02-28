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
        result = dbm_write_query(writefd, op);
        if (result) {
            goto cleanup_ops;
        }
        if (op->op_type == OP_LOAD) {
            result = dbm_write_file(writefd, op);
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
parse_sockfd(int readfd, int writefd)
{
    (void) writefd;
    int result;
    struct db_message msg;
    char *payload;
    int *fetch;
    result = dbm_read(readfd, &msg);
    if (result) {
        goto done;
    }
    switch (msg.dbm_type) {
    case DB_MESSAGE_FETCH_RESULT:
        // print all the fetched ints, one per line
        assert(msg.dbm_len % 4 == 0);
        payload = malloc(sizeof(char) * msg.dbm_len);
        if (payload == NULL) {
            result = ENOMEM;
            goto done;
        }
        result = io_read(readfd, payload, msg.dbm_len);
        if (result) {
            goto cleanup_payload;
        }
        fetch = (int *) payload;
        for (unsigned i = 0; i < msg.dbm_len / 4; i++) {
            printf("%d\n", fetch[i]);
        }
        break;
    case DB_MESSAGE_ERROR:
        // print the error message, dbm_len includes space for null character
        payload = malloc(sizeof(char) * msg.dbm_len);
        if (payload == NULL) {
            result = ENOMEM;
            goto done;
        }
        result = io_read(readfd, payload, msg.dbm_len);
        if (result) {
            goto cleanup_payload;
        }
        printf("%s\n", payload);
        break;
    default:
        assert(0);
        break;
    }

  cleanup_payload:
    free(payload);
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

    while (1) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(STDIN_FILENO, &readfds);
        FD_SET(sockfd, &readfds);
        result = select(sockfd + 1, &readfds, NULL, NULL, NULL);
        if (result == -1) {
            perror("select");
            goto cleanup_sockfd;
        }

        // if we get something from stdin, parse it and write it to the socket
        if (FD_ISSET(STDIN_FILENO, &readfds)) {
            result = parse_stdin(STDIN_FILENO, sockfd);
            if (result) {
                goto cleanup_sockfd;
            }
        }

        // if we get something from the socket, parse it and write it to stdout
        if (FD_ISSET(sockfd, &readfds)) {
            result = parse_sockfd(sockfd, STDOUT_FILENO);
            if (result) {
                goto cleanup_sockfd;
            }
        }
    }

  cleanup_sockfd:
    assert(close(sockfd) == 0);
  done:
    return result;
}
