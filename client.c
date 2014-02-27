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

#define PORT "5000"
#define HOST "localhost"

#define BUFSIZE 4096

static
int copy_file(int readfd, int writefd, uint64_t total_expected)
{
    uint64_t total;
    int nr, nw;
    char buf[BUFSIZE];
    while ((nr = read(readfd, buf, BUFSIZE)) > 0) {
        nw = write(writefd, buf, nr);
        assert(nw == nr);
        total += nr;
    }
    if (nr == 0) {
        assert(total == total_expected);
        return 0;
    } else {
        return nr;
    }
}

static
uint64_t file_size(int fd)
{
    struct stat buf;
    int result =fstat(fd, &buf);
    assert(result == 0);
    return buf.st_size;
}

// return -1 on EOF or error, 0 on success
static
int
parse_stdin(int readfd, int writefd)
{
    int result;
    char buf[BUFSIZE];
    struct db_message msg;
    result = read(readfd, buf, BUFSIZE);
    if (result == 0 || result == -1) {
        return -1;
    }
    struct oparray *ops = parse_query(buf);
    if (ops == NULL) {
        result = -1;
        goto done;
    }
    for (unsigned i = 0; i < oparray_num(ops); i++) {
        struct op *op = oparray_get(ops, i);
        char *query = op_string(op);
        msg.dbm_type = DB_MESSAGE_QUERY;
        msg.dbm_magic = DB_MESSAGE_MAGIC;
        msg.dbm_len = strlen(query) + 1; // +1 for the null byte
        result = dbm_write(writefd, &msg);
        if (result) {
            goto cleanup_query;
        }
        // TODO use robust io
        assert(msg.dbm_len == write(writefd, query, msg.dbm_len + 1));

        // if we have a load type, open the file and send it to the server
        if (op->op_type == OP_LOAD) {
            char *fname = op->op_load.op_load_file;
            int loadfd = open(fname, O_RDONLY);
            if (loadfd == -1) {
                goto cleanup_query;
            }
            msg.dbm_type = DB_MESSAGE_FILE;
            msg.dbm_magic = DB_MESSAGE_MAGIC;
            msg.dbm_len = file_size(loadfd);
            result = dbm_write(writefd, &msg);
            if (result) {
                goto cleanup_loadfd;
            }
            result = copy_file(loadfd, writefd, msg.dbm_len);
            if (result) {
                goto cleanup_loadfd;
            }
          cleanup_loadfd:
            assert(close(loadfd) == 0);
            goto cleanup_query;
        }
        continue;

      cleanup_query:
        free(query);
        goto cleanup_ops;
    }

    result = 0;
  cleanup_ops:
    parse_cleanup_ops(ops);
  done:
    return result;
}



// return 0 on EOF
static
int
parse_sockfd(int readfd, int writefd)
{
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
        // TODO use robust io
        assert(msg.dbm_len % 4 == 0);
        payload = malloc(sizeof(char) * msg.dbm_len);
        result = read(readfd, payload, msg.dbm_len);
        assert(result == msg.dbm_len);
        fetch = (int *) payload;
        for (unsigned i = 0; i < msg.dbm_len / 4; i++) {
            printf("%d\n", fetch[i]);
        }
        break;
    case DB_MESSAGE_ERROR:
        // TODO use robust io
        payload = malloc(sizeof(char) * msg.dbm_len);
        result = read(readfd, payload, msg.dbm_len);
        assert(result == msg.dbm_len);
        result = write(writefd, payload, msg.dbm_len);
        assert(result == msg.dbm_len);
        break;
    default:
        assert(0);
        break;
    }

  done:
    return result;
}

int
main(void)
{
    printf("hello client\n");

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

    // TODO: send query and read response
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
    //char buf[1024];
    //read(sockfd, buf, sizeof(buf));
    //printf("%s\n", buf);
    //result = 0;
    //goto cleanup_sockfd;

  cleanup_sockfd:
    assert(close(sockfd) == 0);
  done:
    return result;
}
