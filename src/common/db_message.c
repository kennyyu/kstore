#include <arpa/inet.h>
#include <assert.h>
#include <stdint.h>
#include <stddef.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include "include/db_message.h"
#include "include/io.h"
#include "include/operators.h"
#include "include/parser.h"

// 64 bit conversion taken from:
// http://stackoverflow.com/questions/809902/64-bit-ntohl-in-c
static
uint64_t
ntoh64(const uint64_t input)
{
    uint64_t rval;
    uint8_t *data = (uint8_t *)&rval;

    data[0] = input >> 56;
    data[1] = input >> 48;
    data[2] = input >> 40;
    data[3] = input >> 32;
    data[4] = input >> 24;
    data[5] = input >> 16;
    data[6] = input >> 8;
    data[7] = input >> 0;

    return rval;
}

static
uint64_t
hton64(const uint64_t input)
{
    return (ntoh64(input));
}

int
dbm_write(int fd, struct db_message *message)
{
    assert(message != NULL);
    assert(message->dbm_magic == DB_MESSAGE_MAGIC);

    int result;
    struct db_message networkmsg;
    networkmsg.dbm_type = htonl(message->dbm_type);
    networkmsg.dbm_magic = htonl(message->dbm_magic);
    networkmsg.dbm_len = hton64(message->dbm_len);
    result = io_write(fd, (void *) &networkmsg, sizeof(struct db_message));
    if (result) {
        goto done;
    }
    result = 0;
  done:
    return result;
}

int
dbm_read(int fd, struct db_message *message)
{
    assert(message != NULL);

    int result;
    struct db_message networkmsg;
    result = io_read(fd, (void *) &networkmsg, sizeof(struct db_message));
    if (result) {
        goto done;
    }
    message->dbm_type = ntohl(networkmsg.dbm_type);
    message->dbm_magic = ntohl(networkmsg.dbm_magic);
    message->dbm_len = ntoh64(networkmsg.dbm_len);
    assert(message->dbm_magic == DB_MESSAGE_MAGIC);
    result = 0;
  done:
    return result;
}

int
dbm_write_query(int fd, struct op *op)
{
    assert(op != NULL);
    int result;
    char *query = op_string(op);
    struct db_message msg;
    msg.dbm_type = DB_MESSAGE_QUERY;
    msg.dbm_magic = DB_MESSAGE_MAGIC;
    msg.dbm_len = strlen(query) + 1; // +1 for the null byte
    result = dbm_write(fd, &msg);
    if (result) {
        goto cleanup_query;
    }
    result = io_write(fd, query, msg.dbm_len);
    if (result) {
        goto cleanup_query;
    }
    result = 0;
    goto cleanup_query;

  cleanup_query:
    free(query);
    return result;
}

int
dbm_read_query(int fd, struct op **retop)
{
    assert(retop != NULL);

    int result;
    struct db_message msg;
    result = dbm_read(fd, &msg);
    if (result) {
        goto done;
    }
    assert(msg.dbm_type == DB_MESSAGE_QUERY);
    char *payload = malloc(sizeof(char) * msg.dbm_len); // includes null byte
    if (payload == NULL) {
        goto done;
    }
    result = io_read(fd, payload, msg.dbm_len);
    if (result) {
        goto cleanup_payload;
    }
    struct op *op = parse_line(payload);
    if (op == NULL) {
        result = -1;
        goto cleanup_payload;
    }

    // success
    printf("got query: [%s]\n", payload);
    *retop = op;
    result = 0;
  cleanup_payload:
    free(payload);
  done:
    return result;
}

int
dbm_read_file(int fd, unsigned curfileid, int *retfd)
{
    assert(retfd != NULL);

    int result;
    struct db_message msg;
    result = dbm_read(fd, &msg);
    if (result) {
        goto done;
    }
    char buf[32];
    bzero(buf, sizeof(buf));
    sprintf(buf, "%u.tmp", curfileid);
    int copyfd = open(buf, O_CREAT | O_RDWR | O_TRUNC, S_IRWXU);
    if (copyfd == -1) {
        result = -1;
        goto done;
    }
    result = io_copy(fd, copyfd, msg.dbm_len);
    if (result) {
        goto cleanup_copyfd;
    }
    // seek back to beginning
    result = lseek(copyfd, 0, SEEK_SET);
    if (result) {
        goto cleanup_copyfd;
    }

    // success
    printf("got file: %s\n", buf);
    *retfd = copyfd;
    result = 0;
    goto done;
  cleanup_copyfd:
    assert(close(copyfd) == 0);
  done:
    return result;
}

int
dbm_write_file(int fd, struct op *op)
{
    assert(op != NULL);
    assert(op->op_type = OP_LOAD);

    int result;
    struct db_message msg;
    char *fname = op->op_load.op_load_file;
    int loadfd = open(fname, O_RDONLY);
    if (loadfd == -1) {
        result = -1;
        goto done;
    }
    msg.dbm_type = DB_MESSAGE_FILE;
    msg.dbm_magic = DB_MESSAGE_MAGIC;
    msg.dbm_len = io_size(loadfd);
    result = dbm_write(fd, &msg);
    if (result) {
        goto cleanup_loadfd;
    }
    result = io_copy(loadfd, fd, msg.dbm_len);
    if (result) {
        goto cleanup_loadfd;
    }
    result = 0;
    goto cleanup_loadfd;

  cleanup_loadfd:
    assert(close(loadfd) == 0);
  done:
    return result;
}
