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
#include "include/rpc.h"
#include "include/io.h"
#include "include/try.h"
#include "include/operators.h"
#include "include/parser.h"
#include "include/dberror.h"
#include "include/array.h"
#include "include/results.h"

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
rpc_write_header(int fd, struct rpc_header *message)
{
    assert(message != NULL);
    assert(message->rpc_magic == RPC_HEADER_MAGIC);

    int result;
    struct rpc_header networkmsg;
    networkmsg.rpc_type = htonl(message->rpc_type);
    networkmsg.rpc_magic = htonl(message->rpc_magic);
    networkmsg.rpc_len = hton64(message->rpc_len);
    TRY(result, io_write(fd, (void *) &networkmsg, sizeof(struct rpc_header)), done);
    result = 0;
  done:
    return result;
}

int
rpc_write_terminate(int fd)
{
    struct rpc_header msg;
    msg.rpc_type = RPC_TERMINATE;
    msg.rpc_len = 0;
    msg.rpc_magic = RPC_HEADER_MAGIC;
    return rpc_write_header(fd, &msg);
}

int
rpc_write_ok(int fd)
{
    struct rpc_header msg;
    msg.rpc_type = RPC_OK;
    msg.rpc_len = 0;
    msg.rpc_magic = RPC_HEADER_MAGIC;
    return rpc_write_header(fd, &msg);
}

int
rpc_read_header(int fd, struct rpc_header *message)
{
    assert(message != NULL);

    int result;
    struct rpc_header networkmsg;
    TRY(result, io_read(fd, (void *) &networkmsg, sizeof(struct rpc_header)), done);
    message->rpc_type = ntohl(networkmsg.rpc_type);
    message->rpc_magic = ntohl(networkmsg.rpc_magic);
    message->rpc_len = ntoh64(networkmsg.rpc_len);
    assert(message->rpc_magic == RPC_HEADER_MAGIC);
    result = 0;
  done:
    return result;
}

int
rpc_write_query(int fd, struct op *op)
{
    assert(op != NULL);
    int result;
    char *query = op_string(op);
    struct rpc_header msg;
    msg.rpc_type = RPC_QUERY;
    msg.rpc_magic = RPC_HEADER_MAGIC;
    msg.rpc_len = strlen(query) + 1; // +1 for the null byte
    TRY(result, rpc_write_header(fd, &msg), cleanup_query);
    TRY(result, io_write(fd, query, msg.rpc_len), cleanup_query);
    result = 0;
    goto cleanup_query;

  cleanup_query:
    free(query);
    return result;
}

int
rpc_read_query(int fd, struct rpc_header *msg, struct op **retop)
{
    assert(retop != NULL);
    assert(msg != NULL);
    assert(retop != NULL);
    assert(msg->rpc_type == RPC_QUERY);

    int result;
    char *payload;
    struct op *op;

    // includes null byte
    TRYNULL(result, DBENOMEM, payload, malloc(sizeof(char) * msg->rpc_len), done);
    TRY(result, io_read(fd, payload, msg->rpc_len), cleanup_payload);
    TRYNULL(result, DBENOMEM, op, parse_line(payload), cleanup_payload);

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
rpc_read_file(int fd, struct rpc_header *msg, char *filename, int *retfd)
{
    assert(retfd != NULL);

    int result;
    int copyfd = open(filename, O_CREAT | O_RDWR | O_TRUNC, S_IRWXU);
    if (copyfd == -1) {
        result = DBEIONOFILE;
        goto done;
    }
    TRY(result, io_copy(fd, copyfd, msg->rpc_len), cleanup_copyfd);
    // seek back to beginning
    result = lseek(copyfd, 0, SEEK_SET);
    if (result) {
        result = DBELSEEK;
        goto cleanup_copyfd;
    }

    // success
    printf("got file: %s\n", filename);
    *retfd = copyfd;
    result = 0;
    goto done;
  cleanup_copyfd:
    assert(close(copyfd) == 0);
  done:
    return result;
}

int
rpc_write_file(int fd, struct op *op)
{
    assert(op != NULL);
    assert(op->op_type = OP_LOAD);

    int result;
    struct rpc_header msg;
    char *fname = op->op_load.op_load_file;
    int loadfd = open(fname, O_RDONLY);
    if (loadfd == -1) {
        result = DBEIONOFILE;
        goto done;
    }
    msg.rpc_type = RPC_FILE;
    msg.rpc_magic = RPC_HEADER_MAGIC;
    msg.rpc_len = io_size(loadfd);
    TRY(result, rpc_write_header(fd, &msg), cleanup_loadfd);
    TRY(result, io_copy(loadfd, fd, msg.rpc_len), cleanup_loadfd);
    result = 0;
    goto cleanup_loadfd;

  cleanup_loadfd:
    assert(close(loadfd) == 0);
  done:
    return result;
}

int
rpc_write_tuple_result(int fd, struct column_vals **tuples, unsigned len)
{
    assert(tuples != NULL);
    assert(len != 0);
    uint64_t ntuples = tuples[0]->cval_len;
    for (uint64_t i = 1; i < len; i++) {
        assert(ntuples == tuples[i]->cval_len);
    }
    int result;
    struct rpc_header msg;
    msg.rpc_type = RPC_TUPLE_RESULT;
    msg.rpc_magic = RPC_HEADER_MAGIC;
    msg.rpc_len = len * sizeof(int);

    // Write a tuple at a time
    for (uint64_t tuple = 0; tuple < ntuples; tuple++) {
        TRY(result, rpc_write_header(fd, &msg), done);
        for (unsigned i = 0; i < len; i++) {
            uint32_t networkint = htonl(tuples[i]->cval_vals[tuple]);
            TRY(result, io_write(fd, &networkint, sizeof(uint32_t)), done);
        }
    }
    result = 0;
    goto done;
  done:
    return result;
}

int
rpc_read_tuple_result(int fd, struct rpc_header *msg,
                      int **rettuple, unsigned *retlen)
{
    assert(msg != NULL);
    assert(rettuple != NULL);
    assert(msg->rpc_type == RPC_TUPLE_RESULT);

    int result;
    uint64_t bytes = msg->rpc_len;
    unsigned nints = bytes / sizeof(int);
    int *tuple;
    TRYNULL(result, DBENOMEM, tuple, malloc(bytes), done);
    for (unsigned i = 0; i < nints; i++) {
        int networkint;
        TRY(result, io_read(fd, &networkint, sizeof(int)), cleanup_malloc);
        tuple[i] = ntohl(networkint);
    }
    result = 0;
    *rettuple = tuple;
    *retlen = nints;
    goto done;
  cleanup_malloc:
    free(tuple);
  done:
    return result;
}

int
rpc_write_fetch_result(int fd, struct column_vals *vals)
{
    assert(vals != NULL);
    int result;
    struct rpc_header msg;
    msg.rpc_type = RPC_FETCH_RESULT;
    msg.rpc_magic = RPC_HEADER_MAGIC;
    msg.rpc_len = vals->cval_len * sizeof(int);
    TRY(result, rpc_write_header(fd, &msg), done);
    for (unsigned i = 0; i < vals->cval_len; i++) {
        uint32_t networkint = htonl(vals->cval_vals[i]);
        TRY(result, io_write(fd, &networkint, sizeof(uint32_t)), done);
    }
    result = 0;
    goto done;
  done:
    return result;
}

// the retvals must be freed
int
rpc_read_fetch_result(int fd, struct rpc_header *msg, int **retvals, unsigned *retn)
{
    assert(retvals != NULL);
    assert(retn != NULL);
    assert(msg != NULL);
    assert(msg->rpc_type == RPC_FETCH_RESULT);
    int result;

    uint32_t bytes = msg->rpc_len;
    assert(bytes % sizeof(int) == 0);
    int *vals;
    TRYNULL(result, DBENOMEM, vals, malloc(bytes), done);
    unsigned nvals = bytes / (sizeof(int));
    for (unsigned i = 0; i < nvals; i++) {
        int networkint;
        TRY(result, io_read(fd, &networkint, sizeof(int)), done);
        vals[i] = ntohl(networkint);
    }

    // success
    result = 0;
    *retvals = vals;
    *retn = nvals;
    goto done;
  done:
    return result;
}

int
rpc_write_select_result(int fd, struct column_ids *cids)
{
    assert(cids != NULL);

    // materialize the ids
    int result;
    unsigned len = 0;
    switch (cids->cid_type) {
    case CID_BITMAP:
        for (unsigned i = 0; i < bitmap_nbits(cids->cid_bitmap); i++) {
            if (bitmap_isset(cids->cid_bitmap, i)) {
                len++;
            }
        }
        break;
    case CID_ARRAY:
        len = idarray_num(cids->cid_array);
        break;
    }

    struct cid_iterator iter;
    cid_iter_init(&iter, cids);

    // Prepare the header and serialize the results
    struct rpc_header msg;
    msg.rpc_type = RPC_SELECT_RESULT;
    msg.rpc_magic = RPC_HEADER_MAGIC;
    msg.rpc_len = len * sizeof(unsigned);
    TRY(result, rpc_write_header(fd, &msg), done);
    while (cid_iter_has_next(&iter)) {
        uint32_t i = (uint32_t) cid_iter_get(&iter);
        uint32_t networkint = htonl((uint32_t) i);
        TRY(result, io_write(fd, &networkint, sizeof(uint32_t)), done);
    }

    // success
    result = 0;
    goto done;
  done:
    cid_iter_cleanup(&iter);
    return result;
}

// the retvals must be freed
int
rpc_read_select_result(int fd, struct rpc_header *msg,
                       unsigned **retids, unsigned *retn)
{
    assert(retids != NULL);
    assert(retn != NULL);
    assert(msg != NULL);
    assert(msg->rpc_type == RPC_SELECT_RESULT);
    int result;

    uint32_t bytes = msg->rpc_len;
    assert(bytes % sizeof(unsigned) == 0);
    unsigned *vals;
    TRYNULL(result, DBENOMEM, vals, malloc(bytes), done);
    unsigned nvals = bytes / (sizeof(unsigned));
    for (unsigned i = 0; i < nvals; i++) {
        int networkint;
        TRY(result, io_read(fd, &networkint, sizeof(unsigned)), done);
        vals[i] = ntohl(networkint);
    }

    // success
    result = 0;
    *retids = vals;
    *retn = nvals;
    goto done;
  done:
    return result;
}

int
rpc_write_error(int fd, char *error)
{
    assert(error != NULL);
    uint32_t len = strlen(error) + 1; // +1 for '\0'
    struct rpc_header msg;
    msg.rpc_type = RPC_ERROR;
    msg.rpc_magic = RPC_HEADER_MAGIC;
    msg.rpc_len = len;
    int result;
    TRY(result, rpc_write_header(fd, &msg), done);
    TRY(result, io_write(fd, error, len), done);
    result = 0;
    goto done;
  done:
    return result;
}

// retmsg must be freed
int
rpc_read_error(int fd, struct rpc_header *msg, char **retmsg)
{
    assert(msg != NULL);
    assert(retmsg != NULL);
    assert(msg->rpc_type == RPC_ERROR);

    uint32_t len = msg->rpc_len;
    int result;
    char *error;
    TRYNULL(result, DBENOMEM, error, malloc(len), done);
    TRY(result, io_read(fd, error, len), cleanup_malloc);
    // success
    result = 0;
    *retmsg = error;
    goto done;
  cleanup_malloc:
    free(error);
  done:
    return result;
}
