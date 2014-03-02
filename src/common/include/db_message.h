#ifndef _RPC_H_
#define _RPC_H_

#include <stdint.h>
#include "operators.h"
#include "../../server/include/storage.h"

#define DB_MESSAGE_MAGIC 0xDEADBEEF

// RPC messages will be split into two parts:
// Header
//   specifies type and length of body
// Body
//   actual payload

enum rpc_type {
    RPC_QUERY,
    RPC_FILE,
    RPC_FETCH_RESULT,
    RPC_ERROR,
    RPC_TERMINATE,
};

struct rpc_header {
    uint32_t rpc_type;
    uint32_t rpc_magic;
    uint64_t rpc_len;
};

int rpc_write_header(int fd, struct rpc_header *message);
int rpc_read_header(int fd, struct rpc_header *message);

int rpc_write_query(int fd, struct op *op);
// the retop must be freed
int rpc_read_query(int fd, struct rpc_header *msg, struct op **retop);

int rpc_write_file(int fd, struct op *op);
// the retfd must be closed
int rpc_read_file(int fd, struct rpc_header *msg, char *filename, int *retfd);

int rpc_write_fetch_result(int fd, struct column_vals *vals);
// the retvals must be freed
int rpc_read_fetch_result(int fd, struct rpc_header *msg, int **retvals, int *retn);

int rpc_write_error(int fd, char *error);
// retmsg must be freed
int rpc_read_error(int fd, struct rpc_header *msg, char **retmsg);

int rpc_write_terminate(int fd);

#endif
