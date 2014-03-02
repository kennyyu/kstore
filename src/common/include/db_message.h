#ifndef _DB_MESSAGE_H_
#define _DB_MESSAGE_H_

#include <stdint.h>
#include "operators.h"
#include "../../server/include/storage.h"

#define DB_MESSAGE_MAGIC 0xDEADBEEF

enum db_message_type {
    DB_MESSAGE_QUERY,
    DB_MESSAGE_FILE,
    DB_MESSAGE_FETCH_RESULT,
    DB_MESSAGE_ERROR,
    DB_MESSAGE_TERMINATE,
};

struct db_message {
    uint32_t dbm_type;
    uint32_t dbm_magic;
    uint64_t dbm_len;
};

int dbm_write(int fd, struct db_message *message);
int dbm_read(int fd, struct db_message *message);

int dbm_write_query(int fd, struct op *op);
// the retop must be freed
int dbm_read_query(int fd, struct db_message *msg, struct op **retop);

int dbm_write_file(int fd, struct op *op);
// the retfd must be closed
int dbm_read_file(int fd, char *filename, int *retfd);

int dbm_write_result(int fd, struct column_vals *vals);
// the retvals must be freed
int dbm_read_result(int fd, struct db_message *msg, int **retvals, int *retn);

int dbm_write_error(int fd, char *error);
// retmsg must be freed
int dbm_read_error(int fd, struct db_message *msg, char **retmsg);

int dbm_write_terminate(int fd);

#endif
