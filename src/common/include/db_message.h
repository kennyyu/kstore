#ifndef _DB_MESSAGE_H_
#define _DB_MESSAGE_H_

#include <stdint.h>
#include "operators.h"

#define DB_MESSAGE_MAGIC 0xDEADBEEF

enum db_message_type {
    DB_MESSAGE_QUERY,
    DB_MESSAGE_FILE,
    DB_MESSAGE_FETCH_RESULT,
    DB_MESSAGE_ERROR,
};

struct db_message {
    uint32_t dbm_type;
    uint32_t dbm_magic;
    uint64_t dbm_len;
};

int dbm_write(int fd, struct db_message *message);
int dbm_read(int fd, struct db_message *message);
int dbm_write_query(int fd, struct op *op);
int dbm_write_file(int fd, struct op *op);

#endif
