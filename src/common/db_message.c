#include <arpa/inet.h>
#include <assert.h>
#include <stdint.h>
#include <stddef.h>
#include <unistd.h>
#include <errno.h>
#include "include/db_message.h"
#include "include/io.h"

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
