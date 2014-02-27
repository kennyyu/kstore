#ifndef _IO_H_
#define _IO_H_

#include <stdint.h>

enum io_result {
    IO_SUCCESS = 0,
    IO_EARLY_EOF = -1,
    IO_CHECK_ERRNO = -2,
};

enum io_result io_read(int fd, void *buf, int nbytes);
enum io_result io_write(int fd, void *buf, int nbytes);
enum io_result io_copy(int readfd, int writefd, uint64_t expected_bytes);
uint64_t io_size(int fd);

#endif
