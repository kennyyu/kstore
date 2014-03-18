#ifndef _IO_H_
#define _IO_H_

#include <stdint.h>

int io_read(int fd, void *buf, int nbytes);
int io_write(int fd, void *buf, int nbytes);
int io_copy(int readfd, int writefd, uint64_t expected_bytes);
uint64_t io_size(int fd);

#endif
