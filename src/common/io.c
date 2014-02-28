#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include "include/io.h"

#define BUFSIZE 4096
#define MIN(a,b) ((a) < (b)) ? (a) : (b)

enum io_type {
    IO_READ,
    IO_WRITE,
};

static
enum io_result
io_readwrite(int fd, void *buf, int nbytes, enum io_type rw)
{
    int result;
    int total = 0;
    while (total < nbytes) {
        assert(rw == IO_READ || rw == IO_WRITE);
        if (rw == IO_READ) {
            result = read(fd, buf + total, nbytes - total);
        } else if (rw == IO_WRITE) {
            result = write(fd, buf + total, nbytes - total);
        }
        switch (result) {
        case 0:
            return IO_EARLY_EOF;
        case -1:
            return IO_CHECK_ERRNO;
        default:
            total += result;
            break;
        }
    }
    assert(total == nbytes);
    return IO_SUCCESS;
}

enum io_result
io_read(int fd, void *buf, int nbytes)
{
    return io_readwrite(fd, buf, nbytes, IO_READ);
}

enum io_result
io_write(int fd, void *buf, int nbytes)
{
    return io_readwrite(fd, buf, nbytes, IO_WRITE);
}

enum io_result
io_copy(int readfd, int writefd, uint64_t expected_bytes)
{
    uint64_t total = 0;
    int nr, nw;
    char buf[BUFSIZE];
    bzero(buf, BUFSIZE);
    while ((nr = read(readfd, buf, MIN(BUFSIZE, expected_bytes - total))) > 0) {
        nw = write(writefd, buf, nr);
        assert(nw == nr);
        total += nr;
    }
    if (nr == -1) {
        return IO_CHECK_ERRNO;
    }
    if (nr == 0 && (total != expected_bytes)) {
        return IO_EARLY_EOF;
    }
    return IO_SUCCESS;
}

uint64_t
io_size(int fd)
{
    struct stat buf;
    int result = fstat(fd, &buf);
    assert(result == 0);
    return buf.st_size;
}
