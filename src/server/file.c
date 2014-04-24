#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include "include/file.h"
#include "../../common/include/dberror.h"
#include "../../common/include/io.h"
#include "../../common/include/bitmap.h"

struct file {
    int f_fd;
    uint64_t f_size;
    struct bitmap *f_page_bitmap;
};

struct file *
file_open(char *name)
{
    int result;
    struct file *f = malloc(sizeof(struct file));
    if (f == NULL) {
        goto done;
    }
    f->f_fd = open(name, O_CREAT | O_RDWR, S_IRWXU);
    if (f->f_fd == -1) {
        goto cleanup_malloc;
    }
    f->f_size = io_size(f->f_fd);
    if (f->f_size == 0) {
        // if we are creating the file for the first time, allocate the
        // first page for the bitmap, and mark the page as taken
        f->f_page_bitmap = bitmap_create(PAGESIZE * 8);
        if (f->f_page_bitmap == NULL) {
            goto cleanup_fd;
        }
        bitmap_mark(f->f_page_bitmap, 0);
        result = io_write(f->f_fd, bitmap_getdata(f->f_page_bitmap), PAGESIZE);
        if (result) {
            bitmap_destroy(f->f_page_bitmap);
            goto cleanup_fd;
        }
        f->f_size += PAGESIZE;
    } else {
        // if we are initing from an already existing file, read the first
        // page and init the bitmap using that page
        unsigned char buf[PAGESIZE];
        bzero(buf, PAGESIZE);
        result = io_read(f->f_fd, buf, PAGESIZE);
        if (result) {
            goto cleanup_fd;
        }
        f->f_page_bitmap = bitmap_init(PAGESIZE * 8, buf);
        if (f->f_page_bitmap == NULL) {
            goto cleanup_fd;
        }
    }
    // seek back to beginning on opening
    result = lseek(f->f_fd, 0, SEEK_SET);
    if (result) {
        goto cleanup_fd;
    }
    goto done;

  cleanup_fd:
    assert(close(f->f_fd) == 0);
  cleanup_malloc:
    free(f);
    f = NULL;
  done:
    return f;
}

void
file_close(struct file *f)
{
    assert(f != NULL);
    assert(close(f->f_fd) == 0);
    bitmap_destroy(f->f_page_bitmap);
    free(f);
}

int
file_alloc_page(struct file *f, page_t *retpage)
{
    page_t page;
    unsigned nbits = PAGESIZE * 8;
    for (page = 0; page < nbits; page++) {
        if (!bitmap_isset(f->f_page_bitmap, page)) {
            bitmap_mark(f->f_page_bitmap, page);
            assert(file_write(f, 0, bitmap_getdata(f->f_page_bitmap)) == 0);
            break;
        }
    }
    if (page == nbits) {
        // no more space in bitmap
        fprintf(stderr, "no more space in bitmap\n");
        return DBENOMEM;
    }
    // if we do get a valid page number, extend the file if necessary
    if (page * PAGESIZE >= f->f_size) {
        int result = ftruncate(f->f_fd, f->f_size + PAGESIZE);
        if (result == -1) {
            perror("ftruncate");
            return result;
        }
        f->f_size += PAGESIZE;
    }
    *retpage = page;
    return 0;
}

void
file_free_page(struct file *f, page_t page)
{
    assert(f != NULL);
    assert(f->f_page_bitmap != NULL);
    assert(bitmap_isset(f->f_page_bitmap, page));
    bitmap_unmark(f->f_page_bitmap, page);
}

bool
file_page_isalloc(struct file *f, page_t page)
{
    assert(f != NULL);
    assert(f->f_page_bitmap != NULL);
    return bitmap_isset(f->f_page_bitmap, page);
}

// returns the total number of pages in the file, alloc'ed or freed
page_t
file_num_pages(struct file *f)
{
    assert(f != NULL);
    return (page_t) (f->f_size / PAGESIZE);
}

// we use pread/pwrite here to avoid modifying the file descriptor offset
int
file_read(struct file *f, page_t page, void *buf)
{
    assert(f != NULL);
    assert(buf != NULL);
    assert(f->f_page_bitmap != NULL);
    assert(bitmap_isset(f->f_page_bitmap, page));

    bzero(buf, PAGESIZE);
    int result = pread(f->f_fd, buf, PAGESIZE, page * PAGESIZE);
    if (result == -1 || result == 0) {
        return DBEIOCHECKERRNO;
    }
    assert(result == PAGESIZE);
    return 0;
}

int
file_write(struct file *f, page_t page, void *buf) {
    assert(f != NULL);
    assert(buf != NULL);
    assert(f->f_page_bitmap != NULL);
    assert(bitmap_isset(f->f_page_bitmap, page));

    int result = pwrite(f->f_fd, buf, PAGESIZE, page * PAGESIZE);
    if (result == -1 || result == 0) {
        return DBEIOCHECKERRNO;
    }
    assert(result == PAGESIZE);
    return 0;
}
