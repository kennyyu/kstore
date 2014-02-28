#ifndef _FILE_H
#define _FILE_H

#include <stdint.h>

#define PAGESIZE 4096

typedef uint64_t page_t;

// these will alloc/free the in memory data structures
struct file *file_open(char *name);
void file_close(struct file *f);

// these will create/delete the on disk data structures
struct file *file_init(char *name);
void file_delete(struct file *f);

int file_alloc_page(struct file *f, page_t *retpage);
int file_read(struct file *f, page_t page, void *buf);
int file_write(struct file *f, page_t page, void *buf);

#endif
