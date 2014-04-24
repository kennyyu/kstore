#ifndef _FILE_H
#define _FILE_H

#include <stdint.h>
#include <stdbool.h>

#define PAGESIZE 4096
#define FILE_BITMAP_PAGES 2
#define FILE_FIRST_PAGE FILE_BITMAP_PAGES

typedef uint64_t page_t;

struct file;

// these will alloc/free the in memory data structures
struct file *file_open(char *name);
void file_close(struct file *f);

// these will create/delete the on disk data structures
int file_alloc_page(struct file *f, page_t *retpage);
void file_free_page(struct file *f, page_t page);
bool file_page_isalloc(struct file *f, page_t page);

// returns the total number of pages in the file, alloc'ed or freed
page_t file_num_pages(struct file *f);
int file_read(struct file *f, page_t page, void *buf);
int file_write(struct file *f, page_t page, void *buf);

#endif
