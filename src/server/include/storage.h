#ifndef _STORAGE_H_
#define _STORAGE_H_

#include <stdint.h>
#include <stdbool.h>
#include "../../common/include/operators.h"
#include "../../common/include/bitmap.h"
#include "../../common/include/array.h"
#include "file.h"

#define COLUMN_TAKEN 0xCAFEBABE
#define COLUMN_FREE 0x0

// NOTE: we do not support deletions yet
// on disk representation of a file
// these will be stored in the metadata file pointed to by storage
// this MUST divide PAGESIZE
struct column_on_disk {
    char cd_col_name[128]; // name of this column
    uint64_t cd_ntuples; // number of tuples in this column
    uint32_t cd_stype; // enum storage_type
    uint32_t cd_magic; // magic value for debugging
    char cd_base_file[56]; // file where data is stored, does not include dbdir
    char cd_index_file[56]; // file where index is stored, does not include dbdir
};

#define COLUMNS_PER_PAGE (PAGESIZE / sizeof(struct column_on_disk))

// in memory representation
struct column {
    struct storage *col_storage;
    struct column_on_disk col_disk;
    struct file *col_base_file;
    struct rwlock *col_rwlock;
    page_t col_page; // page in the storage file
    unsigned col_index; // index in the page in the storage file
    volatile unsigned col_opencount; // ref count on number of times open
    volatile bool col_dirty; // tells us whether we need to synch the buffer
};

DECLARRAY(column);

struct storage {
    char st_dbdir[128]; // name of the db directory
    struct file *st_file; // pointer to metadata file
    struct lock *st_lock; // protect addition of columns
    struct columnarray *st_open_cols; // array of open columns
};

struct column_ids {
    struct bitmap *cid_bitmap;
};

struct column_vals {
    int *cval_vals;
    unsigned cval_len;
};

struct column_entry_unsorted {
    int ce_val;
};

#define COLENTRY_UNSORTED_PER_PAGE (PAGESIZE / sizeof(struct column_entry_unsorted))

struct column_entry_sorted {
    int ce_val;
    uint32_t ce_index;
};

#define COLENTRY_SORTED_PER_PAGE (PAGESIZE / sizeof(struct column_entry_sorted))

// create the directory if it doesn't exist, and init metadata file
struct storage *storage_init(char *dbdir);
void storage_close(struct storage *storage);

// read through all entries on disk, make sure no column with same name
// if there is, return error? return existing column or create it
// add it to list of open cols
int storage_add_column(struct storage *storage, char *colname,
                       enum storage_type stype);

// if not in array, add it and inc ref count
struct column *column_open(struct storage *storage, char *colname);

// dec ref count, close it if 0
void column_close(struct column *col);

int column_insert(struct column *col, int val);
int column_load(struct column *col, int *vals, uint64_t num);

// need reader/writer locks for select,fetch (read) and insert(write)
struct column_ids *column_select(struct column *col, struct op *op);
struct column_vals *column_fetch(struct column *col, struct column_ids *ids);
void column_ids_destroy(struct column_ids *cids);
void column_vals_destroy(struct column_vals *vals);

#endif
