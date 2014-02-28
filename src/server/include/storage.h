#ifndef _STORAGE_H_
#define _STORAGE_H_

#include "../../common/include/operators.h"
#include "../../common/include/bitmap.h"
#include "../../common/include/array.h"
#include "file.h"

// NOTE: we do not support deletions yet
// on disk representation of a file
// these will be stored in the metadata file pointed to by storage
// this MUST divide PAGESIZE
struct column_on_disk {
    char cd_col_name[128]; // name of this column
    uint64_t cd_ntuples; // number of tuples in this column
    uint32_t cd_stype; // enum storage_type
    uint32_t cd_magic; // magic value for debugging
    char cd_file_name[112]; // name of file where data is stored
};

// in memory representation
struct column {
    struct storage *col_storage;
    volatile struct column_on_disk *col_disk;
    struct file *col_file;
    struct lock *col_lock;
    unsigned col_id; // index in the storage file
    volatile unsigned col_opencount; // ref count on number of times open
};

DECLARRAY(column);

struct storage {
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

// create the directory if it doesn't exist, and init metadata file
struct storage *storage_init(char *dbdir);
void storage_close(struct storage *storage);

// read through all entries on disk, make sure no column with same name
// if there is, return error? return existing column or create it
// add it to list of open cols
int storage_add_column(struct storage *storage, char *colname,
                       enum storage_type stype, struct column **retcol);

// if not in array, add it and inc ref count
struct column *column_open(struct storage *storage, char *colname);

// dec ref count, close it if 0
void column_close(struct column *col);

// used for load/insert to init the column with values
int column_insert(struct column, int val);

// need reader/writer locks for select,fetch (read) and insert(write)
struct column_ids *column_select(struct column, struct op *op);
struct column_vals *column_fetch(struct column, struct column_ids *ids);
void column_ids_destroy(struct column_ids *cids);
void column_vals_destroy(struct column_vals *vals);

#endif
