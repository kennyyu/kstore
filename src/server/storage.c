#include <assert.h>
#include <stddef.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdbool.h>
#include "include/file.h"
#include "include/storage.h"
#include "../common/include/operators.h"
#include "../common/include/array.h"
#include "../common/include/synch.h"

#define METADATA_FILENAME "metadata"

#define MIN(a,b) ((a) < (b)) ? (a) : (b)

DEFARRAY(column, /* no inline */);

// create the directory if it doesn't exist, and init metadata file
struct storage *
storage_init(char *dbdir)
{
    int result;
    struct storage *storage = malloc(sizeof(struct storage));
    if (storage == NULL) {
        result = -1;
        goto done;
    }
    strcpy(storage->st_dbdir, dbdir);
    storage->st_lock = lock_create();
    if (storage->st_lock == NULL) {
        goto cleanup_malloc;
    }
    storage->st_open_cols = columnarray_create();
    if (storage->st_open_cols == NULL) {
        goto cleanup_lock;
    }
    // create the directory if it doesn't already exist
    // it will return ENOENT if it already exists
    result = mkdir(dbdir, S_IRWXU);
    if (result == -1 && errno != EEXIST) {
        goto cleanup_colarray;
    }
    char buf[128];
    sprintf(buf, "%s/%s", dbdir, METADATA_FILENAME);
    storage->st_file = file_open(buf);
    if (storage->st_file == NULL) {
        goto cleanup_mkdir;
    }
    goto done;

  cleanup_mkdir:
    assert(rmdir(dbdir) == 0);
  cleanup_colarray:
    columnarray_destroy(storage->st_open_cols);
  cleanup_lock:
    lock_destroy(storage->st_lock);
  cleanup_malloc:
    free(storage);
    storage = NULL;
  done:
    return storage;
}

void
storage_close(struct storage *storage)
{
    assert(storage != NULL);
    // at this point, we should be the only thread running, so we
    // do not need to acquire the lock
    // since there are no other threads running, then the open columns
    // should have been closed when the threads exited
    lock_destroy(storage->st_lock);
    assert(columnarray_num(storage->st_open_cols) == 0);
    columnarray_destroy(storage->st_open_cols);
    file_close(storage->st_file);
    free(storage);
}

// PRECONDITION: MUST BE HOLDING STORAGE LOCK!!!
// if this returns true, the page and index of the column are returned
// if this returns false and retfoundfree is true, then a free page and index
// are returned
static
bool
storage_find_column(struct storage *storage, char *colname,
                    bool *retfoundfree, page_t *retpage, unsigned *retindex)
{
    assert(storage != NULL);
    assert(colname != NULL);
    assert(retfoundfree != NULL);
    assert(retpage != NULL);
    assert(retindex != NULL);
    //assert(have lock)

    int result;
    bool found = false;
    bool free_found = false;
    page_t free_page = 0;
    unsigned free_colindex = 0;
    struct column_on_disk colbuf[COLUMNS_PER_PAGE];
    page_t last_page = file_num_pages(storage->st_file) - 1;
    for (page_t page = FILE_FIRST_PAGE; page <= last_page; page++) {
        result = file_read(storage->st_file, page, (void *) colbuf);
        if (result) {
            found = false;
            goto done;
        }
        // Check if any of the columns match our column name
        for (unsigned i = 0; i < COLUMNS_PER_PAGE; i++) {
            if (colbuf[i].cd_magic == COLUMN_FREE) {
                free_found = true;
                free_page = page;
                free_colindex = i;
                continue;
            }
            assert(colbuf[i].cd_magic == COLUMN_TAKEN);
            if (strcmp(colbuf[i].cd_col_name, colname) == 0) {
                found = true;
                free_page = page;
                free_colindex = i;
                goto done;
            }
        }
    }
  done:
    *retfoundfree = free_found;
    *retpage = free_page;
    *retindex = free_colindex;
    return found;
}

// PRECONDITION: MUST BE HOLDING LOCK
static
bool
storage_find_column_open(struct storage *storage, char *colname,
                         struct column **retcol)
{
    assert(storage != NULL);
    assert(colname != NULL);
    assert(retcol != NULL);

    for (unsigned i = 0; i < columnarray_num(storage->st_open_cols); i++) {
        struct column *col = columnarray_get(storage->st_open_cols, i);
        lock_acquire(col->col_lock);
        if (strcmp(col->col_disk.cd_col_name, colname) == 0) {
            *retcol = col;
            lock_release(col->col_lock);
            return true;
        }
        lock_release(col->col_lock);
    }
    return false;
}

// PRECONDITION: must hold lock on storage
static
int
storage_synch_column(struct storage *storage,
                     struct column_on_disk *coldisk,
                     page_t page,
                     unsigned index)
{
    assert(storage != NULL);
    assert(coldisk != NULL);

    int result;
    struct column_on_disk colbuf[COLUMNS_PER_PAGE];
    result = file_read(storage->st_file, page, colbuf);
    if (result) {
        goto done;
    }
    colbuf[index] = *coldisk;
    result = file_write(storage->st_file, page, colbuf);
    if (result) {
        goto done;
    }
  done:
    return result;
}

int
storage_add_column(struct storage *storage, char *colname,
                   enum storage_type stype)
{
    assert(storage != NULL);
    assert(colname != NULL);
    assert(PAGESIZE % sizeof(struct column_on_disk) == 0);

    // only one column may be added at a time
    lock_acquire(storage->st_lock);

    // scan through all the existing columns to see if any
    // of them have the same name. if there is, return success
    // otherwise, add it to the file
    int result;
    bool freefound;
    page_t colpage;
    unsigned colindex;
    bool found = storage_find_column(storage, colname, &freefound,
                                     &colpage, &colindex);
    if (found) {
        result = 0;
        goto done;
    }

    // if we reach here, then we could not find our column in the file
    // try to insert our column into a free page, otherwise we'll need to
    // extend the file with a new page

    // create a file to store the column data
    char filenamebuf[128];
    sprintf(filenamebuf, "%s/%s.column", storage->st_dbdir, colname);
    struct file *colfile = file_open(filenamebuf);
    if (colfile == NULL) {
        result = -1;
        goto done;
    }

    struct column_on_disk newcol;
    strcpy(newcol.cd_col_name, colname);
    newcol.cd_ntuples = 0;
    newcol.cd_magic = COLUMN_TAKEN;
    newcol.cd_stype = stype;
    sprintf(newcol.cd_file_name, "%s.column", colname);

    // if we couldn't find a free slot earlier, we need to extend
    // the storage metadata file
    if (!freefound) {
        result = file_alloc_page(storage->st_file, &colpage);
        if (result) {
            goto cleanup_file;
        }
        colindex = 0;
    }
    // finally write the new column to disk
    result = storage_synch_column(storage, &newcol, colpage, colindex);
    if (result) {
        goto cleanup_page;
    }

    // success
    result = 0;
    goto done;
  cleanup_page:
    if (!freefound) {
        file_free_page(storage->st_file, colpage);
    }
  cleanup_file:
    assert(remove(filenamebuf) == 0);
  done:
    lock_release(storage->st_lock);
    return result;
}

// if not in array, add it and inc ref count
struct column *
column_open(struct storage *storage, char *colname)
{
    assert(storage != NULL);
    assert(colname != NULL);

    lock_acquire(storage->st_lock);
    int result;
    struct column *col = NULL;

    // first check if the column is already open.
    // if it is, increment the ref count and return it
    bool openfound = storage_find_column_open(storage, colname, &col);
    if (openfound) {
        lock_acquire(col->col_lock);
        col->col_opencount++;
        lock_release(col->col_lock);
        goto done;
    }

    // find the on-disk representation of the column and bring it into memory
    bool freefound;
    page_t colpage;
    unsigned colindex;
    bool found = storage_find_column(storage, colname, &freefound,
                                     &colpage, &colindex);
    if (!found) {
        goto done;
    }
    struct column_on_disk colbuf[COLUMNS_PER_PAGE];
    result = file_read(storage->st_file, colpage, colbuf);
    assert(colbuf[colindex].cd_magic == COLUMN_TAKEN);

    // allocate space for the in-memory representation of the column
    col = malloc(sizeof(struct column));
    if (col == NULL) {
        goto done;
    }
    memcpy(&col->col_disk, &colbuf[colindex], sizeof(struct column_on_disk));

    // open the file for the column
    char filenamebuf[128];
    sprintf(filenamebuf, "%s/%s", storage->st_dbdir,
            col->col_disk.cd_file_name);
    col->col_file = file_open(filenamebuf);
    if (col->col_file == NULL) {
        goto cleanup_malloc;
    }

    // allocate the lock to protect the column
    col->col_lock = lock_create();
    if (col->col_lock == NULL) {
        goto cleanup_file;
    }
    col->col_page = colpage;
    col->col_index = colindex;
    col->col_opencount = 1;
    col->col_storage = storage;

    // finally, add this column to the array of open columns
    result = columnarray_add(storage->st_open_cols, col, NULL);
    if (result) {
        goto cleanup_lock;
    }
    goto done;

  cleanup_lock:
    lock_destroy(col->col_lock);
  cleanup_file:
    file_close(col->col_file);
  cleanup_malloc:
    free(col);
    col = NULL;
  done:
    lock_release(storage->st_lock);
    return col;
}

// dec ref count, close it if 0
void
column_close(struct column *col)
{
    assert(col != NULL);
    // to prevent an open and close happening at the same time for this
    // column, we grab the lock on storage first
    // to avoid deadlock, we grab the storage lock first
    lock_acquire(col->col_lock);
    struct storage *storage = col->col_storage;
    lock_release(col->col_lock);
    assert(storage != NULL);
    lock_acquire(storage->st_lock);

    // decrement the refcnt. if it reaches 0, destroy the column
    bool should_destroy = false;
    lock_acquire(col->col_lock);
    col->col_opencount--;
    if (col->col_opencount == 0) {
        should_destroy = true;
    }
    lock_release(col->col_lock);
    if (!should_destroy) {
        goto done;
    }

    // remove the column from the list of open columns
    unsigned listlen = columnarray_num(storage->st_open_cols);
    for (unsigned i = 0; i < listlen; i++) {
        struct column *opencol = columnarray_get(storage->st_open_cols, i);
        if (opencol == col) {
            columnarray_remove(storage->st_open_cols, i);
            break;
        }
    }
    assert(columnarray_num(storage->st_open_cols) == listlen - 1);
    lock_destroy(col->col_lock);
    file_close(col->col_file);
    free(col);

  done:
    lock_release(storage->st_lock);
}

int
column_load(struct column *col, int *vals, uint64_t num)
{
    assert(col != NULL);
    assert(vals != NULL);
    int result;
    lock_acquire(col->col_lock);

    int intbuf[PAGESIZE / sizeof(int)];
    uint64_t curtuple = 0;
    while (curtuple < num) {
        uint64_t tuples_tocopy = num - curtuple;
        // avoid overflow
        size_t bytes_tocopy =
                sizeof(int) * (MIN(PAGESIZE / sizeof(int), tuples_tocopy));
        bzero(intbuf, PAGESIZE);
        memcpy(intbuf, vals + curtuple, bytes_tocopy);
        page_t page;
        result = file_alloc_page(col->col_file, &page);
        if (result) {
            goto cleanup_col_lock;
        }
        result = file_write(col->col_file, page, intbuf);
        if (result) {
            file_free_page(col->col_file, page);
            goto cleanup_col_lock;
        }
        curtuple += tuples_tocopy;
    }

    // to avoid deadlock, we must drop the column lock and
    // grab the lock on storage first
    // synch updated column data to disk
    struct storage *storage = col->col_storage;
    lock_release(col->col_lock);

    lock_acquire(storage->st_lock);
    lock_acquire(col->col_lock);
    result = storage_synch_column(storage, &col->col_disk,
                                  col->col_page, col->col_index);
    // return result
    lock_release(col->col_lock);
    lock_release(storage->st_lock);
    goto done;

  cleanup_col_lock:
    lock_release(col->col_lock);
  done:
    return result;
}
