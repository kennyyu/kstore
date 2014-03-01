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
#include "include/file.h"
#include "include/storage.h"
#include "../common/include/array.h"
#include "../common/include/synch.h"

#define METADATA_FILENAME "metadata"

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
    assert(columnarray_num(storage->st_open_cols) == 0);
    lock_destroy(storage->st_lock);
    columnarray_destroy(storage->st_open_cols);
    file_close(storage->st_file);
    free(storage);
}
