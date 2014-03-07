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
#include "../common/include/search.h"

#define METADATA_FILENAME "metadata"

#define MIN(a,b) ((a) < (b)) ? (a) : (b)

DEFARRAY(column, /* no inline */);

DECLARRAY_BYTYPE(valarray, int);
DEFARRAY_BYTYPE(valarray, int, /* no inline */);

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
        rwlock_acquire_read(col->col_rwlock);
        if (strcmp(col->col_disk.cd_col_name, colname) == 0) {
            *retcol = col;
            rwlock_release(col->col_rwlock);
            return true;
        }
        rwlock_release(col->col_rwlock);
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
    struct column_on_disk newcol;
    bzero(&newcol, sizeof(struct column_on_disk));
    strcpy(newcol.cd_col_name, colname);
    newcol.cd_ntuples = 0;
    newcol.cd_magic = COLUMN_TAKEN;
    newcol.cd_stype = stype;
    newcol.cd_btree_root = BTREE_PAGE_NULL;

    // create a file to store the column data
    char filenamebuf[56];
    sprintf(filenamebuf, "%s/%s.column", storage->st_dbdir, colname);
    struct file *colfile = file_open(filenamebuf);
    if (colfile == NULL) {
        result = -1;
        goto done;
    }
    file_close(colfile);
    sprintf(newcol.cd_base_file, "%s.column", colname);

    // create a file to store the index data
    char indexnamebuf[56];
    if (stype == STORAGE_BTREE || stype == STORAGE_SORTED) {
        if (stype == STORAGE_BTREE) {
            sprintf(indexnamebuf, "%s/%s.btree", storage->st_dbdir, colname);
            sprintf(newcol.cd_index_file, "%s.btree", colname);
        } else if (stype == STORAGE_SORTED) {
            sprintf(indexnamebuf, "%s/%s.sorted", storage->st_dbdir, colname);
            sprintf(newcol.cd_index_file, "%s.sorted", colname);
        }
        struct file *colindexfile = file_open(indexnamebuf);
        if (colindexfile == NULL) {
            result = -1;
            goto cleanup_file;
        }
        if (stype == STORAGE_BTREE) {
            // Create a page for the root node.
            // The first time we load, it's going to be a
            // leaf node with 0 entries
            // No need to declare a cleanup for rootpage
            // because the colindexfile will be destroyed
            page_t rootpage;
            result = file_alloc_page(colindexfile, &rootpage);
            if (result) {
                file_close(colindexfile);
                goto cleanup_file;
            }
            assert(rootpage != BTREE_PAGE_NULL);
            newcol.cd_btree_root = rootpage;
        }
        file_close(colindexfile);
    }

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
    if (stype == STORAGE_BTREE || stype == STORAGE_SORTED) {
        assert(remove(indexnamebuf) == 0);
    }
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
        rwlock_acquire_write(col->col_rwlock);
        col->col_opencount++;
        rwlock_release(col->col_rwlock);
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

    // open the base file for the column
    char filenamebuf[56];
    sprintf(filenamebuf, "%s/%s", storage->st_dbdir,
            col->col_disk.cd_base_file);
    col->col_base_file = file_open(filenamebuf);
    if (col->col_base_file == NULL) {
        goto cleanup_malloc;
    }

    // open the index file for the column if it exists
    // only columns that have btree and sorted storage
    col->col_index_file = NULL;
    if ((col->col_disk.cd_stype == STORAGE_BTREE
         || col->col_disk.cd_stype == STORAGE_SORTED)) {
        sprintf(filenamebuf, "%s/%s", storage->st_dbdir,
                col->col_disk.cd_index_file);
        col->col_index_file = file_open(filenamebuf);
        if (col->col_index_file == NULL) {
            goto cleanup_file;
        }
    }

    // allocate the lock to protect the column
    col->col_rwlock = rwlock_create();
    if (col->col_rwlock == NULL) {
        goto cleanup_file;
    }
    col->col_page = colpage;
    col->col_index = colindex;
    col->col_opencount = 1;
    col->col_storage = storage;
    col->col_dirty = false;

    // finally, add this column to the array of open columns
    result = columnarray_add(storage->st_open_cols, col, NULL);
    if (result) {
        goto cleanup_lock;
    }
    goto done;

  cleanup_lock:
    rwlock_destroy(col->col_rwlock);
  cleanup_file:
    if (col->col_index_file != NULL) {
        file_close(col->col_index_file);
    }
    file_close(col->col_base_file);
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
    rwlock_acquire_read(col->col_rwlock);
    struct storage *storage = col->col_storage;
    rwlock_release(col->col_rwlock);
    assert(storage != NULL);
    lock_acquire(storage->st_lock);

    // decrement the refcnt. if it reaches 0, destroy the column
    // since we are the last thread to close it, then it is safe to
    // read the contents of the struct without the lock
    bool should_destroy = false;
    rwlock_acquire_write(col->col_rwlock);
    col->col_opencount--;
    if (col->col_opencount == 0) {
        should_destroy = true;
    }
    rwlock_release(col->col_rwlock);
    if (!should_destroy) {
        goto done;
    }

    // synch any changes in the column if it is dirty
    if (col->col_dirty) {
        int result = storage_synch_column(storage, &col->col_disk,
                                          col->col_page, col->col_index);
        if (result) {
            goto done;
        }
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
    rwlock_destroy(col->col_rwlock);
    file_close(col->col_base_file);
    free(col);

  done:
    lock_release(storage->st_lock);
}

static
int
btree_entry_compare(const void *a, const void *b)
{
    struct btree_entry *aent = (struct btree_entry *) a;
    struct btree_entry *bent = (struct btree_entry *) b;
    return (aent->bte_key - bent->bte_key);
}

// PRECONDITION: must be holding lock on column
// Finds all ids on [left, right)
static
int
btree_select_range(struct column *col,
                   page_t pleft, unsigned ixleft,
                   page_t pright, unsigned ixright,
                   struct column_ids *cids)
{
    assert(col != NULL);
    assert(cids != NULL);
    assert(pleft != BTREE_PAGE_NULL);
    assert(pright != BTREE_PAGE_NULL);
    assert(ixleft <= BTENTRY_PER_PAGE);
    assert(ixright <= BTENTRY_PER_PAGE);

    int result;
    struct btree_node nodebuf;

    page_t curpage = pleft;
    unsigned curix = ixleft;
    while (1) {
        assert(curpage != BTREE_PAGE_NULL);
        result = file_read(col->col_index_file, curpage, &nodebuf);
        if (result) {
            goto done;
        }
        assert(nodebuf.bt_header.bth_type == BTREE_NODE_LEAF);
        assert(nodebuf.bt_header.bth_page == curpage);

        unsigned rightbound = (curpage == pright) ? ixright
                : nodebuf.bt_header.bth_nentries;
        for (/* none */; curix < rightbound; curix++) {
            struct btree_entry *entry = &nodebuf.bt_entries[curix];
            bitmap_mark(cids->cid_bitmap, entry->bte_index);
        }
        if ((curpage == pright) && (curix == ixright)) {
            break;
        }
        curpage = nodebuf.bt_header.bth_next;
        curix = 0;
    }

    // success
    result = 0;
    goto done;
  done:
    return result;
}

// PRECONDITION: must be holding lock on column
static
int
btree_search(struct column *col, int val,
             page_t *retpage, unsigned *retindex)
{
    int result;
    page_t targetpage = BTREE_PAGE_NULL;
    unsigned targetindex = 0;
    struct btree_entry target;
    bzero(&target, sizeof(struct btree_entry));
    target.bte_key = val;

    struct btree_node nodebuf;
    page_t curpage = col->col_disk.cd_btree_root;
    while (targetpage == BTREE_PAGE_NULL) {
        assert(curpage != BTREE_PAGE_NULL);
        result = file_read(col->col_index_file, curpage, &nodebuf);
        if (result) {
            goto done;
        }
        unsigned ix = binary_search(&target, &nodebuf.bt_entries,
                                    nodebuf.bt_header.bth_nentries,
                                    sizeof(struct btree_entry),
                                    btree_entry_compare);
        switch (nodebuf.bt_header.bth_type) {
        case BTREE_NODE_INTERNAL:
            if (ix == 0) { // chase left pointer
                curpage = nodebuf.bt_header.bth_left;
            } else { // chase ix - 1
                curpage = nodebuf.bt_entries[ix - 1].bte_page;
            }
            break;
        case BTREE_NODE_LEAF:
            targetpage = curpage;
            targetindex = ix;
            goto success;
        default:
            assert(0);
            break;
        }
    }

    // success
  success:
    result = 0;
    assert(targetpage != BTREE_PAGE_NULL);
    *retpage = targetpage;
    *retindex = targetindex;
    goto done;
  done:
    return result;
}

static
int
btree_node_synch(struct file *f, struct btree_node *node)
{
    assert(f != NULL);
    assert(node != NULL);
    assert(node->bt_header.bth_page != BTREE_PAGE_NULL);
    return file_write(f, node->bt_header.bth_page, node);
}

static
int
btree_insert_entry(struct file *f,
                   struct btree_node *current,
                   struct btree_entry *entry)
{
    assert(current != NULL);
    unsigned nentries = current->bt_header.bth_nentries;
    assert(nentries < BTENTRY_PER_PAGE);

    unsigned ix = binary_search(entry, current->bt_entries, nentries,
                                sizeof(struct btree_entry),
                                btree_entry_compare);
    if (ix < nentries) {
        // memmove allows overlapping regions
        memmove(&current->bt_entries[ix + 1], &current->bt_entries[ix],
                sizeof(struct btree_entry) * (nentries - ix));
    }
    current->bt_entries[ix] = *entry;
    current->bt_header.bth_nentries++;
    return btree_node_synch(f, current);
}

// Inserts the entry into current
// If a split occurred, returns a new entry and a pointer to the node
// that needs to be fixed
static
int
btree_insert_helper(struct file *f,
                    struct btree_node *current,
                    struct btree_entry *entry,
                    struct btree_entry *retentry)
{
    (void) btree_select_range;
    (void) btree_search;
    assert(f != NULL);
    assert(current != NULL);
    assert(entry != NULL);
    assert(retentry != NULL);

    int result = 0;
    struct btree_entry entrybuf;
    bzero(&entrybuf, sizeof(struct btree_entry));
    struct btree_node nodebuf;
    bzero(&nodebuf, sizeof(struct btree_node));

    // If we have an internal node, recurse down until we can insert entry.
    // Each invocation will possibly propagate up to the caller a new
    // entry to insert into the current node.
    if (current->bt_header.bth_type == BTREE_NODE_INTERNAL) {
        assert(current->bt_header.bth_nentries != 0);
        unsigned ix = binary_search(entry,
                                    current->bt_entries,
                                    current->bt_header.bth_nentries,
                                    sizeof(struct btree_entry),
                                    btree_entry_compare);
        page_t pchild;
        if (ix == 0) { // chase left pointer
            pchild = current->bt_header.bth_left;
        } else { // chase ix - 1 pointer
            pchild = current->bt_entries[ix - 1].bte_page;
        }
        assert(pchild != BTREE_PAGE_NULL);
        result = file_read(f, pchild, &nodebuf);
        assert(result == 0);
        bzero(&entrybuf, sizeof(struct btree_entry));
        result = btree_insert_helper(f, &nodebuf, entry, &entrybuf);
        assert(result == 0);

        // If we get a new entry, it must be an entry pointing to
        // a new page. Therefore, if we get NULL, we're done
        if (entrybuf.bte_page == BTREE_PAGE_NULL) {
            goto success;
        }
        // Otherwise, we need to insert the new entry into the current node
        entry = &entrybuf;
    }

    // If we have space in this node, insert it.
    unsigned nentries = current->bt_header.bth_nentries;
    if (nentries < BTENTRY_PER_PAGE) {
        result = btree_insert_entry(f, current, entry);
        assert(result == 0);
        bzero(&entrybuf, sizeof(struct btree_entry));
        goto success;
    }

    // Otherwise, we need to split and copy half the entries to the new node.
    bzero(&nodebuf, sizeof(struct btree_node));
    page_t newpage;
    result = file_alloc_page(f, &newpage);
    assert(result == 0);
    unsigned halfix = nentries / 2;
    nodebuf.bt_header.bth_type = current->bt_header.bth_type;
    nodebuf.bt_header.bth_page = newpage;
    current->bt_header.bth_nentries = halfix;
    struct btree_entry *base;
    switch (current->bt_header.bth_type) {
    case BTREE_NODE_LEAF:
        nodebuf.bt_header.bth_nentries = nentries - halfix;
        nodebuf.bt_header.bth_next = current->bt_header.bth_next;
        current->bt_header.bth_next = newpage;
        base = &current->bt_entries[halfix];
        break;
    case BTREE_NODE_INTERNAL:
        // one of the entries from current becomes the left pointer
        // in the new node
        // we also propagate one of the entries up, no need to maintain
        // another copy at this level
        nodebuf.bt_header.bth_nentries = (nentries - halfix) - 1;
        nodebuf.bt_header.bth_left = current->bt_entries[halfix].bte_page;
        base = &current->bt_entries[halfix + 1];
        break;
    default:
        assert(0);
        break;
    }
    size_t bytes_tocopy =
            sizeof(struct btree_entry) * nodebuf.bt_header.bth_nentries;
    memcpy(&nodebuf.bt_entries, base, bytes_tocopy);
    bzero(base, bytes_tocopy);

    // If the entry to be inserted is geq than the smallest key
    // in the new right node, the entry should be inserted into that node.
    if (btree_entry_compare(entry, &nodebuf.bt_entries[0]) >= 0) {
        result = btree_insert_entry(f, &nodebuf, entry);
        assert(result == 0);
    } else {
        result = btree_insert_entry(f, current, entry);
        assert(result == 0);
    }

    // Propagate the new entry back to the caller to be inserted into the
    // parent node.
    bzero(&entrybuf, sizeof(struct btree_entry));
    switch (current->bt_header.bth_type) {
    case BTREE_NODE_LEAF:
        // if we are a leaf node, we must propagate a copy of the
        // key up and maintain another copy at this level
        entrybuf.bte_key = nodebuf.bt_entries[0].bte_key;
        entrybuf.bte_page = newpage;
        break;
    case BTREE_NODE_INTERNAL:
        // if we are an internal node, we don't need to maintain another
        // copy of the key at this level
        entrybuf.bte_key =
            current->bt_entries[current->bt_header.bth_nentries - 1].bte_key;
        entrybuf.bte_page = newpage;
        current->bt_header.bth_nentries--;
        bzero(&current->bt_entries[current->bt_header.bth_nentries - 1],
              sizeof(struct btree_entry));
        break;
    default:
        assert(0);
        break;
    }
    // Now we need to synch all the changes in the current and new nodes
    result = btree_node_synch(f, &nodebuf);
    assert(result == 0);
    result = btree_node_synch(f, current);
    assert(result == 0);

    goto success;

  success:
    result = 0;
    *retentry = entrybuf;
    goto done;
  done:
    return result;
}

// PRECONDITION: must be holding lock on column
static
int
btree_insert(struct column *col, struct btree_entry *entry)
{
    assert(col != NULL);
    assert(entry != NULL);

    int result;
    page_t newrootpage;
    struct btree_entry entrybuf;
    bzero(&entrybuf, sizeof(struct btree_entry));
    struct btree_node rootbuf;
    result = file_read(col->col_index_file,
                       col->col_disk.cd_btree_root, &rootbuf);
    assert(result == 0);

    result = btree_insert_helper(col->col_index_file, &rootbuf, entry,
                                 &entrybuf);
    assert(result == 0);

    // If after insertion, our current does not have a sibling, we're done
    if (entrybuf.bte_page == BTREE_PAGE_NULL) {
        newrootpage = col->col_disk.cd_btree_root;
        goto success;
    }

    // If we do have a sibling, we need to create a new internal parent node.
    // The left pointer of the new parent node will be the current node,
    // and the first entry of the new parent node will tbe the returned
    // entry which contains a pointer to the sibling node.
    struct btree_node nodebuf;
    bzero(&nodebuf, sizeof(struct btree_node));
    page_t newpage;
    result = file_alloc_page(col->col_index_file, &newpage);
    assert(result == 0);
    nodebuf.bt_header.bth_type = BTREE_NODE_INTERNAL;
    nodebuf.bt_header.bth_nentries = 1;
    nodebuf.bt_header.bth_page = newpage;
    nodebuf.bt_header.bth_left = rootbuf.bt_header.bth_page;
    nodebuf.bt_entries[0] = entrybuf;
    result = btree_node_synch(col->col_index_file, &nodebuf);
    assert(result == 0);
    newrootpage = newpage;
    goto success;

  success:
    result = 0;
    col->col_disk.cd_btree_root = newrootpage;
    col->col_disk.cd_ntuples++;
    col->col_dirty = true;
    goto done;
  done:
    return result;
}

// PRECONDITION: MUST BE HOLDING COLUMN LOCK
// This will mark the entries in the bitmap for the tuples that satisfy
// the select predicate.
static
int
column_select_btree(struct column *col, struct op *op,
                    struct column_ids *cids)
{
    assert(col != NULL);
    assert(op != NULL);
    assert(cids != NULL);

    int result;
    page_t pleft, pright;
    unsigned ixleft, ixright;
    switch (op->op_type) {
    case OP_SELECT_ALL:
    case OP_SELECT_ALL_ASSIGN:
        for (unsigned i = 0; i < col->col_disk.cd_ntuples; i++) {
            bitmap_mark(cids->cid_bitmap, i);
        }
        result = 0;
        goto done;
    case OP_SELECT_RANGE:
    case OP_SELECT_RANGE_ASSIGN:
        result = btree_search(col, op->op_select.op_sel_low,
                              &pleft, &ixleft);
        if (result) {
            goto done;
        }
        result = btree_search(col, op->op_select.op_sel_high + 1,
                              &pright, &ixright);
        if (result) {
            goto done;
        }
        break;
    case OP_SELECT_VALUE:
    case OP_SELECT_VALUE_ASSIGN:
        result = btree_search(col, op->op_select.op_sel_value,
                              &pleft, &ixleft);
        if (result) {
            goto done;
        }
        result = btree_search(col, op->op_select.op_sel_value + 1,
                              &pright, &ixright);
        if (result) {
            goto done;
        }
        break;
    default:
        assert(0);
        break;
    }
    result = btree_select_range(col, pleft, ixleft, pright, ixright, cids);
    if (result) {
        goto done;
    }

    // success
    result = 0;
    goto done;
  done:
    return result;
}

static
int
column_entry_sorted_compare(const void *a, const void *b)
{
    struct column_entry_sorted *aent = (struct column_entry_sorted *) a;
    struct column_entry_sorted *bent = (struct column_entry_sorted *) b;
    return aent->ce_val - bent->ce_val;
}

// MUST BE HOLDING LOCK ON COLUMN
// Returns all the indices in the sorted entries at positions [left, right)
static
int
column_select_sorted_range(struct column *col, uint64_t left, uint64_t right,
                           struct column_ids *cids)
{
    assert(col != NULL);
    assert(cids != NULL);
    assert(left <= right);
    assert(right <= col->col_disk.cd_ntuples);
    assert(PAGESIZE % sizeof(struct column_entry_sorted) == 0);

    int result;
    struct column_entry_sorted colentrybuf[COLENTRY_SORTED_PER_PAGE];
    page_t bufpage = 0;
    for (uint64_t curtuple = left; curtuple < right; curtuple++) {
        // load the page into memory if it's not already in memory
        page_t curpage =
                FILE_FIRST_PAGE + (curtuple / COLENTRY_SORTED_PER_PAGE);
        if (curpage != bufpage) {
            result = file_read(col->col_index_file, curpage, colentrybuf);
            if (result) {
                goto done;
            }
            bufpage = curpage;
        }
        // mark the bit for this id
        unsigned curindex = curtuple % COLENTRY_SORTED_PER_PAGE;
        bitmap_mark(cids->cid_bitmap, colentrybuf[curindex].ce_index);
    }
    result = 0;
    goto done;
  done:
    return result;
}

// PRECONDITION: MUST BE HOLDING COLUMN LOCK
// retindex will store the lower bound position in the sorted file
// where the tuple *should* be inserted.
static
int
column_search_sorted(struct column *col, int val, uint64_t *retindex)
{
    assert(col != NULL);
    assert(retindex != NULL);

    int result;
    struct column_entry_sorted target;
    target.ce_val = val;
    struct column_entry_sorted colentrybuf[COLENTRY_SORTED_PER_PAGE];
    uint64_t ntuples = col->col_disk.cd_ntuples;

    // We perform a binary search on the pages. for each page we read in,
    // we perform a binary search on the tuples in that page to get
    // a lower bound.
    // we set the left boundary to be the first available page, and we
    // set the right boundary to be the first unavailable page
    unsigned index;
    page_t pbuf = 0; // the current page that's in the buffer
    page_t pl = FILE_FIRST_PAGE;
    page_t plast = FILE_FIRST_PAGE +
            ((ntuples + COLENTRY_SORTED_PER_PAGE - 1) / COLENTRY_SORTED_PER_PAGE);
    page_t pr = plast;
    while (pl < pr) {
        page_t pm = pl + (pr - pl) / 2;
        result = file_read(col->col_index_file, pm, colentrybuf);
        if (result) {
            goto done;
        }
        pbuf = pm;

        // if we're on the last page, we need to make sure we don't
        // read more than the number of tuples on that page
        uint64_t ntuples_in_page = (pm == plast - 1) ?
                ntuples % COLENTRY_SORTED_PER_PAGE : COLENTRY_SORTED_PER_PAGE;

        // Attempt to find the tuple on this page
        // this will return a lower bound index where we *should* insert
        // the value if we were to insert. The tuple index is not necessarily
        // equal to our target value
        index = binary_search(&target, colentrybuf, ntuples_in_page,
                                       sizeof(struct column_entry_sorted),
                                       column_entry_sorted_compare);
        if (index == COLENTRY_SORTED_PER_PAGE) {
            pl = pm + 1; // the tuple lives on the right page
        } else {
            pr = pm; // the tuple *might* live on this page or left
        }
    }
    // If our lower bound is the right boundary, then our tuple is
    // greater than everything in our sorted projection
    if (pl == plast) {
        index = 0;
        goto success;
    }
    // Otherwise, we have pl == pr. Check to see if the page that's in
    // our buffer is equal to pl. If it's not, load the page into
    // the buffer.
    if (pl != pbuf) {
        result = file_read(col->col_index_file, pl, colentrybuf);
        if (result) {
            goto done;
        }
    }
    // Now attempt to find our tuple on this page. Since pl != plast,
    // our lower bound MUST be in this page.
    uint64_t ntuples_in_page = (pl == plast - 1) ?
            ntuples % COLENTRY_SORTED_PER_PAGE : COLENTRY_SORTED_PER_PAGE;
    index = binary_search(&target, colentrybuf, ntuples_in_page,
                          sizeof(struct column_entry_sorted),
                          column_entry_sorted_compare);
    assert(index != COLENTRY_SORTED_PER_PAGE);
    goto success;

  success:
    result = 0;
    *retindex = (pl - FILE_FIRST_PAGE) * COLENTRY_SORTED_PER_PAGE + index;
    goto done;
  done:
    return result;
}

// PRECONDITION: MUST BE HOLDING COLUMN LOCK
// This will mark the entries in the bitmap for the tuples that satisfy
// the select predicate.
static
int
column_select_sorted(struct column *col, struct op *op,
                     struct column_ids *cids)
{
    assert(col != NULL);
    assert(op != NULL);
    assert(cids != NULL);

    int result;
    uint64_t left;
    uint64_t right;
    switch (op->op_type) {
    case OP_SELECT_ALL:
    case OP_SELECT_ALL_ASSIGN:
        left = 0;
        right = col->col_disk.cd_ntuples;
        break;
    case OP_SELECT_RANGE:
    case OP_SELECT_RANGE_ASSIGN:
        // we search for the lower bound of high + 1 so that our
        // range is [low,high] inclusive
        result = column_search_sorted(col, op->op_select.op_sel_low, &left);
        if (result) {
            goto done;
        }
        result = column_search_sorted(col, op->op_select.op_sel_high + 1, &right);
        if (result) {
            goto done;
        }
        break;
    case OP_SELECT_VALUE:
    case OP_SELECT_VALUE_ASSIGN:
        result = column_search_sorted(col, op->op_select.op_sel_value, &left);
        if (result) {
            goto done;
        }
        result = column_search_sorted(col, op->op_select.op_sel_value + 1, &right);
        if (result) {
            goto done;
        }
        break;
    default:
        assert(0);
        break;
    }
    result = column_select_sorted_range(col, left, right, cids);
    if (result) {
        goto done;
    }

    // success
    result = 0;
    goto done;
  done:
    return result;
}

static
bool
column_select_predicate(int candidateval, struct op *op)
{
    assert(op != NULL);
    switch (op->op_type) {
    case OP_SELECT_ALL:
    case OP_SELECT_ALL_ASSIGN:
        return true;
    case OP_SELECT_RANGE:
    case OP_SELECT_RANGE_ASSIGN:
        return (candidateval >= op->op_select.op_sel_low)
               && (candidateval <= op->op_select.op_sel_high);
    case OP_SELECT_VALUE:
    case OP_SELECT_VALUE_ASSIGN:
        return (candidateval == op->op_select.op_sel_value);
    default:
        assert(0);
        return false;
    }
}

// PRECONDITION: MUST BE HOLDING COLUMN LOCK
// This will mark the entries in the bitmap for the tuples that satisfy
// the select predicate.
static
int
column_select_unsorted(struct column *col, struct op *op,
                       struct column_ids *cids)
{
    assert(col != NULL);
    assert(op != NULL);
    assert(cids != NULL);
    assert(PAGESIZE % sizeof(struct column_entry_unsorted) == 0);

    int result;
    struct column_entry_unsorted colentrybuf[COLENTRY_UNSORTED_PER_PAGE];
    uint64_t scanned = 0;
    uint64_t ntuples = col->col_disk.cd_ntuples;
    page_t page = FILE_FIRST_PAGE;
    while (scanned < ntuples) {
        result = file_read(col->col_base_file, page, colentrybuf);
        if (result) {
            goto done;
        }
        uint64_t toscan = MIN(ntuples - scanned, COLENTRY_UNSORTED_PER_PAGE);
        for (unsigned i = 0; i < toscan; i++, scanned++) {
            struct column_entry_unsorted entry = colentrybuf[i];
            if (column_select_predicate(entry.ce_val, op)) {
                bitmap_mark(cids->cid_bitmap, scanned);
            }
        }
        page++;
    }
    assert(scanned == ntuples);
    result = 0;
    goto done;
  done:
    return result;
}

struct column_ids *
column_select(struct column *col, struct op *op)
{
    assert(col != NULL);
    assert(op != NULL);
    rwlock_acquire_read(col->col_rwlock);

    int result;
    struct column_ids *cids = malloc(sizeof(struct column_ids));
    if (cids == NULL) {
        goto done;
    }
    cids->cid_bitmap = bitmap_create(col->col_disk.cd_ntuples);
    if (cids->cid_bitmap == NULL) {
        goto cleanup_malloc;
    }
    // select based on the storage type of the column
    switch (col->col_disk.cd_stype) {
    case STORAGE_UNSORTED:
        result = column_select_unsorted(col, op, cids);
        break;
    case STORAGE_SORTED:
        result = column_select_sorted(col, op, cids);
        break;
    case STORAGE_BTREE:
        result = column_select_btree(col, op, cids);
        break;
    default:
        assert(0);
        break;
    }
    if (result) {
        goto cleanup_bitmap;
    }
    // success
    goto done;

  cleanup_bitmap:
    bitmap_destroy(cids->cid_bitmap);
  cleanup_malloc:
    free(cids);
    cids = NULL;
  done:
    rwlock_release(col->col_rwlock);
    return cids;
}

void
column_ids_destroy(struct column_ids *cids)
{
    assert(cids != NULL);
    bitmap_destroy(cids->cid_bitmap);
    free(cids);
}

// PRECONDITION: MUST BE HOLDING LOCK ON COLUMN
static
int
column_fetch_base_data(struct column *col, struct column_ids *ids,
                       struct valarray *vals)
{
    int result;
    uint64_t ntuples = col->col_disk.cd_ntuples;
    page_t curpage = 0;
    struct column_entry_unsorted colentrybuf[COLENTRY_UNSORTED_PER_PAGE];
    for (uint64_t i = 0; i < ntuples; i++) {
        if (!bitmap_isset(ids->cid_bitmap, i)) {
            continue;
        }
        page_t requestedpage =
                FILE_FIRST_PAGE + (i / COLENTRY_UNSORTED_PER_PAGE);
        assert(requestedpage != 0);
        // if the requested page is not the current page in the buffer,
        // read in that page and update the curpage
        if (requestedpage != curpage) {
            result = file_read(col->col_base_file, requestedpage, colentrybuf);
            if (result) {
                goto done;
            }
            curpage = requestedpage;
        }
        unsigned requestedindex = i % COLENTRY_UNSORTED_PER_PAGE;
        int val = colentrybuf[requestedindex].ce_val;
        result = valarray_add(vals, (void *) val, NULL);
        if (result) {
            goto done;
        }
    }
    // success
    result = 0;
    goto done;
  done:
    return result;
}

struct column_vals *
column_fetch(struct column *col, struct column_ids *ids)
{
    assert(col != NULL);
    assert(ids != NULL);
    rwlock_acquire_read(col->col_rwlock);
    uint64_t ntuples = col->col_disk.cd_ntuples;
    assert(ntuples == bitmap_nbits(ids->cid_bitmap));

    struct column_vals *cvals = NULL;
    struct valarray *vals = valarray_create();
    if (vals == NULL) {
        goto done;
    }
    int result;
    switch (col->col_disk.cd_stype) {
    case STORAGE_UNSORTED:
    case STORAGE_SORTED:
    case STORAGE_BTREE:
        // we always use the base data to fetch the values
        result = column_fetch_base_data(col, ids, vals);
        break;
    default:
        assert(0);
        break;
    }
    if (result) {
        goto cleanup_vals;
    }

    // memcpy the results from the resizable array into the pointer in this
    // struct column_vals
    cvals = malloc(sizeof(struct column_vals));
    if (cvals == NULL) {
        goto cleanup_vals;
    }
    cvals->cval_len = valarray_num(vals);
    cvals->cval_vals = malloc(sizeof(int) * cvals->cval_len);
    if (cvals->cval_vals == NULL) {
        goto cleanup_malloc;
    }
    memcpy(cvals->cval_vals, vals->arr.v, sizeof(int) * cvals->cval_len);
    // sort the values before returning
    qsort(cvals->cval_vals, cvals->cval_len, sizeof(int), int_compare);
    result = 0;
    goto cleanup_vals;

  cleanup_malloc:
    free(cvals);
    cvals = NULL;
  cleanup_vals:
    while (valarray_num(vals) > 0) {
        // no need to free the values because they're just ints
        valarray_remove(vals, 0);
    }
    valarray_destroy(vals);
  done:
    rwlock_release(col->col_rwlock);
    return cvals;
}

void
column_vals_destroy(struct column_vals *vals)
{
    assert(vals != NULL);
    free(vals->cval_vals);
    free(vals);
}

static
int
column_load_unsorted(struct file *f, int *vals, uint64_t num)
{
    int result;
    int intbuf[PAGESIZE / sizeof(int)];
    uint64_t curtuple = 0;
    while (curtuple < num) {
        uint64_t tuples_tocopy =
                MIN(COLENTRY_UNSORTED_PER_PAGE, num - curtuple);
        size_t bytes_tocopy = tuples_tocopy * sizeof(int);
        bzero(intbuf, PAGESIZE);
        memcpy(intbuf, vals + curtuple, bytes_tocopy);
        page_t page;
        result = file_alloc_page(f, &page);
        if (result) {
            goto done;
        }
        result = file_write(f, page, intbuf);
        if (result) {
            file_free_page(f, page);
            goto done;
        }
        curtuple += tuples_tocopy;
    }
  done:
    return result;
}

static
int
column_load_index_sorted(struct file *f, int *vals, uint64_t num)
{
    assert(f != NULL);
    assert(vals != NULL);

    int result;
    // create the entries in memory and sort them
    struct column_entry_sorted *entries =
            malloc(num * sizeof(struct column_entry_sorted));
    if (entries == NULL) {
        result = -1;
        goto done;
    }
    for (uint64_t i = 0; i < num; i++) {
        entries[i].ce_val = vals[i];
        entries[i].ce_index = i;
    }
    qsort(entries, num, sizeof(struct column_entry_sorted),
          column_entry_sorted_compare);

    // write the entries out to disk, one page at a time
    struct column_entry_sorted colentrybuf[COLENTRY_SORTED_PER_PAGE];
    uint64_t curtuple = 0;
    while (curtuple < num) {
        uint64_t tuples_tocopy = MIN(COLENTRY_SORTED_PER_PAGE, num - curtuple);
        // avoid overflow
        size_t bytes_tocopy = sizeof(struct column_entry_sorted)
                * tuples_tocopy;
        bzero(colentrybuf, PAGESIZE);
        memcpy(colentrybuf, entries + curtuple, bytes_tocopy);
        page_t page;
        result = file_alloc_page(f, &page);
        if (result) {
            goto cleanup_malloc;
        }
        result = file_write(f, page, colentrybuf);
        if (result) {
            file_free_page(f, page);
            goto cleanup_malloc;
        }
        curtuple += tuples_tocopy;
    }
    result = 0;
    goto cleanup_malloc;
  cleanup_malloc:
    free(entries);
  done:
    return result;
}

static
int
column_load_index_btree(struct column *col, int *vals, uint64_t num)
{
    assert(col->col_index_file != NULL);
    assert(vals != NULL);
    assert(col->col_disk.cd_ntuples == 0);
    int result;

    // the root page should have been created in storage_add_column
    assert(col->col_disk.cd_btree_root != BTREE_PAGE_NULL);

    struct btree_node root;
    bzero(&root, sizeof(struct btree_node));
    root.bt_header.bth_type = BTREE_NODE_LEAF;
    root.bt_header.bth_next = BTREE_PAGE_NULL;
    root.bt_header.bth_page = col->col_disk.cd_btree_root;
    root.bt_header.bth_nentries = 0;
    result = btree_node_synch(col->col_index_file, &root);
    if (result) {
        goto done;
    }

    // Now insert all the tuples one by one
    for (uint64_t i = 0; i < num; i++) {
        struct btree_entry entry;
        bzero(&entry, sizeof(struct btree_entry));
        entry.bte_key = vals[i];
        entry.bte_index = i;
        result = btree_insert(col, &entry);
        assert(result == 0);
    }

    // success
    result = 0;
    goto done;
  done:
    return result;
}

// PRECONDITION: MUST BE HOLDING LOCK
static
int
column_insert_btree(struct column *col, int val)
{
    assert(col != NULL);
    assert(col->col_disk.cd_stype == STORAGE_BTREE);

    uint64_t index = col->col_disk.cd_ntuples;
    struct btree_entry entry;
    bzero(&entry, sizeof(struct btree_entry));
    entry.bte_key = val;
    entry.bte_index = index;
    return btree_insert(col, &entry);
}

// PRECONDITION: MUST BE HOLDING LOCK
static
int
column_insert_sorted(struct column *col, int val)
{
    assert(col != NULL);
    assert(col->col_disk.cd_stype == STORAGE_SORTED);

    int result;
    uint64_t index = col->col_disk.cd_ntuples;
    struct column_entry_sorted entry;
    bzero(&entry, sizeof(struct column_entry_sorted));
    entry.ce_val = val;
    entry.ce_index = index;

    // Find the lower bound on where we should insert this new value
    uint64_t position;
    result = column_search_sorted(col, val, &position);
    if (result) {
        goto done;
    }
    page_t newpage;

    // Insert the value and shift all the pages over
    page_t page = FILE_FIRST_PAGE + (position / COLENTRY_SORTED_PER_PAGE);
    uint64_t ix = position % COLENTRY_SORTED_PER_PAGE;
    struct column_entry_sorted tempentry = entry;
    struct column_entry_sorted colbuf[COLENTRY_SORTED_PER_PAGE];
    while (1) {
        // if the page hasn't been allocated before, then
        // we are inserting at the end
        if (!file_page_isalloc(col->col_index_file, page)) {
            assert(ix == 0);
            result = file_alloc_page(col->col_index_file, &newpage);
            if (result) {
                goto cleanup_page;
            }
            assert(page == newpage);
        }
        result = file_read(col->col_index_file, page, colbuf);
        if (result) {
            goto done;
        }
        uint64_t ntuples_before =
                (page - FILE_FIRST_PAGE) * COLENTRY_SORTED_PER_PAGE;
        unsigned ntuples_this = MIN(COLENTRY_SORTED_PER_PAGE,
                col->col_disk.cd_ntuples - ntuples_before);
        // if there is space in this page, shift everything over
        // and then insert it
        if (ntuples_this < COLENTRY_SORTED_PER_PAGE) {
            memmove(&colbuf[ix + 1], &colbuf[ix],
                    (ntuples_this - ix) * sizeof(struct column_entry_sorted));
            colbuf[ix] = entry;
            result = file_write(col->col_index_file, page, colbuf);
            assert(result == 0);
            break;
        }

        // otherwise we need to save the last item to insert into
        // the next page
        tempentry = colbuf[ntuples_this - 1]; // save the last entry
        memmove(&colbuf[ix + 1], &colbuf[ix],
                (ntuples_this - ix - 1) * sizeof(struct column_entry_sorted));
        colbuf[ix] = entry;
        result = file_write(col->col_index_file, page, colbuf);
        assert(result == 0);

        entry = tempentry;
        ix = 0;
        page++;
    }
    col->col_disk.cd_ntuples++;
    col->col_dirty = true;

    // success
    result = 0;
    goto done;
  cleanup_page:
    file_free_page(col->col_index_file, newpage);
  done:
    return result;
}

// PRECONDITION: MUST BE HOLDING LOCK
static
int
column_insert_unsorted(struct column *col, int val)
{
    assert(col != NULL);

    int result;
    uint64_t index = col->col_disk.cd_ntuples;
    page_t page = FILE_FIRST_PAGE + index / COLENTRY_UNSORTED_PER_PAGE;
    page_t newpage;
    if (!file_page_isalloc(col->col_base_file, page)) {
        result = file_alloc_page(col->col_base_file, &newpage);
        if (result) {
            goto done;
        }
        assert(page == newpage);
    }
    struct column_entry_unsorted colbuf[COLENTRY_UNSORTED_PER_PAGE];
    result = file_read(col->col_base_file, page, colbuf);
    if (result) {
        goto cleanup_page;
    }
    unsigned ix = index % COLENTRY_UNSORTED_PER_PAGE;
    colbuf[ix].ce_val = val;
    result = file_write(col->col_base_file, page, colbuf);
    if (result) {
        goto cleanup_page;
    }

    // success
    result = 0;
    goto done;
  cleanup_page:
    file_free_page(col->col_base_file, newpage);
  done:
    return result;
}

int
column_insert(struct column *col, int val)
{
    assert(col != NULL);
    int result;
    rwlock_acquire_write(col->col_rwlock);

    // we always insert into the unsorted projection as well
    // because we use this for fetching
    result = column_insert_unsorted(col, val);
    if (result) {
        goto done;
    }
    switch (col->col_disk.cd_stype) {
    case STORAGE_BTREE:
        result = column_insert_btree(col, val);
        break;
    case STORAGE_SORTED:
        result = column_insert_sorted(col, val);
        break;
    case STORAGE_UNSORTED:
        col->col_disk.cd_ntuples++;
        col->col_dirty = true;
        break;
    default:
        assert(0);
        break;
    }

    // success
    result = 0;
    goto done;
  done:
    rwlock_release(col->col_rwlock);
    return result;
}

int
column_load(struct column *col, int *vals, uint64_t num)
{
    assert(col != NULL);
    assert(vals != NULL);
    int result;
    rwlock_acquire_write(col->col_rwlock);
    // if we've already loaded this column, prevent a double load
    if (col->col_disk.cd_ntuples > 0) {
        result = 0;
        goto done;
    }

    // TODO: also create index file
    result = column_load_unsorted(col->col_base_file, vals, num);
    switch (col->col_disk.cd_stype) {
    case STORAGE_BTREE:
        result = column_load_index_btree(col, vals, num);
        break;
    case STORAGE_SORTED:
        result = column_load_index_sorted(col->col_index_file, vals, num);
        col->col_disk.cd_ntuples += num;
        col->col_dirty = true;
        break;
    case STORAGE_UNSORTED:
        col->col_disk.cd_ntuples += num;
        col->col_dirty = true;
        break;
    default:
        assert(0);
        break;
    }
    goto done;

  done:
    rwlock_release(col->col_rwlock);
    return result;
}
