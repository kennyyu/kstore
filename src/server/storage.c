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
#include "../common/include/dberror.h"
#include "../common/include/try.h"
#include "../common/include/results.h"

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
    struct storage *storage;

    TRYNULL(result, DBENOMEM, storage, malloc(sizeof(struct storage)), done);
    strcpy(storage->st_dbdir, dbdir);
    TRYNULL(result, DBENOMEM, storage->st_lock, lock_create(), cleanup_malloc);
    TRYNULL(result, DBENOMEM, storage->st_open_cols, columnarray_create(), cleanup_lock);
    // create the directory if it doesn't already exist
    // it will return ENOENT if it already exists
    result = mkdir(dbdir, S_IRWXU);
    if (result == -1 && errno != EEXIST) {
        goto cleanup_colarray;
    }
    char buf[128];
    sprintf(buf, "%s/%s", dbdir, METADATA_FILENAME);
    TRYNULL(result, DBEIONOFILE, storage->st_file, file_open(buf), cleanup_mkdir);

    result = 0;
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
                if (!free_found) {
                    free_found = true;
                    free_page = page;
                    free_colindex = i;
                }
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
    TRY(result, file_read(storage->st_file, page, colbuf), done);
    colbuf[index] = *coldisk;
    TRY(result, file_write(storage->st_file, page, colbuf), done);

    result = 0;
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
    struct file *colfile;
    TRYNULL(result, DBEFILE, colfile, file_open(filenamebuf), done);
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
        struct file *colindexfile;
        TRYNULL(result, DBEFILE, colindexfile, file_open(indexnamebuf), cleanup_file);
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
            struct btree_node root;
            bzero(&root, sizeof(struct btree_node));
            root.bt_header.bth_type = BTREE_NODE_LEAF;
            root.bt_header.bth_next = BTREE_PAGE_NULL;
            root.bt_header.bth_page = rootpage;
            root.bt_header.bth_nentries = 0;
            result = btree_node_synch(colindexfile, &root);
            if (result) {
                file_close(colindexfile);
                goto cleanup_file;
            }
            newcol.cd_btree_root = rootpage;
        }
        file_close(colindexfile);
    }

    // if we couldn't find a free slot earlier, we need to extend
    // the storage metadata file
    if (!freefound) {
        TRY(result, file_alloc_page(storage->st_file, &colpage), cleanup_file);
        colindex = 0;
    }
    // finally write the new column to disk
    TRY(result, storage_synch_column(storage, &newcol, colpage, colindex), cleanup_page);

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
int
column_open(struct storage *storage, char *colname, struct column **retcol)
{
    assert(storage != NULL);
    assert(colname != NULL);
    assert(retcol != NULL);

    lock_acquire(storage->st_lock);
    int result = 0;
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
        result = DBECOLEXISTS;
        DBLOG(result);
        goto done;
    }
    struct column_on_disk colbuf[COLUMNS_PER_PAGE];
    result = file_read(storage->st_file, colpage, colbuf);
    assert(colbuf[colindex].cd_magic == COLUMN_TAKEN);

    // allocate space for the in-memory representation of the column
    TRYNULL(result, DBENOMEM, col, malloc(sizeof(struct column)), done);
    memcpy(&col->col_disk, &colbuf[colindex], sizeof(struct column_on_disk));

    // open the base file for the column
    char filenamebuf[56];
    sprintf(filenamebuf, "%s/%s", storage->st_dbdir,
            col->col_disk.cd_base_file);
    TRYNULL(result, DBEFILE, col->col_base_file, file_open(filenamebuf), cleanup_malloc);

    // open the index file for the column if it exists
    // only columns that have btree and sorted storage
    col->col_index_file = NULL;
    if ((col->col_disk.cd_stype == STORAGE_BTREE
         || col->col_disk.cd_stype == STORAGE_SORTED)) {
        sprintf(filenamebuf, "%s/%s", storage->st_dbdir,
                col->col_disk.cd_index_file);
        TRYNULL(result, DBEFILE, col->col_index_file, file_open(filenamebuf), cleanup_file);
    }

    // allocate the lock to protect the column
    TRYNULL(result, DBENOMEM, col->col_rwlock, rwlock_create(), cleanup_file);
    col->col_page = colpage;
    col->col_index = colindex;
    col->col_opencount = 1;
    col->col_storage = storage;
    col->col_dirty = false;

    // finally, add this column to the array of open columns
    TRY(result, columnarray_add(storage->st_open_cols, col, NULL), cleanup_lock);
    result = 0;
    *retcol = col;
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
    return result;
}

// dec ref count, close it if 0
void
column_close(struct column *col)
{
    assert(col != NULL);
    int result;
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
        TRY(result,
            storage_synch_column(storage, &col->col_disk,
                                 col->col_page, col->col_index),
            done);
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
    if (col->col_disk.cd_stype == STORAGE_BTREE || col->col_disk.cd_stype == STORAGE_SORTED) {
        file_close(col->col_index_file);
    }
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
                   int low, int high,
                   page_t pleft, unsigned ixleft,
                   page_t pright, unsigned ixright,
                   struct column_ids *cids)
{
    assert(col != NULL);
    assert(cids != NULL);
    assert(cids->cid_type == CID_BITMAP);
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
        TRY(result, file_read(col->col_index_file, curpage, &nodebuf), done);

        assert(nodebuf.bt_header.bth_type == BTREE_NODE_LEAF);
        assert(nodebuf.bt_header.bth_page == curpage);

        unsigned rightbound = (curpage == pright) ? ixright
                : nodebuf.bt_header.bth_nentries;
        for (/* none */; curix < rightbound; curix++) {
            struct btree_entry *entry = &nodebuf.bt_entries[curix];
            assert(low <= entry->bte_key);
            assert(entry->bte_key <= high);
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
        TRY(result, file_read(col->col_index_file, curpage, &nodebuf), done);
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
    unsigned ix = 0; // pointer to chase for internal nodes
    struct btree_entry entrybuf;
    bzero(&entrybuf, sizeof(struct btree_entry));
    struct btree_node nodebuf;
    bzero(&nodebuf, sizeof(struct btree_node));

    // If we have an internal node, recurse down until we can insert entry.
    // Each invocation will possibly propagate up to the caller a new
    // entry to insert into the current node.
    if (current->bt_header.bth_type == BTREE_NODE_INTERNAL) {
        assert(current->bt_header.bth_nentries != 0);
        ix = binary_search(entry,
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

        // If we have room in this node, then just insert it.
        unsigned nentries = current->bt_header.bth_nentries;
        if (nentries < BTENTRY_PER_PAGE) {
            assert(ix <= nentries);
            if (ix < nentries) {
                memmove(&current->bt_entries[ix + 1], &current->bt_entries[ix],
                        sizeof(struct btree_entry) * (nentries - ix));
            }
            current->bt_entries[ix] = entrybuf;
            current->bt_header.bth_nentries++;
            result = btree_node_synch(f, current);
            assert(result == 0);
            bzero(&entrybuf, sizeof(struct btree_entry));
            goto success;
        }
    } else {
        if (current->bt_header.bth_nentries < BTENTRY_PER_PAGE) {
            result = btree_insert_entry(f, current, entry);
            assert(result == 0);
            bzero(&entrybuf, sizeof(struct btree_entry));
            goto success;
        }
    }

    // Otherwise, we need to split and copy half the entries to the new node.
    unsigned nentries = current->bt_header.bth_nentries;
    bzero(&nodebuf, sizeof(struct btree_node));
    page_t newpage;
    result = file_alloc_page(f, &newpage);
    assert(result == 0);
    unsigned halfix = nentries / 2;
    nodebuf.bt_header.bth_type = current->bt_header.bth_type;
    nodebuf.bt_header.bth_page = newpage;
    struct btree_entry *base;
    switch (current->bt_header.bth_type) {
    case BTREE_NODE_LEAF:
        current->bt_header.bth_nentries = halfix;
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
        current->bt_header.bth_nentries = halfix + 1;
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

    // Now insert the entry into the appropriate position in this node
    if (current->bt_header.bth_type == BTREE_NODE_INTERNAL) {
        // If we are an internal node, we must insert the new entry
        // at the position adjacent to the pointer we chased in our recursion.
        // We need to determine if the desired location is in the right or
        // left node.
        unsigned left_nentries = current->bt_header.bth_nentries;
        if (ix < left_nentries) {
            memmove(&current->bt_entries[ix + 1], &current->bt_entries[ix],
                    sizeof(struct btree_entry) * (left_nentries - ix));
            current->bt_entries[ix] = *entry;
            current->bt_header.bth_nentries++;
        } else {
            unsigned right_nentries = nodebuf.bt_header.bth_nentries;
            unsigned rightix = ix - left_nentries;
            memmove(&nodebuf.bt_entries[rightix + 1], &nodebuf.bt_entries[rightix],
                    sizeof(struct btree_entry) * (right_nentries - rightix));
            nodebuf.bt_entries[rightix] = *entry;
            nodebuf.bt_header.bth_nentries++;
        }
    } else {
        // If the entry to be inserted is geq than the smallest key
        // in the new right node, the entry should be inserted into that node.
        if (btree_entry_compare(entry, &nodebuf.bt_entries[0]) >= 0) {
            result = btree_insert_entry(f, &nodebuf, entry);
            assert(result == 0);
        } else {
            result = btree_insert_entry(f, current, entry);
            assert(result == 0);
        }
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
        bzero(&current->bt_entries[current->bt_header.bth_nentries - 1],
              sizeof(struct btree_entry));
        current->bt_header.bth_nentries--;
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
    assert(cids->cid_type == CID_BITMAP);

    int result;
    page_t pleft, pright;
    int low, high;
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
        low = op->op_select.op_sel_low;
        high = op->op_select.op_sel_high;
        break;
    case OP_SELECT_VALUE:
    case OP_SELECT_VALUE_ASSIGN:
        low = op->op_select.op_sel_value;
        high = op->op_select.op_sel_value;
        break;
    default:
        assert(0);
        break;
    }
    TRY(result, btree_search(col, low, &pleft, &ixleft), done);
    TRY(result, btree_search(col, high + 1, &pright, &ixright), done);
    TRY(result, btree_select_range(col, low, high, pleft, ixleft, pright, ixright, cids), done);

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
    assert(cids->cid_type == CID_BITMAP);
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
            TRY(result, file_read(col->col_index_file, curpage, colentrybuf), done);
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
        TRY(result, file_read(col->col_index_file, pm, colentrybuf), done);
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
        TRY(result, file_read(col->col_index_file, pl, colentrybuf), done);
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
    assert(cids->cid_type == CID_BITMAP);

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
        TRY(result, column_search_sorted(col, op->op_select.op_sel_low, &left), done);
        TRY(result, column_search_sorted(col, op->op_select.op_sel_high + 1, &right), done);
        break;
    case OP_SELECT_VALUE:
    case OP_SELECT_VALUE_ASSIGN:
        TRY(result, column_search_sorted(col, op->op_select.op_sel_value, &left), done);
        TRY(result, column_search_sorted(col, op->op_select.op_sel_value + 1, &right), done);
        break;
    default:
        assert(0);
        break;
    }
    TRY(result, column_select_sorted_range(col, left, right, cids), done);

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
    assert(cids->cid_type == CID_BITMAP);
    assert(PAGESIZE % sizeof(struct column_entry_unsorted) == 0);

    int result;
    struct column_entry_unsorted colentrybuf[COLENTRY_UNSORTED_PER_PAGE];
    uint64_t scanned = 0;
    uint64_t ntuples = col->col_disk.cd_ntuples;
    page_t page = FILE_FIRST_PAGE;
    while (scanned < ntuples) {
        TRY(result, file_read(col->col_base_file, page, colentrybuf), done);
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
    struct column_ids *cids;
    TRYNULL(result, DBENOMEM, cids, malloc(sizeof(struct column_ids)), done);
    TRYNULL(result, DBENOMEM,
            cids->cid_bitmap, bitmap_create(col->col_disk.cd_ntuples), cleanup_malloc);
    cids->cid_type = CID_BITMAP;
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
    result = 0;
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

struct fetch_tuple {
    unsigned fetch_index;
    unsigned fetch_id;
    int fetch_val;
};

static
int
fetch_tuple_compare_index(const void *a, const void *b)
{
    struct fetch_tuple *ap = (struct fetch_tuple *) a;
    struct fetch_tuple *bp = (struct fetch_tuple *) b;
    return (int) (ap->fetch_index - bp->fetch_index);
}

static
int
fetch_tuple_compare_id(const void *a, const void *b)
{
    struct fetch_tuple *ap = (struct fetch_tuple *) a;
    struct fetch_tuple *bp = (struct fetch_tuple *) b;
    return (int) (ap->fetch_id - bp->fetch_id);
}

static
void
fetch_tuples_sort_index(struct fetch_tuple *tuples, unsigned len)
{
    qsort(tuples, len, sizeof(struct fetch_tuple), fetch_tuple_compare_index);
}

static
void
fetch_tuples_sort_id(struct fetch_tuple *tuples, unsigned len)
{
    qsort(tuples, len, sizeof(struct fetch_tuple), fetch_tuple_compare_id);
}

// PRECONDITION: MUST BE HOLDING LOCK ON COLUMN
static
int
column_fetch_base_data(struct column *col, struct column_ids *ids,
                       struct valarray *vals, struct idarray *fetchids,
                       struct fetch_tuple *ftuples)
{
    int result;
    struct cid_iterator iter;
    cid_iter_init(&iter, ids);

    page_t curpage = 0;
    struct column_entry_unsorted colentrybuf[COLENTRY_UNSORTED_PER_PAGE];
    unsigned ni = 0;
    while (cid_iter_has_next(&iter)) {
        uint64_t i = cid_iter_get(&iter);
        assert(i < col->col_disk.cd_ntuples);
        page_t requestedpage =
                FILE_FIRST_PAGE + (i / COLENTRY_UNSORTED_PER_PAGE);
        assert(requestedpage != 0);
        // if the requested page is not the current page in the buffer,
        // read in that page and update the curpage
        if (requestedpage != curpage) {
            TRY(result, file_read(col->col_base_file, requestedpage, colentrybuf), done);
            curpage = requestedpage;
        }
        unsigned requestedindex = i % COLENTRY_UNSORTED_PER_PAGE;
        int val = colentrybuf[requestedindex].ce_val;
        if (ids->cid_type == CID_BITMAP) {
            TRY(result, valarray_add(vals, (void *) val, NULL), done);
            TRY(result, idarray_add(fetchids, (void *) (unsigned) i, NULL), done);
        } else {
            assert(ftuples[ni].fetch_id == i);
            ftuples[ni].fetch_val = val;
            ni++;
        }
    }
    // success
    result = 0;
    goto done;
  done:
    cid_iter_cleanup(&iter);
    return result;
}

struct column_vals *
column_fetch(struct column *col, struct column_ids *ids)
{
    assert(col != NULL);
    assert(ids != NULL);
    int result;
    struct column_vals *cvals = NULL;

    rwlock_acquire_read(col->col_rwlock);
    uint64_t ntuples = col->col_disk.cd_ntuples;
    switch (ids->cid_type) {
    case CID_BITMAP:
        if (ntuples != bitmap_nbits(ids->cid_bitmap)) {
            result = DBECOLDIFFLEN;
            DBLOG(result);
            goto done;
        }
        break;
    case CID_ARRAY:
        break;
    default: assert(0); break;
    }

    struct fetch_tuple *ftuples = NULL;
    struct valarray *vals = NULL;
    struct idarray *fetchids = NULL;

    if (ids->cid_type == CID_ARRAY) {
        // If we have an array, sort the array so that we can
        // fetch in sequential order. After the fetch, we sort
        // the ftuples based on the original index to maintain
        // alignment fetching after a join.
        unsigned len = idarray_num(ids->cid_array);
        TRYNULL(result, DBENOMEM, ftuples,
                malloc(sizeof(struct fetch_tuple) * len), done);
        for (unsigned i = 0; i < len; i++) {
            ftuples[i].fetch_index = i;
            ftuples[i].fetch_id = (unsigned) idarray_get(ids->cid_array, i);
        }
        fetch_tuples_sort_id(ftuples, len);
        for (unsigned i = 0; i < len; i++) {
            idarray_set(ids->cid_array, i, (void *) ftuples[i].fetch_id);
        }
    } else {
        TRYNULL(result, DBENOMEM, vals, valarray_create(), cleanup_temps);
        TRYNULL(result, DBENOMEM, fetchids, idarray_create(), cleanup_temps);
    }

    switch (col->col_disk.cd_stype) {
    case STORAGE_UNSORTED:
    case STORAGE_SORTED:
    case STORAGE_BTREE:
        // we always use the base data to fetch the values
        TRY(result, column_fetch_base_data(col, ids, vals, fetchids, ftuples), cleanup_temps);
        break;
    default:
        assert(0);
        break;
    }

    // Copy the ids and values into the cval struct
    TRYNULL(result, DBENOMEM, cvals, malloc(sizeof(struct column_vals)), cleanup_temps);
    bzero(cvals, sizeof(struct column_vals));
    strcpy(cvals->cval_col, col->col_disk.cd_col_name);
    cvals->cval_len = (ids->cid_type == CID_ARRAY) ? idarray_num(ids->cid_array)
            : idarray_num(fetchids);
    TRYNULL(result, DBENOMEM, cvals->cval_vals,
            malloc(sizeof(int) * cvals->cval_len), cleanup_malloc);
    TRYNULL(result, DBENOMEM, cvals->cval_ids,
            malloc(sizeof(unsigned) * cvals->cval_len), cleanup_malloc);

    if (ids->cid_type == CID_ARRAY) {
        unsigned len = idarray_num(ids->cid_array);
        fetch_tuples_sort_index(ftuples, len);
        for (unsigned i = 0; i < len; i++) {
            idarray_set(ids->cid_array, i, (void *) ftuples[i].fetch_id);
            cvals->cval_ids[i] = ftuples[i].fetch_id;
            cvals->cval_vals[i] = ftuples[i].fetch_val;
        }
    } else {
        assert(valarray_num(vals) == idarray_num(fetchids));
        memcpy(cvals->cval_vals, vals->arr.v, sizeof(int) * cvals->cval_len);
        memcpy(cvals->cval_ids, fetchids->arr.v, sizeof(unsigned) * cvals->cval_len);
    }

    result = 0;
    goto cleanup_temps;

  cleanup_malloc:
    free(cvals);
    cvals = NULL;
  cleanup_temps:
    if (vals != NULL) {
        vals->arr.num = 0;
        valarray_destroy(vals);
    }
    if (fetchids != NULL) {
        fetchids->arr.num = 0;
        idarray_destroy(fetchids);
    }
    if (ftuples != NULL) {
        free(ftuples);
    }
  done:
    rwlock_release(col->col_rwlock);
    return cvals;
}

static
int
column_load_unsorted(struct file *f, int *vals, uint64_t num)
{
    int result = 0;
    int intbuf[PAGESIZE / sizeof(int)];
    uint64_t curtuple = 0;
    while (curtuple < num) {
        uint64_t tuples_tocopy =
                MIN(COLENTRY_UNSORTED_PER_PAGE, num - curtuple);
        size_t bytes_tocopy = tuples_tocopy * sizeof(int);
        bzero(intbuf, PAGESIZE);
        memcpy(intbuf, vals + curtuple, bytes_tocopy);
        page_t page;
        TRY(result, file_alloc_page(f, &page), done);
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
    struct column_entry_sorted *entries;
    TRYNULL(result, DBENOMEM, entries,
            malloc(num * sizeof(struct column_entry_sorted)), done);
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
        TRY(result, file_alloc_page(f, &page), cleanup_malloc);
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
            TRY(result, file_alloc_page(col->col_index_file, &newpage), cleanup_page);
            assert(page == newpage);
        }
        TRY(result, file_read(col->col_index_file, page, colbuf), done);
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
        TRY(result, file_alloc_page(col->col_base_file, &newpage), done);
        assert(page == newpage);
    }
    struct column_entry_unsorted colbuf[COLENTRY_UNSORTED_PER_PAGE];
    TRY(result, file_read(col->col_base_file, page, colbuf), cleanup_page);
    unsigned ix = index % COLENTRY_UNSORTED_PER_PAGE;
    colbuf[ix].ce_val = val;
    TRY(result, file_write(col->col_base_file, page, colbuf), cleanup_page);

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
    TRY(result, column_insert_unsorted(col, val), done);
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
    if (result) {
        goto done;
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
