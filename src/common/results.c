#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include "include/bitmap.h"
#include "include/results.h"

void
column_ids_destroy(struct column_ids *cids)
{
    assert(cids != NULL);
    switch (cids->cid_type) {
    case CID_BITMAP:
        bitmap_destroy(cids->cid_bitmap);
        break;
    case CID_ARRAY:
        free(cids->cid_ids);
        break;
    default: assert(0); break;
    }
    free(cids);
}

void
column_vals_destroy(struct column_vals *vals)
{
    assert(vals != NULL);
    free(vals->cval_vals);
    free(vals);
}

void
cid_iter_init(struct cid_iterator *iter, struct column_ids *cids)
{
    assert(iter != NULL);
    assert(cids != NULL);
    iter->ciditer_ids = cids;
    iter->ciditer_i = 0;
}

bool
cid_iter_has_next(struct cid_iterator *iter)
{
    assert(iter != NULL);
    switch (iter->ciditer_ids->cid_type) {
    case CID_BITMAP:
        // Keep iterating until we find a bit that is set
        while (iter->ciditer_i < bitmap_nbits(iter->ciditer_ids->cid_bitmap)) {
            if (bitmap_isset(iter->ciditer_ids->cid_bitmap, iter->ciditer_i)) {
                return true;
            }
            iter->ciditer_i++;
        }
        return false;
    case CID_ARRAY:
        // We still have a next element as long as we're less than the bound
        while (iter->ciditer_i < iter->ciditer_ids->cid_len) {
            return true;
        }
        return false;
    default: assert(0); return false;
    }
}

uint64_t
cid_iter_get(struct cid_iterator *iter)
{
    assert(iter != NULL);
    uint64_t id;
    switch (iter->ciditer_ids->cid_type) {
    case CID_BITMAP:
        id = iter->ciditer_i;
        break;
    case CID_ARRAY:
        id = iter->ciditer_ids->cid_ids[iter->ciditer_i];
        break;
    default: assert(0); break;
    }
    // move onto to the next entry
    iter->ciditer_i++;
    return id;
}

void
cid_iter_cleanup(struct cid_iterator *iter)
{
    assert(iter != NULL);
    iter->ciditer_ids = NULL;
    iter->ciditer_i = 0;
}
