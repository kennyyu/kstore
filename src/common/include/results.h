#ifndef _RESULTS_H_
#define _RESULTS_H_

#include <stdint.h>
#include <stdbool.h>
#include "bitmap.h"

enum column_ids_type {
    CID_BITMAP,
    CID_ARRAY,
};

struct column_ids {
    enum column_ids_type cid_type;
    union {
        struct bitmap *cid_bitmap;
        struct {
            unsigned *cid_ids;
            unsigned cid_len;
        };
    };
};

struct cid_iterator {
    struct column_ids *ciditer_ids;
    unsigned ciditer_i;
};

void cid_iter_init(struct cid_iterator *iter, struct column_ids *cids);
bool cid_iter_has_next(struct cid_iterator *iter);
uint64_t cid_iter_get(struct cid_iterator *iter);
void cid_iter_cleanup(struct cid_iterator *iter);

struct column_vals {
    int *cval_vals;
    unsigned cval_len;
};

void column_ids_destroy(struct column_ids *cids);
void column_vals_destroy(struct column_vals *vals);

#endif
