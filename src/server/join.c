#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "../common/include/operators.h"
#include "../common/include/results.h"
#include "../common/include/try.h"
#include "include/storage.h"
#include "include/join.h"
#include "../common/include/dberror.h"

#define MIN(a,b) ((a) < (b)) ? (a) : (b)

// We'll use 2^NHASHBITS Buckets
#define NHASHBITS 16

static
int
column_join_loop(struct storage *storage,
                 struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    int result;

    unsigned VALS_PER_PAGE = PAGESIZE / sizeof(int);
    for (unsigned iL = 0; iL < inputL->cval_len; iL += VALS_PER_PAGE) {
        unsigned lmax = MIN(inputL->cval_len, iL + VALS_PER_PAGE);
        for (unsigned iR = 0; iR < inputR->cval_len; iR += VALS_PER_PAGE) {
            unsigned rmax = MIN(inputR->cval_len, iR + VALS_PER_PAGE);
            for (unsigned l = iL; l < lmax; l++) {
                for (unsigned r = iR; r < rmax; r++) {
                    if (inputL->cval_vals[l] == inputR->cval_vals[r]) {
                        TRY(result, idarray_add(retidsL->cid_array, (void *) l, NULL), done);
                        TRY(result, idarray_add(retidsR->cid_array, (void *) r, NULL), done);
                    }
                }
            }
        }
    }

    // success
    result = 0;
    goto done;
  done:
    return result;
}

struct idval_tuple {
    unsigned idval_id;
    int idval_val;
};

static
int
idval_tuple_compare(const void *a, const void *b)
{
    struct idval_tuple *ap = (struct idval_tuple *) a;
    struct idval_tuple *bp = (struct idval_tuple *) b;
    return ap->idval_val - bp->idval_val;
}

static
int
column_join_sort_repeats(
        struct idval_tuple *tuplesL,
        struct idval_tuple *tuplesR,
        struct column_ids *retidsL,
        struct column_ids *retidsR,
        unsigned nl, unsigned nr, unsigned l, unsigned r,
        unsigned *newl, unsigned *newR)
{
    int result;
    // Do one scan to figure out how long the streak is on the right
    unsigned streak = r;
    do {
        TRY(result, idarray_add(retidsL->cid_array, (void *) tuplesL[l].idval_id, NULL), done);
        TRY(result, idarray_add(retidsR->cid_array, (void *) tuplesR[streak].idval_id, NULL), done);
        streak++;
    } while (streak < nr && tuplesL[l].idval_val == tuplesR[streak].idval_val);

    // Now figure out the streak on the left
    unsigned sl = l + 1;
    for (/* empty */; sl < nl; sl++) {
        if (tuplesL[sl].idval_val == tuplesR[r].idval_val) {
            for (unsigned sr = r; sr < streak; sr++) {
                TRY(result, idarray_add(retidsL->cid_array, (void *) tuplesL[sl].idval_id, NULL), done);
                TRY(result, idarray_add(retidsR->cid_array, (void *) tuplesR[sr].idval_id, NULL), done);
            }
        } else {
            break;
        }
    }

    *newl = sl;
    *newR = streak;
    result = 0;
    goto done;
  done:
    return result;
}

static
int
column_join_sort(struct storage *storage,
                 struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    int result;
    struct idval_tuple *tuplesL = NULL, *tuplesR = NULL;
    TRYNULL(result, DBENOMEM, tuplesL,
            malloc(sizeof(struct idval_tuple) * inputL->cval_len), done);
    TRYNULL(result, DBENOMEM, tuplesR,
            malloc(sizeof(struct idval_tuple) * inputR->cval_len), cleanup_tuplesL);

    // sort our arrays
    for (unsigned i = 0; i < inputL->cval_len; i++) {
        tuplesL[i].idval_id = inputL->cval_ids[i];
        tuplesL[i].idval_val = inputL->cval_vals[i];
    }
    qsort(tuplesL, inputL->cval_len, sizeof(struct idval_tuple), idval_tuple_compare);
    for (unsigned i = 0; i < inputR->cval_len; i++) {
        tuplesR[i].idval_id = inputR->cval_ids[i];
        tuplesR[i].idval_val = inputR->cval_vals[i];
    }
    qsort(tuplesR, inputR->cval_len, sizeof(struct idval_tuple), idval_tuple_compare);

    unsigned l = 0, r = 0;
    while (l < inputL->cval_len && r < inputR->cval_len) {
        int lval = tuplesL[l].idval_val;
        int rval = tuplesR[r].idval_val;
        if (lval < rval) {
            l++;
        } else if (lval > rval) {
            r++;
        } else {
            unsigned newl, newr;
            TRY(result, column_join_sort_repeats(tuplesL, tuplesR, retidsL, retidsR,
                    inputL->cval_len, inputR->cval_len, l, r, &newl, &newr),
                cleanup_tuplesR);
            l = newl;
            r = newr;
        }
    }

    // success
    result = 0;
    goto cleanup_tuplesR;
  cleanup_tuplesR:
    free(tuplesR);
  cleanup_tuplesL:
    free(tuplesL);
  done:
    return result;
}

static
int
column_join_tree(struct storage *storage,
                 struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    assert(strcmp(inputR->cval_col, "") != 0);
    int result;

    // Make sure a Btree exists on the right column
    struct column *col = NULL;
    TRY(result, column_open(storage, inputR->cval_col, &col), done);
    if (col->col_disk.cd_stype != STORAGE_BTREE) {
        result = DBENOTREE;
        DBLOG(result);
        goto cleanup_col;
    }

    // For each value in the left column, scan the btree for the value
    struct column_ids *cids = NULL;
    for (unsigned i = 0; i < inputL->cval_len; i++) {
        struct op op;
        bzero(&op, sizeof(struct op));
        op.op_type = OP_SELECT_VALUE;
        op.op_select.op_sel_value = inputL->cval_vals[i];
        strcpy(op.op_select.op_sel_col, inputR->cval_col);

        TRYNULL(result, DBECOLSELECT, cids, column_select(col, &op), cleanup_cids);
        unsigned leftid = inputL->cval_ids[i];
        struct cid_iterator iter;
        cid_iter_init(&iter, cids);
        while (cid_iter_has_next(&iter)) {
            unsigned rightid = cid_iter_get(&iter);
            TRY(result, idarray_add(retidsL->cid_array, (void *) leftid, NULL), cleanup_cids);
            TRY(result, idarray_add(retidsR->cid_array, (void *) rightid, NULL), cleanup_cids);
        }
        cid_iter_cleanup(&iter);
        column_ids_destroy(cids);
    }

    result = 0;
    goto cleanup_col;
  cleanup_cids:
    if (cids != NULL) {
        column_ids_destroy(cids);
    }
  cleanup_col:
    column_close(col);
  done:
    return result;
}

/*
 * Hash function taken from here:
 * http://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
 */
static
unsigned int hash(unsigned int x, unsigned nbits)
{
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x);
    unsigned nbitmask = (1 << nbits) - 1;
    return x & nbitmask;
}

struct hashentry {
    int he_val;
    unsigned he_id;
};

static
int
column_join_hash(struct storage *storage,
                 struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    int result;

    // Use static hashing.
    // First pass: compute counts for each bucket on right side
    unsigned nbuckets = 1 << NHASHBITS;
    unsigned counts[nbuckets];
    bzero(counts, sizeof(unsigned) * nbuckets);
    for (unsigned i = 0; i < inputR->cval_len; i++) {
        unsigned bucket = hash(inputR->cval_vals[i], NHASHBITS);
        assert(bucket < nbuckets);
        counts[bucket]++;
    }

    // Maintain the offsets for where the buckets start.
    unsigned offsets[nbuckets];
    bzero(offsets, sizeof(unsigned) * nbuckets);
    unsigned next = 0;
    for (unsigned i = 0; i < nbuckets; i++) {
        offsets[i] = next;
        next += counts[i];
    }
    assert(next == inputR->cval_len);

    // Maintain current offsets for where to insert the next entry
    // into bucket.
    unsigned insertions[nbuckets];
    memcpy(insertions, offsets, sizeof(unsigned) * nbuckets);

    // Second pass: actually create our hash table
    struct hashentry *hashtable;
    TRYNULL(result, DBENOMEM, hashtable,
            malloc(sizeof(struct hashentry) * inputR->cval_len), done);
    bzero(hashtable, sizeof(struct hashentry) * inputR->cval_len);
    for (unsigned i = 0; i < inputR->cval_len; i++) {
        int val = inputR->cval_vals[i];
        unsigned bucket = hash(val, NHASHBITS);
        assert(bucket < nbuckets);
        unsigned insertioni = insertions[bucket]++;
        assert(insertioni < inputR->cval_len);
        hashtable[insertioni].he_val = val;
        hashtable[insertioni].he_id = inputR->cval_ids[i];
    }

    // For each entry in the left, probe the hashtable
    for (unsigned i = 0; i < inputL->cval_len; i++) {
        int val = inputL->cval_vals[i];
        unsigned bucket = hash(val, NHASHBITS);
        unsigned offset = offsets[bucket];
        // Must check the entire bucket for possible duplicate values
        for (unsigned bucketi = 0; bucketi < counts[bucket]; bucketi++) {
            unsigned ix = offset + bucketi;
            if (val == hashtable[ix].he_val) {
                TRY(result, idarray_add(retidsL->cid_array, (void *) inputL->cval_ids[i], NULL), cleanup_malloc);
                TRY(result, idarray_add(retidsR->cid_array, (void *) hashtable[ix].he_id, NULL), cleanup_malloc);
            }
        }
    }

    // success
    result = 0;
    goto cleanup_malloc;
  cleanup_malloc:
    free(hashtable);
  done:
    return result;
}

int
column_join(enum join_type jtype,
            struct storage *storage,
            struct column_vals *inputL,
            struct column_vals *inputR,
            struct column_ids **retidsL,
            struct column_ids **retidsR)
{
    assert(storage != NULL);
    assert(inputL != NULL);
    assert(inputR != NULL);
    assert(retidsL != NULL);
    assert(retidsR != NULL);

    if (jtype != JOIN_TREE && inputL->cval_len < inputR->cval_len) {
        return column_join(jtype, storage, inputR, inputL, retidsR, retidsL);
    }

    int result;
    struct column_ids *idsL, *idsR;
    TRYNULL(result, DBENOMEM, idsL, malloc(sizeof(struct column_ids)), done);
    idsL->cid_type = CID_ARRAY;
    TRYNULL(result, DBENOMEM, idsL->cid_array, idarray_create(), cleanup_idsL);
    TRYNULL(result, DBENOMEM, idsR, malloc(sizeof(struct column_ids)), cleanup_idsLarray);
    idsR->cid_type = CID_ARRAY;
    TRYNULL(result, DBENOMEM, idsR->cid_array, idarray_create(), cleanup_idsR);

    switch (jtype) {
    case JOIN_LOOP:
        TRY(result, column_join_loop(storage, inputL, inputR, idsL, idsR), cleanup_idsRarray);
        break;
    case JOIN_SORT:
        TRY(result, column_join_sort(storage, inputL, inputR, idsL, idsR), cleanup_idsRarray);
        break;
    case JOIN_TREE:
        TRY(result, column_join_tree(storage, inputL, inputR, idsL, idsR), cleanup_idsRarray);
        break;
    case JOIN_HASH:
        TRY(result, column_join_hash(storage, inputL, inputR, idsL, idsR), cleanup_idsRarray);
        break;
    default:
        assert(0);
        break;
    }
    assert(idarray_num(idsL->cid_array) == idarray_num(idsR->cid_array));

    result = 0;
    *retidsL = idsL;
    *retidsR = idsR;
    goto done;

  cleanup_idsRarray:
    idsR->cid_array->arr.num = 0;
    idarray_destroy(idsR->cid_array);
  cleanup_idsR:
    free(idsR);
  cleanup_idsLarray:
    idsL->cid_array->arr.num = 0;
    idarray_destroy(idsL->cid_array);
  cleanup_idsL:
    free(idsL);
  done:
    return result;
}
