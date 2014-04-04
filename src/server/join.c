#include <stdlib.h>
#include <stdio.h>

#include "../common/include/operators.h"
#include "../common/include/results.h"
#include "../common/include/try.h"
#include "include/storage.h"
#include "include/join.h"
#include "../common/include/dberror.h"

#define MIN(a,b) ((a) < (b)) ? (a) : (b)

static
int
column_join_loop(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    int result;

    unsigned VALS_PER_PAGE = PAGESIZE / sizeof(int);
    for (unsigned iL = 0; iL < inputL->cval_len; iL += VALS_PER_PAGE) {
        for (unsigned iR = 0; iR < inputR->cval_len; iR += VALS_PER_PAGE) {
            unsigned lmax = iL + MIN(inputL->cval_len - iL, VALS_PER_PAGE);
            unsigned rmax = iR + MIN(inputR->cval_len - iR, VALS_PER_PAGE);
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
column_join_sort(struct column_vals *inputL,
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

    for (unsigned l = 0; l < inputL->cval_len; /* empty */) {
        for (unsigned r = 0; r < inputR->cval_len; /* empty */) {
            if (tuplesL[l].idval_val == tuplesR[r].idval_val) {
                unsigned newl, newr;
                TRY(result, column_join_sort_repeats(tuplesL, tuplesR, retidsL, retidsR,
                        inputL->cval_len, inputR->cval_len, l, r, &newl, &newr),
                    cleanup_tuplesR);
                l = newl;
                r = newr;
                goto restart;
            }
            r++;
        }
        l++;
      restart:
        continue;
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
column_join_tree(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    return column_join_sort(inputL, inputR, retidsL, retidsR);
}

static
int
column_join_hash(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    return column_join_sort(inputL, inputR, retidsL, retidsR);
}

int
column_join(enum join_type jtype,
            struct column_vals *inputL,
            struct column_vals *inputR,
            struct column_ids **retidsL,
            struct column_ids **retidsR)
{
    assert(inputL != NULL);
    assert(inputR != NULL);
    assert(retidsL != NULL);
    assert(retidsR != NULL);

    if (inputL->cval_len < inputR->cval_len) {
        return column_join(jtype, inputR, inputL, retidsR, retidsL);
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
        TRY(result, column_join_loop(inputL, inputR, idsL, idsR), cleanup_idsRarray);
        break;
    case JOIN_SORT:
        TRY(result, column_join_sort(inputL, inputR, idsL, idsR), cleanup_idsRarray);
        break;
    case JOIN_TREE:
        TRY(result, column_join_tree(inputL, inputR, idsL, idsR), cleanup_idsRarray);
        break;
    case JOIN_HASH:
        TRY(result, column_join_hash(inputL, inputR, idsL, idsR), cleanup_idsRarray);
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
