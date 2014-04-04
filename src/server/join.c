#include <stdlib.h>
#include <stdio.h>

#include "../common/include/operators.h"
#include "../common/include/results.h"
#include "../common/include/try.h"
#include "include/storage.h"
#include "include/join.h"

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

static
int
column_join_sort(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    return column_join_loop(inputL, inputR, retidsL, retidsR);
}

static
int
column_join_tree(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    return column_join_loop(inputL, inputR, retidsL, retidsR);
}

static
int
column_join_hash(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids *retidsL,
                 struct column_ids *retidsR)
{
    return column_join_loop(inputL, inputR, retidsL, retidsR);
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
