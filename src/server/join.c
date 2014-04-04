#include <stdlib.h>
#include <stdio.h>

#include "../common/include/operators.h"
#include "../common/include/results.h"
#include "include/storage.h"
#include "include/join.h"

static
int
column_join_loop(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids **retidsL,
                 struct column_ids **retidsR)
{
    return 0;
}

static
int
column_join_sort(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids **retidsL,
                 struct column_ids **retidsR)
{
    return 0;
}

static
int
column_join_tree(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids **retidsL,
                 struct column_ids **retidsR)
{
    return 0;
}

static
int
column_join_hash(struct column_vals *inputL,
                 struct column_vals *inputR,
                 struct column_ids **retidsL,
                 struct column_ids **retidsR)
{
    return 0;
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

    switch (jtype) {
    case JOIN_LOOP: return column_join_loop(inputL, inputR, retidsL, retidsR);
    case JOIN_SORT: return column_join_sort(inputL, inputR, retidsL, retidsR);
    case JOIN_TREE: return column_join_tree(inputL, inputR, retidsL, retidsR);
    case JOIN_HASH: return column_join_hash(inputL, inputR, retidsL, retidsR);
    default: assert(0); return 0;
    }
}
