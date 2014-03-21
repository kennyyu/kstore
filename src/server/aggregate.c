#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include "include/aggregate.h"
#include "../common/include/operators.h"
#include "../common/include/try.h"

int
column_agg(struct column_vals *vals,
           agg_func_t f,
           struct column_vals **retvals)
{
    assert(vals != NULL);
    assert(f != NULL);
    assert(retvals != NULL);
    int agg = f(vals);

    int result;
    struct column_vals *aggval = NULL;
    TRYNULL(result, DBENOMEM, aggval, malloc(sizeof(struct column_vals)), done);
    TRYNULL(result, DBENOMEM, aggval->cval_vals, malloc(sizeof(int) * 1), cleanup_val);
    aggval->cval_len = 1;
    aggval->cval_vals[0] = agg;

    // success
    result = 0;
    *retvals = aggval;
    goto done;
  cleanup_val:
    free(aggval);
  done:
    return result;
}

int
agg_min(struct column_vals *vals)
{
    int min = INT_MAX;
    for (unsigned i = 0; i < vals->cval_len; i++) {
        int val = vals->cval_vals[i];
        min = (val < min) ? val : min;
    }
    return min;
}

int
agg_max(struct column_vals *vals)
{
    int max = INT_MIN;
    for (unsigned i = 0; i < vals->cval_len; i++) {
        int val = vals->cval_vals[i];
        max = (val > max) ? val : max;
    }
    return max;
}

int
agg_sum(struct column_vals *vals)
{
    int sum = 0;
    for (unsigned i = 0; i < vals->cval_len; i++) {
        sum += vals->cval_vals[i];
    }
    return sum;
}

int
agg_count(struct column_vals *vals)
{
    return vals->cval_len;
}

int
agg_avg(struct column_vals *vals)
{
    return agg_sum(vals) / agg_count(vals);
}

agg_func_t
agg_func(enum agg_type atype)
{
    switch (atype) {
    case AGG_MIN: return agg_min;
    case AGG_MAX: return agg_max;
    case AGG_SUM: return agg_sum;
    case AGG_AVG: return agg_avg;
    case AGG_COUNT: return agg_count;
    default: assert(0); return NULL;
    }
}
