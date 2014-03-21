#ifndef _AGGREGATE_H_
#define _AGGREGATE_H_

#include "../../common/include/operators.h"
#include "storage.h"

typedef int (*agg_func_t)(struct column_vals *);

int column_agg(struct column_vals *vals,
               agg_func_t f,
               struct column_vals **retvals);

int agg_min(struct column_vals *vals);
int agg_max(struct column_vals *vals);
int agg_sum(struct column_vals *vals);
int agg_count(struct column_vals *vals);
int agg_avg(struct column_vals *vals);

agg_func_t agg_func(enum agg_type atype);

#endif
