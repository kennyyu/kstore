#ifndef _CSV_H_
#define _CSV_H_

#include <stdio.h>
#include <db/common/array.h>

DECLARRAY_BYTYPE(intarray, int);

struct csv_result {
    char csv_colname[128];
    struct intarray *csv_vals;
};

DECLARRAY(csv_result);

struct csv_resultarray *csv_parse(int fd);
void csv_destroy(struct csv_resultarray *results);

#endif
