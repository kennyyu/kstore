#ifndef _CSV_H_
#define _CSV_H_

#include <stdio.h>
#include "array.h"

DECLARRAY_BYTYPE(intarray, int);

struct csv_result {
    char csv_colname[128];
    struct intarray *csv_vals;
};

DECLARRAY(csv_result);

struct csv_resultarray *csv_parse_header(int fd);
void csv_destroy(struct csv_resultarray *results);

#endif
