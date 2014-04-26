#ifndef _TRY_H_
#define _TRY_H_

#include <stddef.h>
#include <db/common/dberror.h>

#define TRY(result, expr, cleanup) \
    (result) = (expr); \
    if ((result)) { \
        DBLOG((result)); \
        goto cleanup; \
    }

#define TRYNULL(result, err, var, expr, cleanup) \
    (var) = (expr); \
    if ((var) == NULL) { \
        (result) = (err); \
        DBLOG((result)); \
        goto cleanup; \
    }

#endif
