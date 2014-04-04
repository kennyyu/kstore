#ifndef _JOIN_H_
#define _JOIN_H_

#include "../../common/include/operators.h"
#include "../../common/include/results.h"
#include "storage.h"

int column_join(enum join_type jtype,
                struct column_vals *inputL,
                struct column_vals *inputR,
                struct column_ids **retidsL,
                struct column_ids **retidsR);

#endif
