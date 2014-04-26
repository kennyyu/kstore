#ifndef _JOIN_H_
#define _JOIN_H_

#include <db/common/operators.h>
#include <db/common/results.h>
#include <db/server/storage.h>

int column_join(enum join_type jtype,
                struct storage *storage,
                struct column_vals *inputL,
                struct column_vals *inputR,
                struct column_ids **retidsL,
                struct column_ids **retidsR);

#endif
