#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include "include/try.h"
#include "include/dberror.h"
#include "include/operators.h"

char *op_string(struct op *op) {
    int result;
    char *stype;
    char *buf;
    TRYNULL(result, DBENOMEM, buf, malloc(sizeof(char) * TUPLELEN), done);
    bzero(buf, TUPLELEN);

    switch (op->op_type) {
    case OP_SELECT_ALL_ASSIGN:
        sprintf(buf, "%s=select(%s)",
                op->op_select.op_sel_var,
                op->op_select.op_sel_col);
        break;
    case OP_SELECT_RANGE_ASSIGN:
        sprintf(buf, "%s=select(%s,%u,%u)",
                op->op_select.op_sel_var,
                op->op_select.op_sel_col,
                op->op_select.op_sel_low,
                op->op_select.op_sel_high);
        break;
    case OP_SELECT_VALUE_ASSIGN:
        sprintf(buf, "%s=select(%s,%u)",
                op->op_select.op_sel_var,
                op->op_select.op_sel_col,
                op->op_select.op_sel_value);
        break;
    case OP_SELECT_ALL:
        sprintf(buf, "select(%s)",
                op->op_select.op_sel_col);
        break;
    case OP_SELECT_RANGE:
        sprintf(buf, "select(%s,%u,%u)",
                op->op_select.op_sel_col,
                op->op_select.op_sel_low,
                op->op_select.op_sel_high);
        break;
    case OP_SELECT_VALUE:
        sprintf(buf, "select(%s,%u)",
                op->op_select.op_sel_col,
                op->op_select.op_sel_value);
        break;
    case OP_FETCH:
        sprintf(buf, "fetch(%s,%s)",
                op->op_fetch.op_fetch_col,
                op->op_fetch.op_fetch_pos);
        break;
    case OP_FETCH_ASSIGN:
        sprintf(buf, "%s=fetch(%s,%s)",
                op->op_fetch.op_fetch_var,
                op->op_fetch.op_fetch_col,
                op->op_fetch.op_fetch_pos);
        break;
    case OP_CREATE:
        stype = storage_type_string(op->op_create.op_create_stype);
        sprintf(buf, "create(%s,\"%s\")",
                op->op_create.op_create_col,
                stype);
        break;
    case OP_LOAD:
        sprintf(buf, "load(\"%s\")",
                op->op_load.op_load_file);
        break;
    case OP_INSERT:
        sprintf(buf, "insert(%s,%u)",
                op->op_insert.op_insert_col,
                op->op_insert.op_insert_val);
        break;
    case OP_TUPLE:
        sprintf(buf, "tuple(%s)",
                op->op_tuple.op_tuple_vars);
        break;
    case OP_AGG:
        if (op->op_agg.op_agg_assign) {
            sprintf(buf, "%s=%s(%s)",
                    op->op_agg.op_agg_var,
                    agg_type_string(op->op_agg.op_agg_atype),
                    op->op_agg.op_agg_col);
        } else {
            sprintf(buf, "%s(%s)",
                    agg_type_string(op->op_agg.op_agg_atype),
                    op->op_agg.op_agg_col);
        }
        break;
    case OP_MATH:
        if (op->op_math.op_math_assign) {
            sprintf(buf, "%s=%s(%s,%s)",
                    op->op_math.op_math_var,
                    math_type_string(op->op_math.op_math_mtype),
                    op->op_math.op_math_col1,
                    op->op_math.op_math_col2);
        } else {
            sprintf(buf, "%s(%s,%s)",
                    math_type_string(op->op_math.op_math_mtype),
                    op->op_math.op_math_col1,
                    op->op_math.op_math_col2);
        }
        break;
    case OP_PRINT:
        sprintf(buf, "print(%s)", op->op_print.op_print_var);
        break;
    default: assert(0); return NULL;
    }

  done:
    return buf;
}

enum storage_type storage_type_from_string(char *s) {
    if (strcmp(s, "unsorted") == 0) {
        return STORAGE_UNSORTED;
    }
    if (strcmp(s, "sorted") == 0) {
        return STORAGE_SORTED;
    }
    if (strcmp(s, "b+tree") == 0) {
        return STORAGE_BTREE;
    }
    assert(0);
    return -1;
}

char *storage_type_string(enum storage_type stype) {
    switch (stype) {
    case STORAGE_UNSORTED: return "unsorted";
    case STORAGE_SORTED: return "sorted";
    case STORAGE_BTREE: return "b+tree";
    default: assert(0); return NULL;
    }
}

char *math_type_string(enum math_type mtype) {
    switch (mtype) {
    case MATH_ADD: return "add";
    case MATH_SUB: return "sub";
    case MATH_MUL: return "mul";
    case MATH_DIV: return "div";
    default: assert(0); return NULL;
    }
}

char *agg_type_string(enum agg_type atype) {
    switch (atype) {
    case AGG_MIN: return "min";
    case AGG_MAX: return "max";
    case AGG_SUM: return "sum";
    case AGG_AVG: return "avg";
    case AGG_COUNT: return "count";
    default: assert(0); return NULL;
    }
}
