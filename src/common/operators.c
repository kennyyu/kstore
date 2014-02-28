#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include "include/operators.h"

char *op_type_string(enum op_type op_type) {
    switch (op_type) {
    case OP_SELECT_ALL_ASSIGN: return "OP_SELECT_ALL_ASSIGN";
    case OP_SELECT_RANGE_ASSIGN: return "OP_SELECT_RANGE_ASSIGN";
    case OP_SELECT_VALUE_ASSIGN: return "OP_SELECT_VALUE_ASSIGN";
    case OP_SELECT_ALL: return "OP_SELECT_ALL";
    case OP_SELECT_RANGE: return "OP_SELECT_RANGE";
    case OP_SELECT_VALUE: return "OP_SELECT_VALUE";
    case OP_FETCH: return "OP_FETCH";
    case OP_CREATE: return "OP_CREATE";
    case OP_LOAD: return "OP_LOAD";
    case OP_INSERT: return "OP_INSERT";
    default: assert(0); return NULL;
    }
}
char *op_string(struct op *op) {
    char *stype;
    char *buf = malloc(sizeof(char) * 2048);
    if (buf == NULL) {
        goto done;
    }
    bzero(buf, 2048);

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
