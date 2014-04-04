#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include "include/array.h"
#include "include/try.h"
#include "include/operators.h"
#include "include/parser.h"

DEFARRAY(op, /*no inline */);

DECLARRAY_BYTYPE(stringarray, char);
DEFARRAY_BYTYPE(stringarray, char, /* no inline */);

static
int
add_string(struct stringarray *vec, char *query, unsigned start, unsigned end)
{
    int result = 0;
    // create space for the chars + 1 for '\0'
    char *line;
    TRYNULL(result, DBENOMEM, line, malloc(sizeof(char) * (end - start + 1)), done);
    memcpy(line, &query[start], end - start);
    line[end] = '\0';
    TRY(result, stringarray_add(vec, line, NULL), cleanup_malloc);
    result = 0;
    goto done;

  cleanup_malloc:
    free(line);
  done:
    return result;
}

static
struct stringarray *
parse_split_lines(char *query)
{
    int result;
    struct stringarray *vec = NULL;
    TRYNULL(result, DBENOMEM, vec, stringarray_create(), done);
    unsigned start = 0;
    unsigned next = 0;
    char c;
    while ((c = query[next]) != '\0') {
        if (c == '\n') {
            TRY(result, add_string(vec, query, start, next), cleanup_vec);
            start = ++next;
        } else {
            next++;
        }
    }
    if (next == start) {
        goto done;
    }
    TRY(result, add_string(vec, query, start, next), cleanup_vec);
    goto done;

  cleanup_vec:
    while (stringarray_num(vec) != 0) {
        char *line = stringarray_get(vec, 0);
        stringarray_remove(vec, 0);
        free(line);
    }
    stringarray_destroy(vec);
  done:
    return vec;
}

struct op *
parse_line(char *line)
{
    int result;
    struct op *op;
    TRYNULL(result, DBENOMEM, op, malloc(sizeof(struct op)), done);
    char stype_buf[16];

    // Because scanf is greedy, we need to put the select
    // in decreasing number of arguments.
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=select(%[^,],%u,%u)",
        (char *) &op->op_select.op_sel_var,
        (char *) &op->op_select.op_sel_col,
        &op->op_select.op_sel_low,
        &op->op_select.op_sel_high) == 4) {
        op->op_type = OP_SELECT_RANGE_ASSIGN;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=select(%[^,],%u)",
        (char *) &op->op_select.op_sel_var,
        (char *) &op->op_select.op_sel_col,
        &op->op_select.op_sel_value) == 3) {
        op->op_type = OP_SELECT_VALUE_ASSIGN;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=select(%[^,)])",
        (char *) &op->op_select.op_sel_var,
        (char *) &op->op_select.op_sel_col) == 2) {
        op->op_type = OP_SELECT_ALL_ASSIGN;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "select(%[^,],%u,%u)",
        (char *) &op->op_select.op_sel_col,
        &op->op_select.op_sel_low,
        &op->op_select.op_sel_high) == 3) {
        op->op_type = OP_SELECT_RANGE;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "select(%[^,],%u)",
        (char *) &op->op_select.op_sel_col,
        &op->op_select.op_sel_value) == 2) {
        op->op_type = OP_SELECT_VALUE;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "select(%[^,)])",
        (char *) &op->op_select.op_sel_col) == 1) {
        op->op_type = OP_SELECT_ALL;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=fetch(%[^,],%[^)])",
        (char *) &op->op_fetch.op_fetch_var,
        (char *) &op->op_fetch.op_fetch_col,
        (char *) &op->op_fetch.op_fetch_pos) == 3) {
        op->op_type = OP_FETCH_ASSIGN;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "fetch(%[^,],%[^)])",
        (char *) &op->op_fetch.op_fetch_col,
        (char *) &op->op_fetch.op_fetch_pos) == 2) {
        op->op_type = OP_FETCH;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "create(%[^,],\"%[^)\"])",
        (char *) &op->op_create.op_create_col,
        (char *) stype_buf) == 2) {
        op->op_type = OP_CREATE;
        op->op_create.op_create_stype = storage_type_from_string(stype_buf);
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "load(\"%[^)\"])",
        (char *) &op->op_load.op_load_file) == 1) {
        op->op_type = OP_LOAD;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "insert(%[^,],%u)",
        (char *) &op->op_insert.op_insert_col,
        &op->op_insert.op_insert_val) == 2) {
        op->op_type = OP_INSERT;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "tuple(%[^)])",
        (char *) &op->op_tuple.op_tuple_vars) == 1) {
        op->op_type = OP_TUPLE;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=add(%[^,],%[^)])",
        (char *) &op->op_math.op_math_var,
        (char *) &op->op_math.op_math_col1,
        (char *) &op->op_math.op_math_col2) == 3) {
        op->op_type = OP_MATH;
        op->op_math.op_math_mtype = MATH_ADD;
        op->op_math.op_math_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "add(%[^,],%[^)])",
        (char *) &op->op_math.op_math_col1,
        (char *) &op->op_math.op_math_col2) == 2) {
        op->op_type = OP_MATH;
        op->op_math.op_math_mtype = MATH_ADD;
        op->op_math.op_math_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=sub(%[^,],%[^)])",
        (char *) &op->op_math.op_math_var,
        (char *) &op->op_math.op_math_col1,
        (char *) &op->op_math.op_math_col2) == 3) {
        op->op_type = OP_MATH;
        op->op_math.op_math_mtype = MATH_SUB;
        op->op_math.op_math_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "sub(%[^,],%[^)])",
        (char *) &op->op_math.op_math_col1,
        (char *) &op->op_math.op_math_col2) == 2) {
        op->op_type = OP_MATH;
        op->op_math.op_math_mtype = MATH_SUB;
        op->op_math.op_math_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=mul(%[^,],%[^)])",
        (char *) &op->op_math.op_math_var,
        (char *) &op->op_math.op_math_col1,
        (char *) &op->op_math.op_math_col2) == 3) {
        op->op_type = OP_MATH;
        op->op_math.op_math_mtype = MATH_MUL;
        op->op_math.op_math_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "mul(%[^,],%[^)])",
        (char *) &op->op_math.op_math_col1,
        (char *) &op->op_math.op_math_col2) == 2) {
        op->op_type = OP_MATH;
        op->op_math.op_math_mtype = MATH_MUL;
        op->op_math.op_math_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=div(%[^,],%[^)])",
        (char *) &op->op_math.op_math_var,
        (char *) &op->op_math.op_math_col1,
        (char *) &op->op_math.op_math_col2) == 3) {
        op->op_type = OP_MATH;
        op->op_math.op_math_mtype = MATH_DIV;
        op->op_math.op_math_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "div(%[^,],%[^)])",
        (char *) &op->op_math.op_math_col1,
        (char *) &op->op_math.op_math_col2) == 2) {
        op->op_type = OP_MATH;
        op->op_math.op_math_mtype = MATH_DIV;
        op->op_math.op_math_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=min(%[^,)])",
        (char *) &op->op_agg.op_agg_var,
        (char *) &op->op_agg.op_agg_col) == 2) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_MIN;
        op->op_agg.op_agg_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "min(%[^,)])",
        (char *) &op->op_agg.op_agg_col) == 1) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_MIN;
        op->op_agg.op_agg_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=max(%[^,)])",
        (char *) &op->op_agg.op_agg_var,
        (char *) &op->op_agg.op_agg_col) == 2) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_MAX;
        op->op_agg.op_agg_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "max(%[^,)])",
        (char *) &op->op_agg.op_agg_col) == 1) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_MAX;
        op->op_agg.op_agg_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=avg(%[^,)])",
        (char *) &op->op_agg.op_agg_var,
        (char *) &op->op_agg.op_agg_col) == 2) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_AVG;
        op->op_agg.op_agg_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "avg(%[^,)])",
        (char *) &op->op_agg.op_agg_col) == 1) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_AVG;
        op->op_agg.op_agg_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=sum(%[^,)])",
        (char *) &op->op_agg.op_agg_var,
        (char *) &op->op_agg.op_agg_col) == 2) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_SUM;
        op->op_agg.op_agg_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "sum(%[^,)])",
        (char *) &op->op_agg.op_agg_col) == 1) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_SUM;
        op->op_agg.op_agg_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=]=count(%[^,)])",
        (char *) &op->op_agg.op_agg_var,
        (char *) &op->op_agg.op_agg_col) == 2) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_COUNT;
        op->op_agg.op_agg_assign = true;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "count(%[^,)])",
        (char *) &op->op_agg.op_agg_col) == 1) {
        op->op_type = OP_AGG;
        op->op_agg.op_agg_atype = AGG_COUNT;
        op->op_agg.op_agg_assign = false;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "print(%[^,)])",
        (char *) &op->op_print.op_print_var) == 1) {
        op->op_type = OP_PRINT;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=,],%[^=,]=loopjoin(%[^,],%[^)])",
        (char *) &op->op_join.op_join_varL,
        (char *) &op->op_join.op_join_varR,
        (char *) &op->op_join.op_join_inputL,
        (char *) &op->op_join.op_join_inputR) == 4) {
        op->op_type = OP_JOIN;
        op->op_join.op_join_jtype = JOIN_LOOP;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=,],%[^=,]=sortjoin(%[^,],%[^)])",
        (char *) &op->op_join.op_join_varL,
        (char *) &op->op_join.op_join_varR,
        (char *) &op->op_join.op_join_inputL,
        (char *) &op->op_join.op_join_inputR) == 4) {
        op->op_type = OP_JOIN;
        op->op_join.op_join_jtype = JOIN_SORT;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=,],%[^=,]=treejoin(%[^,],%[^)])",
        (char *) &op->op_join.op_join_varL,
        (char *) &op->op_join.op_join_varR,
        (char *) &op->op_join.op_join_inputL,
        (char *) &op->op_join.op_join_inputR) == 4) {
        op->op_type = OP_JOIN;
        op->op_join.op_join_jtype = JOIN_TREE;
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "%[^=,],%[^=,]=hashjoin(%[^,],%[^)])",
        (char *) &op->op_join.op_join_varL,
        (char *) &op->op_join.op_join_varR,
        (char *) &op->op_join.op_join_inputL,
        (char *) &op->op_join.op_join_inputR) == 4) {
        op->op_type = OP_JOIN;
        op->op_join.op_join_jtype = JOIN_HASH;
        goto done;
    }
    goto cleanup_op;

  cleanup_op:
    free(op);
    op = NULL;
  done:
    return op;
}

void
parse_cleanup(struct op *op)
{
    assert(op != NULL);
    free(op);
}

void
parse_cleanup_ops(struct oparray *ops)
{
    assert(ops != NULL);
    while (oparray_num(ops) != 0) {
        struct op *op = oparray_get(ops, 0);
        oparray_remove(ops, 0);
        free(op);
    }
    oparray_destroy(ops);
}

struct oparray *
parse_query(char *query)
{
    int result;
    struct oparray *ops = NULL;
    struct stringarray *lines = NULL;

    TRYNULL(result, DBENOMEM, lines, parse_split_lines(query), done);
    TRYNULL(result, DBENOMEM, ops, oparray_create(), cleanup_lines);
    for (unsigned i = 0; i < stringarray_num(lines); i++) {
        struct op *op;
        TRYNULL(result, DBEPARSE, op,
                parse_line(stringarray_get(lines, i)), cleanup_oparray);
        TRY(result, oparray_add(ops, op, NULL), cleanup_oparray);
    }
    goto cleanup_lines;

  cleanup_oparray:
    parse_cleanup_ops(ops);
    ops = NULL;
  cleanup_lines:
    while (stringarray_num(lines) != 0) {
        char *line = stringarray_get(lines, 0);
        stringarray_remove(lines, 0);
        free(line);
    }
    stringarray_destroy(lines);
  done:
    return ops;
}
