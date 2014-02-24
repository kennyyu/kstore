#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include "../common/include/array.h"
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
    char *line = malloc(sizeof(char) * (end - start + 1));
    if (line == NULL) {
        result = ENOMEM;
        goto done;
    }
    memcpy(line, &query[start], end - start);
    line[end] = '\0';
    result = stringarray_add(vec, line, NULL);
    if (result) {
        free(line);
        goto done;
    }
    result = 0;
    goto done;

  done:
    return result;
}

static
struct stringarray *
parse_split_lines(char *query)
{
    int result;
    struct stringarray *vec = stringarray_create();
    if (vec == NULL) {
        goto done;
    }
    unsigned start = 0;
    unsigned next = 0;
    char c;
    while ((c = query[next]) != '\0') {
        if (c == '\n') {
            result = add_string(vec, query, start, next);
            if (result) {
                goto cleanup_vec;
            }
            start = ++next;
        } else {
            next++;
        }
    }
    if (next == start) {
        goto done;
    }
    result = add_string(vec, query, start, next);
    if (result) {
        goto cleanup_vec;
    }
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

static
struct op *
parse_line(const char *line)
{
    struct op *op = malloc(sizeof(struct op));
    if (op == NULL) {
        goto done;
    }
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
    if (sscanf(line, "select(%[^,)])",
        (char *) &op->op_select.op_sel_col) == 1) {
        op->op_type = OP_SELECT_ALL;
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
    if (sscanf(line, "create(%[^,],%[^)])",
        (char *) &op->op_create.op_create_col,
        (char *) stype_buf) == 2) {
        op->op_type = OP_CREATE;
        op->op_create.op_create_stype = storage_type_from_string(stype_buf);
        goto done;
    }
    bzero(op, sizeof(struct op));
    if (sscanf(line, "load(%[^)])",
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
    goto cleanup_op;

  cleanup_op:
    free(op);
    op = NULL;
  done:
    return op;
}

void
parse_cleanup(struct oparray *ops)
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

    struct stringarray *lines = parse_split_lines(query);
    if (lines == NULL) {
        goto done;
    }
    ops = oparray_create();
    if (ops == NULL) {
        goto cleanup_lines;
    }
    for (unsigned i = 0; i < stringarray_num(lines); i++) {
        struct op *op = parse_line(stringarray_get(lines, i));
        if (op == NULL) {
            goto cleanup_oparray;
        }
        result = oparray_add(ops, op, NULL);
        if (result) {
            goto cleanup_oparray;
        }
    }
    goto cleanup_lines;

  cleanup_oparray:
    parse_cleanup(ops);
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
