#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "../src/server/include/parser.h"

void testselectall(void) {
    char *query = "select(C)";
    struct oparray *ops = parse_query(query);
    assert(ops != NULL);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_SELECT_ALL);
    assert(strcmp(op->op_select.op_sel_col,"C") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testselectrange(void) {
    char *query = "select(C,14,20)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_SELECT_RANGE);
    assert(op->op_select.op_sel_low == 14);
    assert(op->op_select.op_sel_high == 20);
    assert(strcmp(op->op_select.op_sel_col,"C") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testselectvalue(void) {
    char *query = "select(C,14)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_SELECT_VALUE);
    assert(op->op_select.op_sel_value == 14);
    assert(strcmp(op->op_select.op_sel_col,"C") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testselectallassign(void) {
    char *query = "foo=select(C)";
    struct oparray *ops = parse_query(query);
    assert(ops != NULL);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_SELECT_ALL_ASSIGN);
    assert(strcmp(op->op_select.op_sel_var,"foo") == 0);
    assert(strcmp(op->op_select.op_sel_col,"C") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testselectrangeassign(void) {
    char *query = "foo=select(C,14,20)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_SELECT_RANGE_ASSIGN);
    assert(op->op_select.op_sel_low == 14);
    assert(op->op_select.op_sel_high == 20);
    assert(strcmp(op->op_select.op_sel_var,"foo") == 0);
    assert(strcmp(op->op_select.op_sel_col,"C") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testselectvalueassign(void) {
    char *query = "foo=select(C,14)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_SELECT_VALUE_ASSIGN);
    assert(op->op_select.op_sel_value == 14);
    assert(strcmp(op->op_select.op_sel_var,"foo") == 0);
    assert(strcmp(op->op_select.op_sel_col,"C") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testfetch(void) {
    char *query = "fetch(C,foo)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_FETCH);
    assert(strcmp(op->op_fetch.op_fetch_pos,"foo") == 0);
    assert(strcmp(op->op_fetch.op_fetch_col,"C") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testcreatesorted(void) {
    char *query = "create(C,sorted)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_CREATE);
    assert(strcmp(op->op_create.op_create_col,"C") == 0);
    assert(op->op_create.op_create_stype == STORAGE_SORTED);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testcreateunsorted(void) {
    char *query = "create(C,unsorted)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_CREATE);
    assert(strcmp(op->op_create.op_create_col,"C") == 0);
    assert(op->op_create.op_create_stype == STORAGE_UNSORTED);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testcreatebtree(void) {
    char *query = "create(C,b+tree)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_CREATE);
    assert(strcmp(op->op_create.op_create_col,"C") == 0);
    assert(op->op_create.op_create_stype == STORAGE_BTREE);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testload(void) {
    char *query = "load(/home/jharvard/foo.csv)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_LOAD);
    assert(strcmp(op->op_load.op_load_file, "/home/jharvard/foo.csv") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testinsert(void) {
    char *query = "insert(C,5)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_INSERT);
    assert(strcmp(op->op_insert.op_insert_col,"C") == 0);
    assert(op->op_insert.op_insert_val == 5);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup(ops);
}

void testbad(void) {
    char *query = "";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 0);
    parse_cleanup(ops);
}

void testmultiple(void) {
    char *s;
    struct op *op;
    char *query = "inter=select(C,4,20)\nfetch(D,inter)\ninsert(D,5)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 3);

    op = oparray_get(ops, 0);
    assert(op->op_type == OP_SELECT_RANGE_ASSIGN);
    assert(strcmp(op->op_select.op_sel_var,"inter") == 0);
    assert(strcmp(op->op_select.op_sel_col,"C") == 0);
    assert(op->op_select.op_sel_low == 4);
    assert(op->op_select.op_sel_high == 20);
    s = op_string(op);
    assert(strcmp("inter=select(C,4,20)", s) == 0);
    free(s);

    op = oparray_get(ops, 1);
    assert(op->op_type == OP_FETCH);
    assert(strcmp(op->op_fetch.op_fetch_pos,"inter") == 0);
    assert(strcmp(op->op_fetch.op_fetch_col,"D") == 0);
    s = op_string(op);
    assert(strcmp("fetch(D,inter)", s) == 0);
    free(s);

    op = oparray_get(ops, 2);
    assert(op->op_type == OP_INSERT);
    assert(strcmp(op->op_insert.op_insert_col,"D") == 0);
    assert(op->op_insert.op_insert_val == 5);
    s = op_string(op);
    assert(strcmp("insert(D,5)", s) == 0);
    free(s);

    parse_cleanup(ops);
}

int main(void) {
    /*
    char buf[1024];
    int result = read(STDIN_FILENO, buf, sizeof(buf));
    if (result != -1) {
        buf[result] = '\0'; // null terminate the string
    }
    struct oparray *ops = parse_query(buf);
    (void) ops;
    */
    testbad();
    testselectall();
    testselectrange();
    testselectvalue();
    testselectallassign();
    testselectrangeassign();
    testselectvalueassign();
    testfetch();
    testcreatesorted();
    testcreateunsorted();
    testcreatebtree();
    testload();
    testinsert();
    testmultiple();
}
