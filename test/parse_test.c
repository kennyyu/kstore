#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "../src/common/include/parser.h"

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
    parse_cleanup_ops(ops);
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
    parse_cleanup_ops(ops);
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
    parse_cleanup_ops(ops);
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
    parse_cleanup_ops(ops);
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
    parse_cleanup_ops(ops);
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
    parse_cleanup_ops(ops);
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
    parse_cleanup_ops(ops);
}

void testfetchassign(void) {
    char *query = "cout=fetch(C,foo)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_FETCH_ASSIGN);
    assert(strcmp(op->op_fetch.op_fetch_var,"cout") == 0);
    assert(strcmp(op->op_fetch.op_fetch_pos,"foo") == 0);
    assert(strcmp(op->op_fetch.op_fetch_col,"C") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testcreatesorted(void) {
    char *query = "create(C,\"sorted\")";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_CREATE);
    assert(strcmp(op->op_create.op_create_col,"C") == 0);
    assert(op->op_create.op_create_stype == STORAGE_SORTED);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testcreateunsorted(void) {
    char *query = "create(C,\"unsorted\")";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_CREATE);
    assert(strcmp(op->op_create.op_create_col,"C") == 0);
    assert(op->op_create.op_create_stype == STORAGE_UNSORTED);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testcreatebtree(void) {
    char *query = "create(C,\"b+tree\")";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_CREATE);
    assert(strcmp(op->op_create.op_create_col,"C") == 0);
    assert(op->op_create.op_create_stype == STORAGE_BTREE);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testload(void) {
    char *query = "load(\"/home/jharvard/foo.csv\")";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_LOAD);
    assert(strcmp(op->op_load.op_load_file, "/home/jharvard/foo.csv") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testinsert(void) {
    char *query = "insert(C,5)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_INSERT_SINGLE);
    assert(strcmp(op->op_insert.op_insert_col,"C") == 0);
    assert(op->op_insert.op_insert_val == 5);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testtuple(void) {
    char *query = "tuple(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_TUPLE);
    assert(strcmp(op->op_tuple.op_tuple_vars,"aout,bout") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testaddassign(void) {
    char *query = "x=add(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_MATH);
    assert(op->op_math.op_math_mtype == MATH_ADD);
    assert(strcmp(op->op_math.op_math_col1,"aout") == 0);
    assert(strcmp(op->op_math.op_math_col2,"bout") == 0);
    assert(op->op_math.op_math_assign == true);
    assert(strcmp(op->op_math.op_math_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testadd(void) {
    char *query = "add(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_MATH);
    assert(op->op_math.op_math_mtype == MATH_ADD);
    assert(strcmp(op->op_math.op_math_col1,"aout") == 0);
    assert(strcmp(op->op_math.op_math_col2,"bout") == 0);
    assert(op->op_math.op_math_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testsubassign(void) {
    char *query = "x=sub(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_MATH);
    assert(op->op_math.op_math_mtype == MATH_SUB);
    assert(strcmp(op->op_math.op_math_col1,"aout") == 0);
    assert(strcmp(op->op_math.op_math_col2,"bout") == 0);
    assert(op->op_math.op_math_assign == true);
    assert(strcmp(op->op_math.op_math_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testsub(void) {
    char *query = "sub(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_MATH);
    assert(op->op_math.op_math_mtype == MATH_SUB);
    assert(strcmp(op->op_math.op_math_col1,"aout") == 0);
    assert(strcmp(op->op_math.op_math_col2,"bout") == 0);
    assert(op->op_math.op_math_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testmulassign(void) {
    char *query = "x=mul(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_MATH);
    assert(op->op_math.op_math_mtype == MATH_MUL);
    assert(strcmp(op->op_math.op_math_col1,"aout") == 0);
    assert(strcmp(op->op_math.op_math_col2,"bout") == 0);
    assert(op->op_math.op_math_assign == true);
    assert(strcmp(op->op_math.op_math_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testmul(void) {
    char *query = "mul(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_MATH);
    assert(op->op_math.op_math_mtype == MATH_MUL);
    assert(strcmp(op->op_math.op_math_col1,"aout") == 0);
    assert(strcmp(op->op_math.op_math_col2,"bout") == 0);
    assert(op->op_math.op_math_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testdivassign(void) {
    char *query = "x=div(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_MATH);
    assert(op->op_math.op_math_mtype == MATH_DIV);
    assert(strcmp(op->op_math.op_math_col1,"aout") == 0);
    assert(strcmp(op->op_math.op_math_col2,"bout") == 0);
    assert(op->op_math.op_math_assign == true);
    assert(strcmp(op->op_math.op_math_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testdiv(void) {
    char *query = "div(aout,bout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_MATH);
    assert(op->op_math.op_math_mtype == MATH_DIV);
    assert(strcmp(op->op_math.op_math_col1,"aout") == 0);
    assert(strcmp(op->op_math.op_math_col2,"bout") == 0);
    assert(op->op_math.op_math_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testminassign(void) {
    char *query = "x=min(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_MIN);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == true);
    assert(strcmp(op->op_agg.op_agg_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testmin(void) {
    char *query = "min(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_MIN);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testmaxassign(void) {
    char *query = "x=max(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_MAX);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == true);
    assert(strcmp(op->op_agg.op_agg_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testmax(void) {
    char *query = "max(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_MAX);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testavgassign(void) {
    char *query = "x=avg(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_AVG);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == true);
    assert(strcmp(op->op_agg.op_agg_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testavg(void) {
    char *query = "avg(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_AVG);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testsumassign(void) {
    char *query = "x=sum(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_SUM);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == true);
    assert(strcmp(op->op_agg.op_agg_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testsum(void) {
    char *query = "sum(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_SUM);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testcountassign(void) {
    char *query = "x=count(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_COUNT);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == true);
    assert(strcmp(op->op_agg.op_agg_var,"x") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testcount(void) {
    char *query = "count(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_AGG);
    assert(op->op_agg.op_agg_atype == AGG_COUNT);
    assert(strcmp(op->op_agg.op_agg_col,"aout") == 0);
    assert(op->op_agg.op_agg_assign == false);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testprint(void) {
    char *query = "print(aout)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_PRINT);
    assert(strcmp(op->op_print.op_print_var,"aout") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testloopjoin(void) {
    char *query = "r,s=loopjoin(a,b)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_JOIN);
    assert(op->op_join.op_join_jtype == JOIN_LOOP);
    assert(strcmp(op->op_join.op_join_varL,"r") == 0);
    assert(strcmp(op->op_join.op_join_varR,"s") == 0);
    assert(strcmp(op->op_join.op_join_inputL,"a") == 0);
    assert(strcmp(op->op_join.op_join_inputR,"b") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testsortjoin(void) {
    char *query = "r,s=sortjoin(a,b)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_JOIN);
    assert(op->op_join.op_join_jtype == JOIN_SORT);
    assert(strcmp(op->op_join.op_join_varL,"r") == 0);
    assert(strcmp(op->op_join.op_join_varR,"s") == 0);
    assert(strcmp(op->op_join.op_join_inputL,"a") == 0);
    assert(strcmp(op->op_join.op_join_inputR,"b") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testtreejoin(void) {
    char *query = "r,s=treejoin(a,b)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_JOIN);
    assert(op->op_join.op_join_jtype == JOIN_TREE);
    assert(strcmp(op->op_join.op_join_varL,"r") == 0);
    assert(strcmp(op->op_join.op_join_varR,"s") == 0);
    assert(strcmp(op->op_join.op_join_inputL,"a") == 0);
    assert(strcmp(op->op_join.op_join_inputR,"b") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testhashjoin(void) {
    char *query = "r,s=hashjoin(a,b)";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 1);
    struct op *op = oparray_get(ops, 0);
    assert(op->op_type == OP_JOIN);
    assert(op->op_join.op_join_jtype == JOIN_HASH);
    assert(strcmp(op->op_join.op_join_varL,"r") == 0);
    assert(strcmp(op->op_join.op_join_varR,"s") == 0);
    assert(strcmp(op->op_join.op_join_inputL,"a") == 0);
    assert(strcmp(op->op_join.op_join_inputR,"b") == 0);
    char *s = op_string(op);
    assert(strcmp(query, s) == 0);
    free(s);
    parse_cleanup_ops(ops);
}

void testbad(void) {
    char *query = "";
    struct oparray *ops = parse_query(query);
    assert(oparray_num(ops) == 0);
    parse_cleanup_ops(ops);
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
    assert(op->op_type == OP_INSERT_SINGLE);
    assert(strcmp(op->op_insert.op_insert_col,"D") == 0);
    assert(op->op_insert.op_insert_val == 5);
    s = op_string(op);
    assert(strcmp("insert(D,5)", s) == 0);
    free(s);

    parse_cleanup_ops(ops);
}

int main(void) {
    testbad();
    testselectall();
    testselectrange();
    testselectvalue();
    testselectallassign();
    testselectrangeassign();
    testselectvalueassign();
    testfetch();
    testfetchassign();
    testcreatesorted();
    testcreateunsorted();
    testcreatebtree();
    testload();
    testinsert();
    testtuple();
    testmultiple();
    testmin();
    testminassign();
    testmax();
    testmaxassign();
    testsum();
    testsumassign();
    testcount();
    testcountassign();
    testavg();
    testavgassign();
    testadd();
    testaddassign();
    testsub();
    testsubassign();
    testmul();
    testmulassign();
    testdiv();
    testdivassign();
    testprint();
    testloopjoin();
    testsortjoin();
    testtreejoin();
    testhashjoin();
}
