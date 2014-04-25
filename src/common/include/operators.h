#ifndef _OPERATORS_H_
#define _OPERATORS_H_

#include <stdbool.h>

#define COLUMNLEN 256
#define TUPLELEN 16384

enum op_type {
    OP_SELECT_ALL_ASSIGN,
    OP_SELECT_RANGE_ASSIGN,
    OP_SELECT_VALUE_ASSIGN,
    OP_SELECT_ALL,
    OP_SELECT_RANGE,
    OP_SELECT_VALUE,
    OP_FETCH,
    OP_FETCH_ASSIGN,
    OP_CREATE,
    OP_LOAD,
    OP_INSERT_SINGLE,
    OP_INSERT,
    OP_DELETE,
    OP_UPDATE,
    OP_TUPLE,
    OP_AGG,
    OP_MATH,
    OP_PRINT,
    OP_JOIN,
};

enum storage_type {
    STORAGE_SORTED,
    STORAGE_UNSORTED,
    STORAGE_BTREE,
};

enum agg_type {
    AGG_MIN,
    AGG_MAX,
    AGG_SUM,
    AGG_AVG,
    AGG_COUNT,
};

enum math_type {
    MATH_ADD,
    MATH_SUB,
    MATH_MUL,
    MATH_DIV,
};

enum join_type {
    JOIN_LOOP,
    JOIN_SORT,
    JOIN_TREE,
    JOIN_HASH,
};

struct op_tuple {
    char op_tuple_vars[TUPLELEN];
};

// struct for all the select queries
// depending on the select operator, some fields will not be used
struct op_select {
    char op_sel_var[COLUMNLEN];
    char op_sel_col[COLUMNLEN];
    union {
        struct {
            unsigned op_sel_low;
            unsigned op_sel_high;
        };
        unsigned op_sel_value;
    };
};

struct op_fetch {
    char op_fetch_col[COLUMNLEN];
    char op_fetch_pos[COLUMNLEN / 2];
    char op_fetch_var[COLUMNLEN / 2];
};

struct op_create {
    char op_create_col[COLUMNLEN];
    enum storage_type op_create_stype;
};

// load operators will have a file descriptor for the CSV file
struct op_load {
    char op_load_file[COLUMNLEN];
};

struct op_insert_single {
    char op_insert_single_col[COLUMNLEN];
    int op_insert_single_val;
};

struct op_insert {
    char op_insert_cols[TUPLELEN];
};

struct op_delete {
    char op_delete_var[COLUMNLEN];
    char op_delete_cols[COLUMNLEN];
};

struct op_update {
    char op_update_var[COLUMNLEN];
    char op_update_col[COLUMNLEN];
    int op_update_val;
};

struct op_agg {
    enum agg_type op_agg_atype;
    bool op_agg_assign;
    char op_agg_var[COLUMNLEN];
    char op_agg_col[COLUMNLEN];
};

struct op_math {
    enum math_type op_math_mtype;
    bool op_math_assign;
    char op_math_var[COLUMNLEN];
    char op_math_col1[COLUMNLEN];
    char op_math_col2[COLUMNLEN];
};

struct op_print {
    char op_print_var[COLUMNLEN];
};

struct op_join {
    enum join_type op_join_jtype;
    char op_join_inputL[COLUMNLEN];
    char op_join_inputR[COLUMNLEN];
    char op_join_varL[COLUMNLEN];
    char op_join_varR[COLUMNLEN];
};

struct op {
    enum op_type op_type;
    union {
        struct op_select op_select;
        struct op_fetch op_fetch;
        struct op_create op_create;
        struct op_load op_load;
        struct op_insert_single op_insert_single;
        struct op_insert op_insert;
        struct op_delete op_delete;
        struct op_update op_update;
        struct op_tuple op_tuple;
        struct op_agg op_agg;
        struct op_math op_math;
        struct op_print op_print;
        struct op_join op_join;
    };
};

enum storage_type storage_type_from_string(char *s);
char *storage_type_string(enum storage_type stype);
char *math_type_string(enum math_type mtype);
char *agg_type_string(enum agg_type atype);
char *join_type_string(enum join_type jtype);

// This string must be destroyed by the caller
char *op_string(struct op *op);

// TODO
// support var=operator(...) in general
// mmap files

#endif
