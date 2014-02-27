#ifndef _OPERATORS_H_
#define _OPERATORS_H_

#define COLUMNLEN 256

enum op_type {
    OP_SELECT_ALL_ASSIGN,
    OP_SELECT_RANGE_ASSIGN,
    OP_SELECT_VALUE_ASSIGN,
    OP_SELECT_ALL,
    OP_SELECT_RANGE,
    OP_SELECT_VALUE,
    OP_FETCH,
    OP_CREATE,
    OP_LOAD,
    OP_INSERT,
};

enum storage_type {
    STORAGE_SORTED,
    STORAGE_UNSORTED,
    STORAGE_BTREE,
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
    char op_fetch_pos[COLUMNLEN];
};

struct op_create {
    char op_create_col[COLUMNLEN];
    enum storage_type op_create_stype;
};

// load operators will have a file descriptor for the CSV file
struct op_load {
    char op_load_file[COLUMNLEN];
};

struct op_insert {
    char op_insert_col[COLUMNLEN];
    unsigned op_insert_val;
};

struct op {
    enum op_type op_type;
    union {
        struct op_select op_select;
        struct op_fetch op_fetch;
        struct op_create op_create;
        struct op_load op_load;
        struct op_insert op_insert;
    };
};

enum storage_type storage_type_from_string(char *s);
char *storage_type_string(enum storage_type stype);
char *op_type_string(enum op_type op_type);

// This string must be destroyed by the caller
char *op_string(struct op *op);

// TODO
// support var=operator(...) in general
// mmap files

#endif
