#ifndef _PARSE_H_
#define _PARSE_H_

#include "operators.h"
#include "../../common/include/array.h"

DECLARRAY(op);

struct op *parse_line(char *line);
void parse_cleanup_op(struct op *op);
struct oparray *parse_query(char *query);
void parse_cleanup_ops(struct oparray *ops);

#endif
