#ifndef _SERVER_PARSE_H_
#define _SERVER_PARSE_H_

#include "operators.h"
#include "../../common/include/array.h"

DECLARRAY(op);

struct oparray *parse_query(char *query);
void parse_cleanup(struct oparray *ops);

#endif
