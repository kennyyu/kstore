#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "../src/server/include/parser.h"

int main(void) {
    char buf[1024];
    int result = read(STDIN_FILENO, buf, sizeof(buf));
    if (result != -1) {
        buf[result] = '\0'; // null terminate the string
    }
    parse_query(buf);
}
