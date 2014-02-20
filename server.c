#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "include/threadpool.h"

int main(void) {
    struct threadpool *tpool = threadpool_create(5);
    assert(tpool != NULL);
    threadpool_destroy(tpool);
    printf("hello server\n");
}
