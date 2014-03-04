#include <assert.h>
#include "include/search.h"

unsigned
binary_search(void *x, void *vals, unsigned nvals, size_t size,
              int (*compare)(void *a, void *b))
{
    unsigned l = 0;
    unsigned r = nvals;
    while (l < r) {
        unsigned m = l + (r - l) / 2;
        void *y = *((void **)(vals + size * m));
        if (compare(y, x) < 0) {
            l = m + 1;
        } else {
            r = m;
        }
    }
    return l;
}
