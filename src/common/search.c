#include <assert.h>
#include <db/common/search.h>

int
int_compare(const void *a, const void *b)
{
    return (*((int *) a) - *((int *) b));
}

unsigned
binary_search(void *x, void *vals, unsigned nvals, size_t size,
              int (*compare)(const void *a, const void *b))
{
    unsigned l = 0;
    unsigned r = nvals;
    while (l < r) {
        unsigned m = l + (r - l) / 2;
        void *y = vals + size * m;
        if (compare(y, x) < 0) {
            l = m + 1;
        } else {
            r = m;
        }
    }
    return l;
}
