#ifndef _SEARCH_H_
#define _SEARCH_H_

#include <stddef.h>

// Binary search
// Returns the lower bound index on where x would be inserted
// if we were to insert x into the array. The caller must still check
// if the value at the returned index.
unsigned binary_search(void *x, void *vals, unsigned nvals, size_t size,
                       int (*compare)(const void *a, const void *b));

#endif
