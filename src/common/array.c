/*-
 * Copyright (c) 2009 The NetBSD Foundation, Inc.
 * All rights reserved.
 *
 * This code is derived from software contributed to The NetBSD Foundation
 * by David A. Holland.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE NETBSD FOUNDATION, INC. AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#define ARRAYINLINE

#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "include/array.h"

struct array *
array_create(void) {
    struct array *a;

    a = malloc(sizeof(*a));
    if (a != NULL) {
        array_init(a);
    }
    return a;
}

void array_destroy(struct array *a) {
    array_cleanup(a);
    free(a);
}

void array_init(struct array *a) {
    a->num = a->max = 0;
    a->v = NULL;
}

void array_cleanup(struct array *a) {
    /*
     * Require array to be empty - helps avoid memory leaks since
     * we don't/can't free anything any contents may be pointing
     * to.
     */
    ARRAYASSERT(a->num == 0);
    free(a->v);
#ifdef ARRAYS_CHECKED
    a->v = NULL;
#endif
}

int array_setsize(struct array *a, unsigned num) {
    void **newptr;
    unsigned newmax;

    if (num > a->max) {
        /* Don't touch A until the allocation succeeds. */
        newmax = a->max;
        while (num > newmax) {
            newmax = newmax ? newmax * 2 : 4;
        }

        /*
         * We don't have krealloc, and it wouldn't be
         * worthwhile to implement just for this. So just
         * allocate a new block and copy. (Exercise: what
         * about this and/or malloc makes it not worthwhile?)
         */

        newptr = malloc(newmax * sizeof(*a->v));
        if (newptr == NULL) {
            return ENOMEM;
        }
        memcpy(newptr, a->v, a->num * sizeof(*a->v));
        free(a->v);
        a->v = newptr;
        a->max = newmax;
    }
    a->num = num;

    return 0;
}

void array_remove(struct array *a, unsigned index) {
    unsigned num_to_move;

    ARRAYASSERT(a->num <= a->max);
    ARRAYASSERT(index < a->num);

    num_to_move = a->num - (index + 1);
    memmove(a->v + index, a->v + index + 1, num_to_move * sizeof(void *));
    a->num--;
}

unsigned array_num(const struct array *a) {
    return a->num;
}

void *
array_get(const struct array *a, unsigned index) {
    ARRAYASSERT(index < a->num);
    return a->v[index];
}

void array_set(const struct array *a, unsigned index, void *val) {
    ARRAYASSERT(index < a->num);
    a->v[index] = val;
}

int array_add(struct array *a, void *val, unsigned *index_ret) {
    unsigned index;
    int ret;

    index = a->num;
    ret = array_setsize(a, index + 1);
    if (ret) {
        return ret;
    }
    a->v[index] = val;
    if (index_ret != NULL) {
        *index_ret = index;
    }
    return 0;
}
