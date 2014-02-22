#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "../src/common/include/list.h"

struct num {
    int n;
};

DECLLIST(num);
DEFLIST(num);

void test_head(void) {
    struct numlist *lst = numlist_create();
    assert(lst != NULL);
    assert(numlist_size(lst) == 0);
    struct num num1 = { .n = 5 };
    struct num num2 = { .n = 6 };
    struct num num3 = { .n = 7 };
    assert(numlist_addhead(lst, &num1) == 0);
    assert(numlist_addhead(lst, &num2) == 0);
    assert(numlist_addhead(lst, &num3) == 0);
    assert(numlist_size(lst) == 3);
    assert(numlist_gethead(lst) == &num3);
    assert(numlist_remhead(lst) == &num3);
    assert(numlist_gethead(lst) == &num2);
    assert(numlist_size(lst) == 2);
    assert(numlist_remhead(lst) == &num2);
    assert(numlist_gethead(lst) == &num1);
    assert(numlist_remhead(lst) == &num1);
    assert(numlist_gethead(lst) == NULL);
    assert(numlist_size(lst) == 0);
    numlist_destroy(lst);
}

void test_tail(void) {
    struct numlist *lst = numlist_create();
    assert(lst != NULL);
    assert(numlist_size(lst) == 0);
    struct num num1 = { .n = 5 };
    struct num num2 = { .n = 6 };
    struct num num3 = { .n = 7 };
    assert(numlist_addtail(lst, &num1) == 0);
    assert(numlist_addtail(lst, &num2) == 0);
    assert(numlist_addtail(lst, &num3) == 0);
    assert(numlist_size(lst) == 3);
    assert(numlist_gettail(lst) == &num3);
    assert(numlist_remtail(lst) == &num3);
    assert(numlist_gettail(lst) == &num2);
    assert(numlist_size(lst) == 2);
    assert(numlist_remtail(lst) == &num2);
    assert(numlist_gettail(lst) == &num1);
    assert(numlist_remtail(lst) == &num1);
    assert(numlist_gettail(lst) == NULL);
    assert(numlist_size(lst) == 0);
    numlist_destroy(lst);
}

void test_iter(void) {
    struct numlist *lst = numlist_create();
    assert(lst != NULL);
    assert(numlist_size(lst) == 0);
    struct num num1 = { .n = 5 };
    struct num num2 = { .n = 6 };
    struct num num3 = { .n = 7 };
    assert(numlist_addtail(lst, &num1) == 0);
    assert(numlist_addtail(lst, &num2) == 0);
    assert(numlist_addtail(lst, &num3) == 0);
    assert(numlist_size(lst) == 3);
    int nums[3] = {5, 6, 7};
    unsigned i = 0;
    for (struct numlistnode *node = numlist_iterhead(lst); node != NULL;
         node = numlist_next(node), i++) {
        struct num *item = numlist_getentry(node);
        assert(item->n == nums[i]);
    }
    i = 2;
    for (struct numlistnode *node = numlist_itertail(lst); node != NULL;
         node = numlist_prev(node), i--) {
        struct num *item = numlist_getentry(node);
        assert(item->n == nums[i]);
    }
    while (numlist_size(lst) > 0) {
        numlist_remhead(lst);
    }
    numlist_destroy(lst);
}

int main(void) {
    test_head();
    test_tail();
    test_iter();
}
