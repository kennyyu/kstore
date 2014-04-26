#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <db/common/search.h>

void testempty(void) {
    int arr[0];
    int val = 5;
    assert(binary_search(&val, arr, 0, sizeof(int), int_compare) == 0);
}

void testsingle(void) {
    int arr[1] = {3};
    int val = 2;
    assert(binary_search(&val, arr, 1, sizeof(int), int_compare) == 0);
    val = 3;
    assert(binary_search(&val, arr, 1, sizeof(int), int_compare) == 0);
    val = 4;
    assert(binary_search(&val, arr, 1, sizeof(int), int_compare) == 1);
}

void testduplicates(void) {
    int arr[7] = {2, 3, 4, 5, 5, 6, 7};
    int val = 1;
    assert(binary_search(&val, arr, 7, sizeof(int), int_compare) == 0);
    val = 2;
    assert(binary_search(&val, arr, 7, sizeof(int), int_compare) == 0);
    val = 3;
    assert(binary_search(&val, arr, 7, sizeof(int), int_compare) == 1);
    val = 4;
    assert(binary_search(&val, arr, 7, sizeof(int), int_compare) == 2);
    val = 5;
    assert(binary_search(&val, arr, 7, sizeof(int), int_compare) == 3);
    val = 6;
    assert(binary_search(&val, arr, 7, sizeof(int), int_compare) == 5);
    val = 7;
    assert(binary_search(&val, arr, 7, sizeof(int), int_compare) == 6);
    val = 10;
    assert(binary_search(&val, arr, 7, sizeof(int), int_compare) == 7);
}

int main(void) {
    testempty();
    testsingle();
    testduplicates();
}
