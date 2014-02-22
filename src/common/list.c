#include <assert.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include "include/list.h"

struct listnode {
    struct listnode *ln_next;
    struct listnode *ln_prev;
    void *ln_item;
};

struct list {
    struct listnode lst_head;
    struct listnode lst_tail;
    unsigned int lst_size;
};

struct list *
list_create(void)
{
    struct list *lst = malloc(sizeof(struct list));
    if (lst == NULL) {
        goto done;
    }
    // initialize the head and tail to point to each other
    lst->lst_head.ln_prev = NULL;
    lst->lst_head.ln_next = &lst->lst_tail;
    lst->lst_tail.ln_next = NULL;
    lst->lst_tail.ln_prev = &lst->lst_head;
    lst->lst_size = 0;
  done:
    return lst;
}

void
list_destroy(struct list *lst)
{
    assert(lst != NULL);
    // we only want to destroy empty lists to ensure that the
    // user has freed any objects referenced in this list
    assert(list_size(lst) == 0);
    free(lst);
}

unsigned list_size(struct list *lst) {
    assert(lst != NULL);
    return lst->lst_size;
}

int
list_addhead(struct list *lst, void *item)
{
    assert(lst != NULL);
    assert(lst->lst_head.ln_prev == NULL);
    assert(lst->lst_tail.ln_next == NULL);
    int result;
    struct listnode *lnode = malloc(sizeof(struct listnode));
    if (lnode == NULL) {
        result = ENOMEM;
        goto done;
    }
    lnode->ln_item = item;
    lnode->ln_prev = &lst->lst_head;
    lnode->ln_next = lst->lst_head.ln_next;
    lnode->ln_next->ln_prev = lnode;
    lnode->ln_prev->ln_next = lnode;
    lst->lst_size++;
    result = 0;
  done:
    return result;
}

int
list_addtail(struct list *lst, void *item)
{
    assert(lst != NULL);
    assert(lst->lst_head.ln_prev == NULL);
    assert(lst->lst_tail.ln_next == NULL);
    int result;
    struct listnode *lnode = malloc(sizeof(struct listnode));
    if (lnode == NULL) {
        result = ENOMEM;
        goto done;
    }
    lnode->ln_item = item;
    lnode->ln_next = &lst->lst_tail;
    lnode->ln_prev = lst->lst_tail.ln_prev;
    lnode->ln_next->ln_prev = lnode;
    lnode->ln_prev->ln_next = lnode;
    lst->lst_size++;
    result = 0;
  done:
    return result;
}

void *
list_gethead(struct list *lst)
{
    assert(lst != NULL);
    if (lst->lst_size == 0) {
        return NULL;
    }
    return lst->lst_head.ln_next->ln_item;
}

void *
list_gettail(struct list *lst)
{
    assert(lst != NULL);
    if (lst->lst_size == 0) {
        return NULL;
    }
    return lst->lst_tail.ln_prev->ln_item;
}

void *
list_remhead(struct list *lst)
{
    assert(lst != NULL);
    assert(lst->lst_size > 0);
    struct listnode *lnode = lst->lst_head.ln_next;
    assert(lnode != &lst->lst_tail);
    void *item = lnode->ln_item;
    lnode->ln_next->ln_prev = lnode->ln_prev;
    lnode->ln_prev->ln_next = lnode->ln_next;
    free(lnode);
    lst->lst_size--;
    return item;
}

void *
list_remtail(struct list *lst)
{
    assert(lst != NULL);
    assert(lst->lst_size > 0);
    struct listnode *lnode = lst->lst_tail.ln_prev;
    assert(lnode != &lst->lst_head);
    void *item = lnode->ln_item;
    lnode->ln_next->ln_prev = lnode->ln_prev;
    lnode->ln_prev->ln_next = lnode->ln_next;
    free(lnode);
    lst->lst_size--;
    return item;
}

// returns NULL if there are no more items left
struct listnode *
list_iterhead(struct list *lst)
{
    assert(lst != NULL);
    if (lst->lst_size == 0) {
        return NULL;
    }
    assert(lst->lst_head.ln_prev == NULL);
    assert(lst->lst_tail.ln_next == NULL);
    assert(lst->lst_head.ln_next != &lst->lst_tail);
    assert(lst->lst_tail.ln_prev != &lst->lst_head);
    return lst->lst_head.ln_next;
}

struct listnode *
list_itertail(struct list *lst)
{
    assert(lst != NULL);
    if (lst->lst_size == 0) {
        return NULL;
    }
    assert(lst->lst_head.ln_prev == NULL);
    assert(lst->lst_tail.ln_next == NULL);
    assert(lst->lst_head.ln_next != &lst->lst_tail);
    assert(lst->lst_tail.ln_prev != &lst->lst_head);
    return lst->lst_tail.ln_prev;
}

struct listnode *
list_next(struct listnode *lnode)
{
    assert(lnode != NULL);
    assert(lnode->ln_next != NULL);
    assert(lnode->ln_prev != NULL);
    if (lnode->ln_next->ln_next == NULL) {
        return NULL;
    }
    return lnode->ln_next;
}

struct listnode *
list_prev(struct listnode *lnode)
{
    assert(lnode != NULL);
    assert(lnode->ln_next != NULL);
    assert(lnode->ln_prev != NULL);
    if (lnode->ln_prev->ln_prev == NULL) {
        return NULL;
    }
    return lnode->ln_prev;
}

void *
list_getentry(struct listnode *lnode)
{
    assert(lnode != NULL);
    assert(lnode->ln_next != NULL);
    assert(lnode->ln_prev != NULL);
    return lnode->ln_item;
}
