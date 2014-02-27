#ifndef _LIST_H
#define _LIST_H

struct listnode;
struct list;

struct list *list_create(void);
void list_destroy(struct list *lst);
unsigned list_size(struct list *lst);
int list_addhead(struct list *lst, void *item);
int list_addtail(struct list *lst, void *item);
void *list_gethead(struct list *lst);
void *list_gettail(struct list *lst);
void *list_remhead(struct list *lst);
void *list_remtail(struct list *lst);

// returns NULL if there are no more items left
struct listnode *list_iterhead(struct list *lst);
struct listnode *list_itertail(struct list *lst);
struct listnode *list_next(struct listnode *lnode);
struct listnode *list_prev(struct listnode *lnode);
void *list_getentry(struct listnode *lnode);

/*
 * EXAMPLE USE:
 *
 * struct foo {};
 * DECLLIST(foo);
 * DEFLIST(foo);
 * for (struct foolistnode *lnode = foolist_iterhead(lst);
 *      lnode != NULL; lnode = foolist_next(lnode)) {
 *     struct foo *item = foolist_getentry(lnode);
 *     ...
 * }
 */

#define DECLLIST_BYTYPE(LIST, T) \
    struct LIST; \
    struct LIST##node; \
    struct LIST *LIST##_create(void); \
    void LIST##_destroy(struct LIST *lst); \
    unsigned LIST##_size(struct LIST *lst); \
    int LIST##_addhead(struct LIST *lst, T *item); \
    int LIST##_addtail(struct LIST *lst, T *item); \
    T *LIST##_gethead(struct LIST *lst); \
    T *LIST##_gettail(struct LIST *lst); \
    T *LIST##_remhead(struct LIST *lst); \
    T *LIST##_remtail(struct LIST *lst); \
    struct LIST##node *LIST##_iterhead(struct LIST *lst); \
    struct LIST##node *LIST##_itertail(struct LIST *lst); \
    struct LIST##node *LIST##_next(struct LIST##node *lnode); \
    struct LIST##node *LIST##_prev(struct LIST##node *lnode); \
    T *LIST##_getentry(struct LIST##node *lnode);

#define DEFLIST_BYTYPE(LIST, T) \
    struct LIST { \
        struct list *lst; \
    }; \
    struct LIST##node {}; \
    struct LIST *LIST##_create(void) { \
        struct LIST *lst = malloc(sizeof(struct LIST)); \
        if (lst == NULL) { \
            goto done; \
        } \
        lst->lst = list_create(); \
        if (lst->lst == NULL) { \
            goto cleanup_malloc; \
        } \
        goto done; \
      cleanup_malloc: \
        free(lst); \
        lst = NULL; \
      done: \
        return lst; \
    } \
    void LIST##_destroy(struct LIST *lst) { \
        list_destroy(lst->lst); \
        free(lst); \
    } \
    unsigned LIST##_size(struct LIST *lst) { \
        return list_size(lst->lst); \
    } \
    int LIST##_addhead(struct LIST *lst, T *item) { \
        return list_addhead(lst->lst, (void *) item); \
    }\
    int LIST##_addtail(struct LIST *lst, T *item) { \
        return list_addtail(lst->lst, (void *) item); \
    }\
    T *LIST##_gethead(struct LIST *lst) { \
        return list_gethead(lst->lst); \
    }\
    T *LIST##_gettail(struct LIST *lst) { \
        return list_gettail(lst->lst); \
    } \
    T *LIST##_remhead(struct LIST *lst) { \
        return list_remhead(lst->lst); \
    } \
    T *LIST##_remtail(struct LIST *lst) { \
        return list_remtail(lst->lst); \
    } \
    struct LIST##node *LIST##_iterhead(struct LIST *lst) { \
        return (struct LIST##node *) list_iterhead(lst->lst); \
    }\
    struct LIST##node *LIST##_itertail(struct LIST *lst) { \
        return (struct LIST##node *) list_itertail(lst->lst); \
    } \
    struct LIST##node *LIST##_next(struct LIST##node *lnode) { \
        return (struct LIST##node *) list_next((struct listnode *) lnode); \
    } \
    struct LIST##node *LIST##_prev(struct LIST##node *lnode) { \
        return (struct LIST##node *) list_prev((struct listnode *) lnode); \
    } \
    T *LIST##_getentry(struct LIST##node *lnode) { \
        return (T *) list_getentry((struct listnode *) lnode); \
    }

#define DECLLIST(T) DECLLIST_BYTYPE(T##list, struct T)
#define DEFLIST(T) DEFLIST_BYTYPE(T##list, struct T)

#endif
