#ifndef _BTREE_H_
#define _BTREE_H_

#include <stdint.h>
#include "file.h"
#include "../../common/include/cassert.h"

// location of the root page
#define BTREE_ROOT_PAGE FILE_FIRST_PAGE

enum btree_node_type {
    BTREE_NODE_INTERNAL,
    BTREE_NODE_LEAF,
};

struct btree_entry {
    int bte_key;
    uint32_t bte_padding;
    union {
        uint64_t bte_index; // index of this value
        page_t bte_page; // pointer to page with values >= bte_key
    };
};

CASSERT(PAGESIZE % sizeof(struct btree_entry) == 0);

struct btree_header {
    enum btree_node_type bth_type;
    uint32_t bth_nentries; // num entries in this node
    union {
        page_t bth_next; // used for LEAF nodes
        page_t bth_left; // used for INTERNAL nodes
    };
};

CASSERT(sizeof(struct btree_header) % sizeof(struct btree_entry) == 0);

#define BTENTRY_PER_PAGE ((PAGESIZE - sizeof(struct btree_header)) / sizeof(struct btree_entry))

// this will be the size of a page
struct btree_node {
    struct btree_header bt_header;
    struct btree_entry bt_entries[BTENTRY_PER_PAGE];
};

CASSERT(PAGESIZE == sizeof(struct btree_node));

#endif
