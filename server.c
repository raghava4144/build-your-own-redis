// server.c
//
// Commit 3: the event loop.
//
// Commits 1-2 could only serve ONE client at a time - accept() blocked until
// someone connected, then we sat there handling only that client until they
// disconnected, before accepting the next one.
//
// This version handles MANY clients at once, on a single thread, by never
// blocking on any individual one. The recipe:
//
//   1. Every socket is set to non-blocking mode. A read() with nothing
//      available returns immediately (errno == EAGAIN) instead of freezing.
//   2. Each loop iteration, we ask the OS via poll() "which of my sockets
//      actually have something ready right now?" - that's the ONLY blocking
//      call in the whole program, and it waits for ANY socket, not one
//      specific client.
//   3. Because one client's message can now arrive across several loop
//      iterations (we only grab whatever's available, not "wait until we
//      have 4 bytes"), each connection needs its own persistent buffers to
//      remember what's been received/sent so far. That's `struct Conn`.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <time.h>      // clock_gettime, for timers
#include <pthread.h>   // thread pool, Commit 9

static void die(const char *m) {
    perror(m);
    exit(1);
}

const size_t k_max_msg = 4096;
const size_t k_max_args = 16;      // max strings in one command

// ---------------------------------------------------------------------
// Buffer: a small growable byte array, since plain C doesn't give us
// std::vector. We only need two operations: append to the back, and
// consume (remove) from the front.
//
// NOTE: buf_consume() below shifts the remaining bytes down with memmove(),
// which is O(n). The book flags this as something to optimize later (a
// proper ring buffer avoids the shift) - we're keeping it simple for now
// and can revisit it as a performance pass once everything works.
// ---------------------------------------------------------------------
typedef struct {
    uint8_t *data;
    size_t size;    // bytes currently stored
    size_t cap;     // allocated capacity
} Buffer;

static void buf_init(Buffer *b) {
    b->data = NULL;
    b->size = 0;
    b->cap = 0;
}

static void buf_free(Buffer *b) {
    free(b->data);
    b->data = NULL;
    b->size = 0;
    b->cap = 0;
}

static void buf_append(Buffer *b, const uint8_t *data, size_t len) {
    if (b->size + len > b->cap) {
        size_t new_cap = b->cap ? b->cap * 2 : 64;
        while (new_cap < b->size + len) {
            new_cap *= 2;
        }
        uint8_t *new_data = realloc(b->data, new_cap);
        if (!new_data) {
            die("realloc() in buf_append");
        }
        b->data = new_data;
        b->cap = new_cap;
    }
    memcpy(b->data + b->size, data, len);
    b->size += len;
}

static void buf_consume(Buffer *b, size_t n) {
    if (n >= b->size) {
        b->size = 0;
        return;
    }
    memmove(b->data, b->data + n, b->size - n);
    b->size -= n;
}

// ---------------------------------------------------------------------
// The data store (Commit 5: a real hashtable)
//
// Commit 4's linear scan was O(N) per operation and capped at 1024 keys.
// This version is a chaining hashtable with INTRUSIVE nodes (the HNode
// lives directly inside Entry - no separate allocation for list nodes,
// same trick as the book), plus progressive resizing so a big rehash
// never freezes every client at once.
// ---------------------------------------------------------------------

#include <stddef.h>   // offsetof
#include <assert.h>

// Recover the address of the struct containing `ptr` as its `member`
// field. This is what makes the hashtable code "generic" without C++
// templates or void* - it operates purely on HNode, and container_of()
// converts back to the real Entry when needed.
#define container_of(ptr, type, member) \
    ((type *)((char *)(ptr) - offsetof(type, member)))

// An intrusive singly-linked-list node. No data of its own - it lives
// embedded inside Entry below.
typedef struct HNode {
    struct HNode *next;
    uint64_t hcode;     // cached hash value, avoids recomputing on lookups
} HNode;

// A fixed-size chaining hashtable: an array of "slots", each slot the head
// of a linked list of colliding entries.
typedef struct {
    HNode **tab;    // array of slot heads, size (mask + 1)
    size_t mask;    // always (power of 2) - 1, so hash & mask == hash % size
    size_t size;    // number of keys currently in this table
} HTab;

static void h_init(HTab *htab, size_t n) {
    assert(n > 0 && (n & (n - 1)) == 0);   // n must be a power of 2
    htab->tab = calloc(n, sizeof(HNode *));
    htab->mask = n - 1;
    htab->size = 0;
}

static void h_insert(HTab *htab, HNode *node) {
    size_t pos = node->hcode & htab->mask;
    node->next = htab->tab[pos];
    htab->tab[pos] = node;
    htab->size++;
}

// Returns the address of the pointer THAT POINTS TO the matching node
// (either a slot in `tab`, or another node's `next` field) - not the node
// itself. That's deliberate: having the parent pointer's address is what
// lets deletion (h_detach) unlink a node in O(1) without a special case
// for "removing the first node in the chain."
static HNode **h_lookup(HTab *htab, HNode *key, bool (*eq)(HNode *, HNode *)) {
    if (!htab->tab) {
        return NULL;
    }
    size_t pos = key->hcode & htab->mask;
    HNode **from = &htab->tab[pos];
    for (HNode *cur = *from; cur != NULL; cur = *from) {
        if (cur->hcode == key->hcode && eq(cur, key)) {
            return from;
        }
        from = &cur->next;
    }
    return NULL;
}

static HNode *h_detach(HTab *htab, HNode **from) {
    HNode *node = *from;
    *from = node->next;
    htab->size--;
    return node;
}

// HMap: the resizable hashtable built from two HTabs. `newer` is what's
// normally used; when it gets too full, it becomes `older` and a fresh,
// bigger `newer` takes over. Keys are migrated from `older` to `newer` a
// few at a time on every operation (see hm_help_rehashing), instead of
// all at once - that's what keeps a resize from freezing the server.
typedef struct {
    HTab newer;
    HTab older;
    size_t migrate_pos;    // where we left off migrating in `older`
} HMap;

#define K_REHASHING_WORK 128     // max keys migrated per operation
#define K_MAX_LOAD_FACTOR 8      // trigger a resize once avg chain length hits this

static void hm_help_rehashing(HMap *hmap) {
    size_t nwork = 0;
    while (nwork < K_REHASHING_WORK && hmap->older.size > 0) {
        HNode **from = &hmap->older.tab[hmap->migrate_pos];
        if (!*from) {
            hmap->migrate_pos++;
            continue;   // empty slot, nothing to migrate here
        }
        h_insert(&hmap->newer, h_detach(&hmap->older, from));
        nwork++;
    }
    if (hmap->older.size == 0 && hmap->older.tab) {
        free(hmap->older.tab);
        hmap->older = (HTab){0};
    }
}

static void hm_trigger_rehashing(HMap *hmap) {
    hmap->older = hmap->newer;                      // newer becomes older
    h_init(&hmap->newer, (hmap->newer.mask + 1) * 2); // fresh table, 2x size
    hmap->migrate_pos = 0;
}

static void hm_insert(HMap *hmap, HNode *node) {
    if (!hmap->newer.tab) {
        h_init(&hmap->newer, 4);   // lazily create the table on first use
    }
    h_insert(&hmap->newer, node);

    if (!hmap->older.tab) {   // only trigger a NEW resize if one isn't already running
        size_t threshold = (hmap->newer.mask + 1) * K_MAX_LOAD_FACTOR;
        if (hmap->newer.size >= threshold) {
            hm_trigger_rehashing(hmap);
        }
    }
    hm_help_rehashing(hmap);   // migrate a bit more, whether we just triggered or not
}

static HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *)) {
    hm_help_rehashing(hmap);   // small, steady progress on every operation
    HNode **from = h_lookup(&hmap->newer, key, eq);
    if (!from) {
        from = h_lookup(&hmap->older, key, eq);
    }
    return from ? *from : NULL;
}

static HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *)) {
    hm_help_rehashing(hmap);
    HNode **from = h_lookup(&hmap->newer, key, eq);
    if (from) {
        return h_detach(&hmap->newer, from);
    }
    from = h_lookup(&hmap->older, key, eq);
    if (from) {
        return h_detach(&hmap->older, from);
    }
    return NULL;
}

// FNV-1a: a fast, well-distributed hash function designed for hashtables
// (NOT a cryptographic hash - using MD5/SHA1 here would be slow and
// overkill, they solve a different problem).
static uint64_t fnv1a_hash(const uint8_t *data, size_t len) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= data[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

// ---------------------------------------------------------------------
// AVL tree (Commit 7): a height-balanced binary search tree.
//
// Why we need this in addition to the hashtable: a hashtable is great for
// "give me the value for this exact key" (O(1)) but useless for "give me
// everything in order" or "give me the item ranked 50th" - hashing
// deliberately scrambles order. A sorted set needs BOTH: instant lookup
// by name (hashtable, reused from Commit 5) AND a structure that keeps
// everything sorted by score (this tree).
//
// "Balanced" matters: a plain binary search tree can degenerate into a
// straight line (O(N) operations) if items arrive in sorted order. AVL
// trees actively re-balance after every insert/delete so the height never
// exceeds O(log N), guaranteeing fast operations even in the worst case.
// ---------------------------------------------------------------------
typedef struct AVLNode {
    struct AVLNode *parent;
    struct AVLNode *left;
    struct AVLNode *right;
    uint32_t height;   // height of this subtree, for balance checks
    uint32_t cnt;       // size of this subtree, for O(log N) rank queries
} AVLNode;

static void avl_init(AVLNode *node) {
    node->parent = node->left = node->right = NULL;
    node->height = 1;
    node->cnt = 1;
}

static uint32_t avl_height(AVLNode *node) { return node ? node->height : 0; }
static uint32_t avl_cnt(AVLNode *node) { return node ? node->cnt : 0; }
static uint32_t max_u32(uint32_t a, uint32_t b) { return a > b ? a : b; }

// Recompute this node's height/count from its children. Must be called
// bottom-up after any structural change - a node's stats depend on its
// children's stats.
static void avl_update(AVLNode *node) {
    node->height = 1 + max_u32(avl_height(node->left), avl_height(node->right));
    node->cnt = 1 + avl_cnt(node->left) + avl_cnt(node->right);
}

// Rotations: reshape a subtree to fix a height imbalance WITHOUT changing
// the sorted order of the data. See the ASCII diagrams in the book if you
// want to re-derive why this preserves order - the short version is that
// only 2 parent/child links change, everything else's relative position
// stays the same.
static AVLNode *rot_left(AVLNode *node) {
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->right;
    AVLNode *inner = new_node->left;
    node->right = inner;
    if (inner) { inner->parent = node; }
    new_node->parent = parent;
    new_node->left = node;
    node->parent = new_node;
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *rot_right(AVLNode *node) {
    AVLNode *parent = node->parent;
    AVLNode *new_node = node->left;
    AVLNode *inner = new_node->right;
    node->left = inner;
    if (inner) { inner->parent = node; }
    new_node->parent = parent;
    new_node->right = node;
    node->parent = new_node;
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

// Fix a subtree where the LEFT side is too tall (by exactly 2 - that's
// the only case that can happen from a single insert/delete).
static AVLNode *avl_fix_left(AVLNode *node) {
    if (avl_height(node->left->left) < avl_height(node->left->right)) {
        node->left = rot_left(node->left);   // turn it into the simple case
    }
    return rot_right(node);
}

static AVLNode *avl_fix_right(AVLNode *node) {
    if (avl_height(node->right->right) < avl_height(node->right->left)) {
        node->right = rot_right(node->right);
    }
    return rot_left(node);
}

// Walk from a changed node up to the root, updating stats and fixing any
// imbalance along the way. Returns the new root (rotations can change
// which node is on top).
static AVLNode *avl_fix(AVLNode *node) {
    while (true) {
        AVLNode **from = &node;
        AVLNode *parent = node->parent;
        if (parent) {
            from = (parent->left == node) ? &parent->left : &parent->right;
        }
        avl_update(node);

        uint32_t l = avl_height(node->left);
        uint32_t r = avl_height(node->right);
        if (l == r + 2) {
            *from = avl_fix_left(node);
        } else if (l + 2 == r) {
            *from = avl_fix_right(node);
        }

        if (!parent) {
            return *from;
        }
        node = parent;
    }
}

// Detach a node that has 0 or 1 children (the "easy case" - the other
// case, 2 children, works by swapping with a neighbor down to this case,
// see avl_del below).
static AVLNode *avl_del_easy(AVLNode *node) {
    AVLNode *child = node->left ? node->left : node->right;
    AVLNode *parent = node->parent;
    if (child) {
        child->parent = parent;
    }
    if (!parent) {
        return child;   // we just removed the root
    }
    AVLNode **from = (parent->left == node) ? &parent->left : &parent->right;
    *from = child;
    return avl_fix(parent);
}

static AVLNode *avl_del(AVLNode *node) {
    if (!node->left || !node->right) {
        return avl_del_easy(node);
    }
    // 2 children: find the in-order successor (leftmost node in the right
    // subtree), detach IT instead (always the easy case, since it has no
    // left child by definition), then swap it into this node's position.
    AVLNode *victim = node->right;
    while (victim->left) {
        victim = victim->left;
    }
    AVLNode *root = avl_del_easy(victim);

    *victim = *node;    // copy links/stats - victim now stands in for node
    if (victim->left) { victim->left->parent = victim; }
    if (victim->right) { victim->right->parent = victim; }

    AVLNode **from = &root;
    AVLNode *parent = node->parent;
    if (parent) {
        from = (parent->left == node) ? &parent->left : &parent->right;
    }
    *from = victim;
    return root;
}

// The payoff for maintaining `cnt` on every node: walk from `node` to
// whatever is `offset` positions away IN SORTED ORDER, in O(log N) -
// without this, "give me the next 20 items" would mean walking one at a
// time (O(offset)), which is exactly the pagination problem SQL has.
static AVLNode *avl_offset(AVLNode *node, int64_t offset) {
    int64_t pos = 0;
    while (offset != pos) {
        if (pos < offset && pos + (int64_t)avl_cnt(node->right) >= offset) {
            node = node->right;
            pos += (int64_t)avl_cnt(node->left) + 1;
        } else if (pos > offset && pos - (int64_t)avl_cnt(node->left) <= offset) {
            node = node->left;
            pos -= (int64_t)avl_cnt(node->right) + 1;
        } else {
            AVLNode *parent = node->parent;
            if (!parent) {
                return NULL;    // ran off the end of the tree
            }
            if (parent->right == node) {
                pos -= (int64_t)avl_cnt(node->left) + 1;
            } else {
                pos += (int64_t)avl_cnt(node->right) + 1;
            }
            node = parent;
        }
    }
    return node;
}

// ---------------------------------------------------------------------
// ZSet: a sorted set of (score, name) pairs, indexed TWO ways at once -
// by the AVL tree above (for sorted order and range/rank queries) and by
// a hashtable (for instant point lookup by name, same HMap type we
// already built). Both indexes point at the SAME node - this is the
// "multi-indexed intrusive data structure" idea from the book: one
// allocation, two structures running through it.
// ---------------------------------------------------------------------
typedef struct ZNode {
    AVLNode tree;
    HNode hmap;
    double score;
    size_t len;
    char name[];    // flexible array member - the name's bytes live
                     // directly after this struct, one allocation total
} ZNode;

typedef struct {
    AVLNode *root;   // sorted by (score, name)
    HMap hmap;        // indexed by name only
} ZSet;

static ZNode *znode_new(const char *name, size_t len, double score) {
    ZNode *node = malloc(sizeof(ZNode) + len);
    avl_init(&node->tree);
    node->hmap.next = NULL;
    node->hmap.hcode = fnv1a_hash((const uint8_t *)name, len);
    node->score = score;
    node->len = len;
    memcpy(node->name, name, len);
    return node;
}

// A throwaway struct for hashtable lookups by name - same trick as
// store_find()'s lookup_key: points at the caller's bytes, never inserted.
typedef struct {
    HNode node;
    const char *name;
    size_t len;
} HKey;

static bool hkey_eq(HNode *lhs, HNode *rhs) {
    ZNode *znode = container_of(lhs, ZNode, hmap);
    HKey *hkey = container_of(rhs, HKey, node);
    return znode->len == hkey->len
        && memcmp(znode->name, hkey->name, znode->len) == 0;
}

static ZNode *zset_lookup(ZSet *zset, const char *name, size_t len) {
    HKey key;
    key.node.hcode = fnv1a_hash((const uint8_t *)name, len);
    key.name = name;
    key.len = len;
    HNode *found = hm_lookup(&zset->hmap, &key.node, &hkey_eq);
    return found ? container_of(found, ZNode, hmap) : NULL;
}

// Tuple comparison: order by score first, name as a tiebreak. This is
// what makes "sorted set" meaningful when scores collide.
static bool zless(AVLNode *lhs, AVLNode *rhs) {
    ZNode *zl = container_of(lhs, ZNode, tree);
    ZNode *zr = container_of(rhs, ZNode, tree);
    if (zl->score != zr->score) {
        return zl->score < zr->score;
    }
    size_t minlen = zl->len < zr->len ? zl->len : zr->len;
    int rv = memcmp(zl->name, zr->name, minlen);
    if (rv != 0) {
        return rv < 0;
    }
    return zl->len < zr->len;
}

// Same comparison, but against an explicit (score, name) instead of
// another tree node - used by zset_seekge() to find where a query starts.
static bool zless_key(AVLNode *node, double score, const char *name, size_t len) {
    ZNode *zn = container_of(node, ZNode, tree);
    if (zn->score != score) {
        return zn->score < score;
    }
    size_t minlen = zn->len < len ? zn->len : len;
    int rv = memcmp(zn->name, name, minlen);
    if (rv != 0) {
        return rv < 0;
    }
    return zn->len < len;
}

static void tree_insert(ZSet *zset, ZNode *node) {
    AVLNode *parent = NULL;
    AVLNode **from = &zset->root;
    while (*from) {
        parent = *from;
        from = zless(&node->tree, parent) ? &parent->left : &parent->right;
    }
    *from = &node->tree;
    node->tree.parent = parent;
    zset->root = avl_fix(&node->tree);
}

static void zset_update(ZSet *zset, ZNode *node, double score) {
    if (node->score == score) {
        return;   // no change - skip the detach/reinsert entirely
    }
    zset->root = avl_del(&node->tree);
    avl_init(&node->tree);
    node->score = score;
    tree_insert(zset, node);
}

// Insert a new (score, name) pair, or update the score if that name
// already exists. Returns true if this created a brand new pair.
static bool zset_insert(ZSet *zset, const char *name, size_t len, double score) {
    ZNode *existing = zset_lookup(zset, name, len);
    if (existing) {
        zset_update(zset, existing, score);
        return false;
    }
    ZNode *node = znode_new(name, len, score);
    hm_insert(&zset->hmap, &node->hmap);
    tree_insert(zset, node);
    return true;
}

static void zset_delete(ZSet *zset, ZNode *node) {
    HKey key;
    key.node.hcode = node->hmap.hcode;
    key.name = node->name;
    key.len = node->len;
    hm_delete(&zset->hmap, &key.node, &hkey_eq);
    zset->root = avl_del(&node->tree);
    free(node);
}

// Find the first pair >= (score, name) in sorted order - the starting
// point for a range query.
static ZNode *zset_seekge(ZSet *zset, double score, const char *name, size_t len) {
    AVLNode *found = NULL;
    AVLNode *node = zset->root;
    while (node) {
        if (zless_key(node, score, name, len)) {
            node = node->right;
        } else {
            found = node;   // candidate - keep looking for something closer
            node = node->left;
        }
    }
    return found ? container_of(found, ZNode, tree) : NULL;
}

static ZNode *znode_offset(ZNode *node, int64_t offset) {
    AVLNode *tnode = node ? avl_offset(&node->tree, offset) : NULL;
    return tnode ? container_of(tnode, ZNode, tree) : NULL;
}

// Free every pair in a zset. Used when a key holding a zset gets deleted
// or overwritten with SET. Relies on hm_foreach() below being safe to use
// with a callback that frees its argument - see the h_scan fix that goes
// along with this commit.
static void zset_free_cb(HNode *node, void *arg) {
    (void)arg;
    ZNode *znode = container_of(node, ZNode, hmap);
    free(znode);
}

static void zset_clear(ZSet *zset);   // forward-declared, defined after hm_foreach below

// ---------------------------------------------------------------------
// Timers (Commit 8) - defined here, ahead of Entry, because Entry needs
// a heap_idx field and entry_set_ttl() (added further down) needs the
// heap functions below to already be declared.
//
// clock_gettime(CLOCK_MONOTONIC, ...) instead of CLOCK_REALTIME: wall-clock
// time (the actual date/time) can jump backwards or forwards if the OS
// adjusts it (NTP sync, manual change, etc), which would corrupt any
// duration math ("how long has this been idle"). Monotonic time only ever
// moves forward and isn't tied to any real-world clock - exactly what we
// want for measuring elapsed time.
// ---------------------------------------------------------------------
static uint64_t get_monotonic_msec(void) {
    struct timespec ts = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

// A circular doubly-linked list, used for idle-connection timeouts. Since
// EVERY connection gets the same timeout duration, whoever's been idle
// longest is always at the front - so this never needs to be searched or
// sorted, just "move to the back on activity, check the front for
// expiry." The dummy head node (never itself a real connection) means
// insert/detach never need a special case for an empty list.
typedef struct DList {
    struct DList *prev;
    struct DList *next;
} DList;

static void dlist_init(DList *node) {
    node->prev = node->next = node;
}

static bool dlist_empty(DList *node) {
    return node->next == node;
}

static void dlist_detach(DList *node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

static void dlist_insert_before(DList *target, DList *rookie) {
    DList *prev = target->prev;
    prev->next = rookie;
    rookie->prev = prev;
    rookie->next = target;
    target->prev = rookie;
}

// ---------------------------------------------------------------------
// Heap: array-encoded binary min-heap, used for key TTLs (EXPIRE).
//
// Unlike connection timeouts, TTLs are ARBITRARY - one key might expire
// in 2 seconds, another in 2 hours. A linked list only works when every
// timeout is the same duration; here we genuinely need "always know the
// SMALLEST value, cheaply" - that's exactly what a min-heap gives us:
// O(1) to peek the minimum, O(log N) to insert/update/remove.
// ---------------------------------------------------------------------
typedef struct {
    uint64_t val;    // the expiration timestamp (monotonic ms)
    size_t *ref;      // points back to the OWNER's record of its own heap
                        // index (Entry::heap_idx below) - see the note on
                        // heap_up/heap_down for why this indirection matters
} HeapItem;

static size_t heap_parent(size_t i) { return (i + 1) / 2 - 1; }
static size_t heap_left(size_t i) { return i * 2 + 1; }
static size_t heap_right(size_t i) { return i * 2 + 2; }

// Bubble the item at `pos` UP while it's smaller than its parent. Every
// swap updates *item.ref too - that's the whole point of storing a
// pointer back to the owner's index field instead of just a plain value:
// when items move around in the array, whoever owns them needs to find
// out, or a later heap_update()/heap_remove() would look in the wrong slot.
static void heap_up(HeapItem *a, size_t pos) {
    HeapItem t = a[pos];
    while (pos > 0 && a[heap_parent(pos)].val > t.val) {
        a[pos] = a[heap_parent(pos)];
        *a[pos].ref = pos;
        pos = heap_parent(pos);
    }
    a[pos] = t;
    *a[pos].ref = pos;
}

static void heap_down(HeapItem *a, size_t pos, size_t len) {
    HeapItem t = a[pos];
    while (true) {
        size_t l = heap_left(pos);
        size_t r = heap_right(pos);
        size_t min_pos = pos;
        uint64_t min_val = t.val;
        if (l < len && a[l].val < min_val) {
            min_pos = l;
            min_val = a[l].val;
        }
        if (r < len && a[r].val < min_val) {
            min_pos = r;
        }
        if (min_pos == pos) {
            break;
        }
        a[pos] = a[min_pos];
        *a[pos].ref = pos;
        pos = min_pos;
    }
    a[pos] = t;
    *a[pos].ref = pos;
}

static void heap_update(HeapItem *a, size_t pos, size_t len) {
    if (pos > 0 && a[heap_parent(pos)].val > a[pos].val) {
        heap_up(a, pos);
    } else {
        heap_down(a, pos, len);
    }
}

// A dynamic array of HeapItem - grows like Buffer does, capacity doubling.
typedef struct {
    HeapItem *data;
    size_t size;
    size_t cap;
} Heap;

static void heap_push(Heap *h, HeapItem item) {
    if (h->size == h->cap) {
        size_t new_cap = h->cap ? h->cap * 2 : 16;
        h->data = realloc(h->data, new_cap * sizeof(HeapItem));
        h->cap = new_cap;
    }
    h->data[h->size] = item;
    h->size++;
    heap_update(h->data, h->size - 1, h->size);
}

// Remove the item at `pos` (not necessarily the minimum - a key can have
// its TTL cancelled or updated from anywhere, not just when it expires).
// Swap-with-last-then-shrink is O(1), vs O(N) to shift everything down
// like a plain array delete would need.
static void heap_remove(Heap *h, size_t pos) {
    h->data[pos] = h->data[h->size - 1];
    h->size--;
    if (pos < h->size) {
        heap_update(h->data, pos, h->size);
    }
}

static Heap g_heap;   // zero-initialized: empty heap, no TTLs set yet

// Entry: our actual key/value pair, with an HNode embedded directly in
// it (the intrusive part). There's no separate list-node allocation -
// the hashtable's linked list literally runs through these structs.
//
// A key can now hold EITHER a plain string OR a sorted set - `type`
// says which, and only the matching fields are meaningful. Keeping both
// sets of fields unconditionally (rather than a C union) costs a little
// extra memory per entry but keeps the code simple - a deliberate
// trade-off, not an oversight.
enum {
    T_STR = 1,
    T_ZSET = 2,
};

typedef struct {
    HNode node;
    char *key;
    size_t key_len;
    uint32_t type;
    char *val;          // meaningful when type == T_STR
    size_t val_len;
    ZSet zset;           // meaningful when type == T_ZSET
    size_t heap_idx;     // this entry's position in g_heap, or SIZE_MAX if no TTL set
} Entry;

static HMap g_db;   // zero-initialized by default (static storage)

static bool entry_eq(HNode *lhs, HNode *rhs) {
    Entry *le = container_of(lhs, Entry, node);
    Entry *re = container_of(rhs, Entry, node);
    return le->key_len == re->key_len
        && memcmp(le->key, re->key, le->key_len) == 0;
}

// Returns the matching Entry, or NULL if the key isn't present.
static Entry *store_find(const uint8_t *key, size_t key_len) {
    // A throwaway Entry just for the lookup - its key POINTS AT the
    // caller's bytes rather than copying them, since it never outlives
    // this function call and is never inserted into the table.
    Entry lookup_key;
    lookup_key.key = (char *)key;
    lookup_key.key_len = key_len;
    lookup_key.node.hcode = fnv1a_hash(key, key_len);

    HNode *node = hm_lookup(&g_db, &lookup_key.node, &entry_eq);
    return node ? container_of(node, Entry, node) : NULL;
}

// ---------------------------------------------------------------------
// Thread pool (Commit 9): offload freeing a LARGE sorted set to a
// background thread, so deleting/overwriting a key with thousands of
// members doesn't stall every other connected client while we walk the
// whole thing and free each node.
//
// Producer-consumer, the standard pattern: a shared queue, a mutex
// protecting it, and a condition variable so idle worker threads sleep
// instead of busy-waiting when there's no work.
//
// IMPORTANT SAFETY PROPERTY: worker threads here NEVER touch g_db,
// g_heap, or any live Entry - they only operate on a ZSet that's already
// been fully detached (copied out by value) from the system before being
// handed off. That's what makes this safe without needing to protect our
// main data structures with locks: by the time a worker thread sees the
// data, nothing else in the program has a reference to it anymore.
// ---------------------------------------------------------------------
typedef struct WorkNode {
    void (*func)(void *);
    void *arg;
    struct WorkNode *next;
} WorkNode;

typedef struct {
    pthread_t *threads;
    size_t num_threads;
    WorkNode *head;   // FIFO queue: push at tail, pop at head
    WorkNode *tail;
    pthread_mutex_t mu;
    pthread_cond_t not_empty;
} ThreadPool;

static void *tp_worker(void *arg) {
    ThreadPool *tp = (ThreadPool *)arg;
    while (1) {
        pthread_mutex_lock(&tp->mu);
        // Always re-check the condition in a loop, not an if - see the
        // "spurious wakeups" discussion in the book. With multiple worker
        // threads, one can be woken by a signal meant for another and
        // find the queue already emptied by whoever got there first.
        while (!tp->head) {
            pthread_cond_wait(&tp->not_empty, &tp->mu);
        }
        WorkNode *node = tp->head;
        tp->head = node->next;
        if (!tp->head) {
            tp->tail = NULL;
        }
        pthread_mutex_unlock(&tp->mu);

        node->func(node->arg);   // do the actual work OUTSIDE the lock
        free(node);
    }
    return NULL;
}

static void thread_pool_init(ThreadPool *tp, size_t num_threads) {
    pthread_mutex_init(&tp->mu, NULL);
    pthread_cond_init(&tp->not_empty, NULL);
    tp->head = tp->tail = NULL;
    tp->num_threads = num_threads;
    tp->threads = malloc(num_threads * sizeof(pthread_t));
    for (size_t i = 0; i < num_threads; i++) {
        if (pthread_create(&tp->threads[i], NULL, &tp_worker, tp) != 0) {
            die("pthread_create");
        }
    }
}

static void thread_pool_queue(ThreadPool *tp, void (*func)(void *), void *arg) {
    WorkNode *node = malloc(sizeof(WorkNode));
    node->func = func;
    node->arg = arg;
    node->next = NULL;

    pthread_mutex_lock(&tp->mu);
    if (tp->tail) {
        tp->tail->next = node;
    } else {
        tp->head = node;   // queue was empty
    }
    tp->tail = node;
    pthread_cond_signal(&tp->not_empty);   // wake ONE sleeping worker
    pthread_mutex_unlock(&tp->mu);
}

static ThreadPool g_thread_pool;

#ifndef K_LARGE_CONTAINER_SIZE
#define K_LARGE_CONTAINER_SIZE 1000   // overridable at compile time for testing
#endif

// A heap-allocated box just so we have something to hand a whole ZSet
// (by value - root pointer, hashtable, everything) across to a worker
// thread as a single void* argument.
typedef struct {
    ZSet zset;
} ZSetBox;

static void zset_free_worker(void *arg) {
    ZSetBox *box = (ZSetBox *)arg;
    zset_clear(&box->zset);
    free(box);
}

// Decide whether to free this zset's contents right now (small, cheap)
// or hand it off to a worker thread (large, potentially slow). Either
// way, `*zset` is left empty/unusable after this call - the caller
// should not touch it again.
// Forward-declared: hm_size() is defined later in the file (Commit 6,
// alongside hm_foreach), but we need it here, ahead of its definition.
static size_t hm_size(HMap *hmap);

static void zset_destroy(ZSet *zset) {
    size_t size = hm_size(&zset->hmap);
    if (size > K_LARGE_CONTAINER_SIZE) {
        ZSetBox *box = malloc(sizeof(ZSetBox));
        box->zset = *zset;   // struct copy - box now owns everything zset pointed to
        thread_pool_queue(&g_thread_pool, &zset_free_worker, box);
    } else {
        zset_clear(zset);
    }
}

// Insert or overwrite a key. Always succeeds (memory allocation failure
// aside) - no more "store is full" limitation from Commit 4.
static int store_set(const uint8_t *key, size_t key_len,
                      const uint8_t *val, size_t val_len) {
    Entry *ent = store_find(key, key_len);
    if (ent) {
        // SET always overwrites, regardless of what type was there before
        // (same behavior as real Redis) - free whatever the old value was.
        if (ent->type == T_ZSET) {
            zset_destroy(&ent->zset);
        } else {
            free(ent->val);
        }
    } else {
        ent = calloc(1, sizeof(Entry));   // zeroed - zset fields start clean
        ent->key = malloc(key_len);
        memcpy(ent->key, key, key_len);
        ent->key_len = key_len;
        ent->node.hcode = fnv1a_hash(key, key_len);
        ent->node.next = NULL;
        ent->heap_idx = SIZE_MAX;   // no TTL yet - 0 would be a real heap
                                     // slot, so calloc's zeroing is NOT
                                     // safe to rely on here
        hm_insert(&g_db, &ent->node);
    }
    ent->type = T_STR;
    ent->val = malloc(val_len);
    memcpy(ent->val, val, val_len);
    ent->val_len = val_len;
    return 0;
}

// Set (ttl_ms >= 0) or cancel (ttl_ms < 0) a key's expiration timer.
// Keeps Entry::heap_idx and its slot in g_heap in sync in both
// directions - this is the ONLY place that should touch either one.
static void entry_set_ttl(Entry *ent, int64_t ttl_ms) {
    if (ttl_ms < 0) {
        if (ent->heap_idx != SIZE_MAX) {
            heap_remove(&g_heap, ent->heap_idx);
            ent->heap_idx = SIZE_MAX;
        }
        return;
    }
    uint64_t expire_at = get_monotonic_msec() + (uint64_t)ttl_ms;
    if (ent->heap_idx != SIZE_MAX) {
        // already has a TTL - just update it in place
        g_heap.data[ent->heap_idx].val = expire_at;
        heap_update(g_heap.data, ent->heap_idx, g_heap.size);
    } else {
        HeapItem item = { expire_at, &ent->heap_idx };
        heap_push(&g_heap, item);
    }
}

// Returns true if a key was found and removed.
static bool store_del(const uint8_t *key, size_t key_len) {
    Entry lookup_key;
    lookup_key.key = (char *)key;
    lookup_key.key_len = key_len;
    lookup_key.node.hcode = fnv1a_hash(key, key_len);

    HNode *node = hm_delete(&g_db, &lookup_key.node, &entry_eq);
    if (!node) {
        return false;
    }
    Entry *ent = container_of(node, Entry, node);
    entry_set_ttl(ent, -1);   // cancel any pending TTL - never leave a
                               // dangling pointer sitting in g_heap
    free(ent->key);
    if (ent->type == T_ZSET) {
        // The Entry struct itself is tiny and always freed immediately
        // below - only the POTENTIALLY HUGE zset contents might get
        // deferred to a worker thread. zset_destroy() has already fully
        // detached everything it needs by the time it returns, so it's
        // always safe to free `ent` right after, regardless of which
        // path (sync or async) it took internally.
        zset_destroy(&ent->zset);
    } else {
        free(ent->val);
    }
    free(ent);
    return true;
}

// ---------------------------------------------------------------------
// Request parsing and command handling
// ---------------------------------------------------------------------

// One length-prefixed string as parsed out of a request. `data` points
// directly into the connection's incoming buffer - no copy - which is
// safe because we always finish using it (building the response) before
// that buffer gets consumed/overwritten.
typedef struct {
    const uint8_t *data;
    uint32_t len;
} Str;

// Parse the payload of one message into a list of strings:
//   [count:4][len:4][string]...[len:4][string]
// Returns 0 on success, -1 on any malformed input.
static int32_t parse_req(const uint8_t *data, size_t size, Str *cmd, size_t *argc) {
    const uint8_t *end = data + size;

    if (data + 4 > end) {
        return -1;
    }
    uint32_t nstr = 0;
    memcpy(&nstr, data, 4);
    data += 4;
    if (nstr > k_max_args) {
        return -1;
    }

    size_t i = 0;
    while (i < nstr) {
        if (data + 4 > end) {
            return -1;
        }
        uint32_t len = 0;
        memcpy(&len, data, 4);
        data += 4;
        if (data + len > end) {
            return -1;
        }
        cmd[i].data = data;
        cmd[i].len = len;
        data += len;
        i++;
    }

    if (data != end) {
        return -1;  // trailing garbage after the last string
    }
    *argc = nstr;
    return 0;
}

// ---------------------------------------------------------------------
// Serialization (Commit 6): Tag-Length-Value encoding
//
// Commit 4-5's response was always [status:4][raw bytes] - fine for "here's
// a string" but it can't tell an integer apart from a string, and it can't
// represent a LIST of things at all. This is the same problem real binary
// formats solve: put a tag byte before each value saying what kind of
// value it is, so the reader knows how to interpret what follows -
// including, for arrays, that what follows is itself more tagged values.
// ---------------------------------------------------------------------
enum {
    TAG_NIL = 0,    // no value (e.g. GET on a missing key)
    TAG_ERR = 1,    // an error code + message
    TAG_STR = 2,    // a length-prefixed string
    TAG_INT = 3,    // a 64-bit integer
    TAG_DBL = 4,    // a double - added this commit, for ZSCORE
    TAG_ARR = 5,    // a count, followed by that many tagged values
};

static void out_nil(Buffer *out) {
    uint8_t tag = TAG_NIL;
    buf_append(out, &tag, 1);
}

static void out_str(Buffer *out, const char *s, size_t len) {
    uint8_t tag = TAG_STR;
    buf_append(out, &tag, 1);
    uint32_t l = (uint32_t)len;
    buf_append(out, (const uint8_t *)&l, 4);
    buf_append(out, (const uint8_t *)s, len);
}

static void out_int(Buffer *out, int64_t val) {
    uint8_t tag = TAG_INT;
    buf_append(out, &tag, 1);
    buf_append(out, (const uint8_t *)&val, 8);
}

static void out_dbl(Buffer *out, double val) {
    uint8_t tag = TAG_DBL;
    buf_append(out, &tag, 1);
    buf_append(out, (const uint8_t *)&val, 8);
}

static void out_err(Buffer *out, uint32_t code, const char *msg) {
    uint8_t tag = TAG_ERR;
    buf_append(out, &tag, 1);
    buf_append(out, (const uint8_t *)&code, 4);
    uint32_t mlen = (uint32_t)strlen(msg);
    buf_append(out, (const uint8_t *)&mlen, 4);
    buf_append(out, (const uint8_t *)msg, mlen);
}

// Only writes the tag + count. The caller is responsible for following
// this with exactly `n` more tagged values (out_str/out_int/etc, even
// nested out_arr calls) - the format doesn't enforce this itself, same
// as the book's version.
static void out_arr(Buffer *out, uint32_t n) {
    uint8_t tag = TAG_ARR;
    buf_append(out, &tag, 1);
    buf_append(out, (const uint8_t *)&n, 4);
}

// arr_begin/arr_end: same "reserve now, patch later" trick as
// response_begin/response_end, but for an array's count specifically.
// Needed for zquery, where we don't know how many results there'll be
// until we've actually walked the tree and hit the limit or run out.
static size_t arr_begin(Buffer *out) {
    uint8_t tag = TAG_ARR;
    buf_append(out, &tag, 1);
    size_t pos = out->size;
    uint32_t placeholder = 0;
    buf_append(out, (const uint8_t *)&placeholder, 4);
    return pos;
}

static void arr_end(Buffer *out, size_t pos, uint32_t n) {
    memcpy(out->data + pos, &n, 4);
}

// Parse a length-prefixed string argument as a number. Copies into a
// small stack buffer first since strtod/strtoll need a NUL-terminated
// C string, and our Str type is just a pointer + length (not terminated).
static bool parse_double(Str s, double *out) {
    if (s.len >= 63) {
        return false;
    }
    char buf[64];
    memcpy(buf, s.data, s.len);
    buf[s.len] = '\0';
    char *end = NULL;
    *out = strtod(buf, &end);
    return end != buf && *end == '\0';
}

static bool parse_int(Str s, int64_t *out) {
    if (s.len >= 63) {
        return false;
    }
    char buf[64];
    memcpy(buf, s.data, s.len);
    buf[s.len] = '\0';
    char *end = NULL;
    *out = strtoll(buf, &end, 10);
    return end != buf && *end == '\0';
}


// ---------------------------------------------------------------------
// hm_foreach: walk every key in the hashtable, calling `cb` on each one.
// Needed for the new `keys` command below - this is the first time we
// need to iterate the WHOLE table rather than look up one specific key.
// ---------------------------------------------------------------------
typedef void (*HScanCb)(HNode *, void *);

static void h_scan(HTab *tab, HScanCb cb, void *arg) {
    if (!tab->tab) {
        return;
    }
    for (size_t i = 0; i <= tab->mask; i++) {
        HNode *node = tab->tab[i];
        while (node) {
            // Capture `next` BEFORE calling cb(). Commit 6's version read
            // node->next as part of the for-loop's increment step, which
            // runs AFTER cb() - fine for a callback that only reads the
            // node (like cb_keys), but a use-after-free waiting to happen
            // for one that frees it (like zset_free_cb, added this commit).
            HNode *next = node->next;
            cb(node, arg);
            node = next;
        }
    }
}

// Scans BOTH tables - important during a resize, when some keys are still
// sitting in `older` waiting to be migrated. Skipping it would mean `keys`
// silently misses keys that haven't been migrated yet.
static void hm_foreach(HMap *hmap, HScanCb cb, void *arg) {
    h_scan(&hmap->newer, cb, arg);
    h_scan(&hmap->older, cb, arg);
}

static size_t hm_size(HMap *hmap) {
    return hmap->newer.size + hmap->older.size;
}

// (Defined here, not up near zset_free_cb, because it needs hm_foreach.)
static void zset_clear(ZSet *zset) {
    hm_foreach(&zset->hmap, &zset_free_cb, NULL);
    free(zset->hmap.newer.tab);
    free(zset->hmap.older.tab);
    zset->hmap.newer = (HTab){0};
    zset->hmap.older = (HTab){0};
    zset->root = NULL;
}

static bool cmd_is(Str s, const char *literal) {
    size_t n = strlen(literal);
    return s.len == n && memcmp(s.data, literal, n) == 0;
}

static void cb_keys(HNode *node, void *arg) {
    Buffer *out = (Buffer *)arg;
    Entry *ent = container_of(node, Entry, node);
    out_str(out, ent->key, ent->key_len);
}

// Note the signature change from Commit 5: instead of filling in a
// Response struct and having a separate make_response() step, we write
// directly into the output buffer as we go. This is necessary for `keys`
// - an array's size isn't known as one fixed struct, it's produced by
// writing values one at a time as we walk the hashtable.
static void do_request(Str *cmd, size_t argc, Buffer *out) {
    if (argc == 2 && cmd_is(cmd[0], "get")) {
        Entry *ent = store_find(cmd[1].data, cmd[1].len);
        if (!ent) {
            out_nil(out);
            return;
        }
        if (ent->type != T_STR) {
            out_err(out, 2, "WRONGTYPE key holds a different kind of value");
            return;
        }
        out_str(out, ent->val, ent->val_len);

    } else if (argc == 3 && cmd_is(cmd[0], "set")) {
        store_set(cmd[1].data, cmd[1].len, cmd[2].data, cmd[2].len);
        out_nil(out);   // real Redis replies "OK" here; we keep it simple

    } else if (argc == 2 && cmd_is(cmd[0], "del")) {
        bool found = store_del(cmd[1].data, cmd[1].len);
        out_int(out, found ? 1 : 0);

    } else if (argc == 1 && cmd_is(cmd[0], "keys")) {
        out_arr(out, (uint32_t)hm_size(&g_db));
        hm_foreach(&g_db, &cb_keys, out);

    } else if (argc == 3 && cmd_is(cmd[0], "expire")) {
        // expire key <ttl_ms>
        int64_t ttl_ms;
        if (!parse_int(cmd[2], &ttl_ms)) {
            out_err(out, 1, "expected an integer number of milliseconds");
            return;
        }
        Entry *ent = store_find(cmd[1].data, cmd[1].len);
        if (!ent) {
            out_int(out, 0);   // real Redis convention: 0 = key didn't exist
            return;
        }
        entry_set_ttl(ent, ttl_ms);
        out_int(out, 1);

    } else if (argc == 2 && cmd_is(cmd[0], "ttl")) {
        Entry *ent = store_find(cmd[1].data, cmd[1].len);
        if (!ent) {
            out_int(out, -2);   // real Redis convention: -2 = key doesn't exist
            return;
        }
        if (ent->heap_idx == SIZE_MAX) {
            out_int(out, -1);   // -1 = key exists but has no TTL
            return;
        }
        uint64_t now_ms = get_monotonic_msec();
        uint64_t expire_at = g_heap.data[ent->heap_idx].val;
        int64_t remaining = (expire_at > now_ms) ? (int64_t)(expire_at - now_ms) : 0;
        out_int(out, remaining);

    } else if (argc == 2 && cmd_is(cmd[0], "persist")) {
        Entry *ent = store_find(cmd[1].data, cmd[1].len);
        if (!ent || ent->heap_idx == SIZE_MAX) {
            out_int(out, 0);   // nothing to remove
            return;
        }
        entry_set_ttl(ent, -1);
        out_int(out, 1);

    } else if (argc == 4 && cmd_is(cmd[0], "zadd")) {
        // zadd key score name
        double score;
        if (!parse_double(cmd[2], &score)) {
            out_err(out, 1, "expected a valid number for score");
            return;
        }
        Entry *ent = store_find(cmd[1].data, cmd[1].len);
        if (!ent) {
            ent = calloc(1, sizeof(Entry));
            ent->key = malloc(cmd[1].len);
            memcpy(ent->key, cmd[1].data, cmd[1].len);
            ent->key_len = cmd[1].len;
            ent->node.hcode = fnv1a_hash(cmd[1].data, cmd[1].len);
            ent->type = T_ZSET;
            ent->heap_idx = SIZE_MAX;   // no TTL yet - same reasoning as store_set
            hm_insert(&g_db, &ent->node);
        } else if (ent->type != T_ZSET) {
            out_err(out, 2, "WRONGTYPE key holds a different kind of value");
            return;
        }
        bool added = zset_insert(&ent->zset, (const char *)cmd[3].data, cmd[3].len, score);
        out_int(out, added ? 1 : 0);

    } else if (argc == 3 && cmd_is(cmd[0], "zscore")) {
        Entry *ent = store_find(cmd[1].data, cmd[1].len);
        if (!ent || ent->type != T_ZSET) {
            out_nil(out);
            return;
        }
        ZNode *znode = zset_lookup(&ent->zset, (const char *)cmd[2].data, cmd[2].len);
        if (!znode) {
            out_nil(out);
            return;
        }
        out_dbl(out, znode->score);

    } else if (argc == 3 && cmd_is(cmd[0], "zrem")) {
        Entry *ent = store_find(cmd[1].data, cmd[1].len);
        if (!ent || ent->type != T_ZSET) {
            out_int(out, 0);
            return;
        }
        ZNode *znode = zset_lookup(&ent->zset, (const char *)cmd[2].data, cmd[2].len);
        if (!znode) {
            out_int(out, 0);
            return;
        }
        zset_delete(&ent->zset, znode);
        out_int(out, 1);

    } else if (argc == 6 && cmd_is(cmd[0], "zquery")) {
        // zquery key score name offset limit
        // Returns pairs >= (score, name) in sorted order, skipping the
        // first `offset` of them, up to `limit` pairs total.
        double score;
        int64_t offset = 0, limit = 0;
        if (!parse_double(cmd[2], &score)
            || !parse_int(cmd[4], &offset)
            || !parse_int(cmd[5], &limit)) {
            out_err(out, 1, "invalid score/offset/limit");
            return;
        }
        Entry *ent = store_find(cmd[1].data, cmd[1].len);
        if (!ent || ent->type != T_ZSET) {
            out_arr(out, 0);
            return;
        }

        ZNode *znode = zset_seekge(&ent->zset, score, (const char *)cmd[3].data, cmd[3].len);
        znode = znode_offset(znode, offset);

        size_t hdr = arr_begin(out);
        uint32_t n = 0;
        int64_t pairs = 0;
        while (znode && pairs < limit) {
            out_str(out, znode->name, znode->len);
            out_dbl(out, znode->score);
            znode = znode_offset(znode, 1);
            n += 2;   // each pair contributes 2 array entries: name, score
            pairs++;
        }
        arr_end(out, hdr, n);

    } else {
        out_err(out, 1, "unrecognized command or wrong number of arguments");
    }
}

// Reserve 4 bytes at the current position for the outer message length,
// which we don't know yet - do_request() is about to write a variable
// amount of data. We come back and fill this in once we know the total
// (see response_end below). Returns the position of the reserved header,
// so the caller can pass it back.
static size_t response_begin(Buffer *out) {
    size_t header_pos = out->size;
    uint32_t placeholder = 0;
    buf_append(out, (const uint8_t *)&placeholder, 4);
    return header_pos;
}

static void response_end(Buffer *out, size_t header_pos) {
    uint32_t msg_len = (uint32_t)(out->size - header_pos - 4);
    if (msg_len > k_max_msg) {
        // Whatever we wrote was too big - throw it away and write a
        // proper error instead, so we never send an oversized message.
        out->size = header_pos + 4;
        out_err(out, 2, "response too big");
        msg_len = (uint32_t)(out->size - header_pos - 4);
    }
    memcpy(out->data + header_pos, &msg_len, 4);
}

// ---------------------------------------------------------------------
// Conn: everything the event loop needs to remember about one client,
// carried across loop iterations (unlike Commit 2's local `rbuf`, which
// only lived for the duration of one request).
// ---------------------------------------------------------------------
typedef struct Conn {
    int fd;
    bool want_read;
    bool want_write;
    bool want_close;
    Buffer incoming;   // bytes read from the socket, not yet fully parsed
    Buffer outgoing;   // bytes generated by us, waiting to be written
    uint64_t last_active_ms;   // for idle-connection timeout
    DList idle_node;            // this connection's spot in the idle list
} Conn;

// Dummy head of the idle-connection list - see the DList comment above
// for why a dummy node avoids special-casing an empty list. Initialized
// in main() via dlist_init() before the event loop starts.
static DList g_idle_list;

// fd -> Conn* lookup table. On Linux, fds are allocated as the smallest
// available non-negative integer, so a flat array indexed by fd is dense
// and simple - no hashtable needed here.
static Conn **g_fd2conn = NULL;
static size_t g_fd2conn_cap = 0;

static void fd2conn_put(int fd, Conn *conn) {
    if ((size_t)fd >= g_fd2conn_cap) {
        size_t new_cap = g_fd2conn_cap ? g_fd2conn_cap * 2 : 8;
        while (new_cap <= (size_t)fd) {
            new_cap *= 2;
        }
        Conn **new_arr = realloc(g_fd2conn, new_cap * sizeof(Conn *));
        if (!new_arr) {
            die("realloc() in fd2conn_put");
        }
        for (size_t i = g_fd2conn_cap; i < new_cap; i++) {
            new_arr[i] = NULL;
        }
        g_fd2conn = new_arr;
        g_fd2conn_cap = new_cap;
    }
    g_fd2conn[fd] = conn;
}

static void fd_set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        die("fcntl(F_GETFL)");
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        die("fcntl(F_SETFL)");
    }
}

// Accept one pending connection (non-blocking) and set up its Conn state.
static Conn *handle_accept(int listen_fd) {
    struct sockaddr_in client_addr = {0};
    socklen_t addrlen = sizeof(client_addr);
    int conn_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addrlen);
    if (conn_fd < 0) {
        return NULL;   // not a fatal error - just nothing to accept right now
    }

    fd_set_nonblocking(conn_fd);

    Conn *conn = calloc(1, sizeof(Conn));
    conn->fd = conn_fd;
    conn->want_read = true;    // we want to read their first request
    buf_init(&conn->incoming);
    buf_init(&conn->outgoing);

    conn->last_active_ms = get_monotonic_msec();
    dlist_insert_before(&g_idle_list, &conn->idle_node);

    printf("new connection, fd=%d\n", conn_fd);
    return conn;
}

// Try to parse ONE complete [len][payload] message out of conn->incoming.
// Returns true if it found and processed one (caller should call again -
// there may be more, e.g. if the client sent several messages back to
// back and they all arrived in one read()). Returns false if there isn't
// enough data yet - that's normal, not an error.
static bool try_one_request(Conn *conn) {
    if (conn->incoming.size < 4) {
        return false;   // don't even have the length header yet
    }

    uint32_t len = 0;
    memcpy(&len, conn->incoming.data, 4);
    if (len > k_max_msg) {
        fprintf(stderr, "message too long, closing fd=%d\n", conn->fd);
        conn->want_close = true;
        return false;
    }

    if (4 + len > conn->incoming.size) {
        return false;   // header's here, but the body hasn't fully arrived
    }

    const uint8_t *payload = conn->incoming.data + 4;

    Str cmd[k_max_args];
    size_t argc = 0;

    size_t header_pos = response_begin(&conn->outgoing);
    if (parse_req(payload, len, cmd, &argc) != 0) {
        out_err(&conn->outgoing, 1, "bad request");
    } else {
        do_request(cmd, argc, &conn->outgoing);
    }
    response_end(&conn->outgoing, header_pos);

    buf_consume(&conn->incoming, 4 + len);
    return true;
}

static void handle_read(Conn *conn) {
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));

    if (rv < 0 && errno == EAGAIN) {
        return;     // spurious wakeup, nothing actually ready - not an error
    }
    if (rv < 0) {
        conn->want_close = true;
        return;
    }
    if (rv == 0) {
        conn->want_close = true;   // client closed their end (EOF)
        return;
    }

    buf_append(&conn->incoming, buf, (size_t)rv);

    // Keep parsing as long as there's a full message sitting in the
    // buffer - a single read() can contain more than one message if the
    // client sent them back to back.
    while (try_one_request(conn)) {
    }

    // Switch to writing if we generated a response.
    if (conn->outgoing.size > 0) {
        conn->want_read = false;
        conn->want_write = true;
    }
}

static void handle_write(Conn *conn) {
    ssize_t rv = write(conn->fd, conn->outgoing.data, conn->outgoing.size);

    if (rv < 0 && errno == EAGAIN) {
        return;     // socket's write buffer is full right now, try later
    }
    if (rv < 0) {
        conn->want_close = true;
        return;
    }

    buf_consume(&conn->outgoing, (size_t)rv);

    if (conn->outgoing.size == 0) {
        // fully sent - go back to waiting for their next request
        conn->want_read = true;
        conn->want_write = false;
    }
    // else: partial write, stay in want_write mode, poll() will tell us
    // again next time the socket has room.
}

static void destroy_conn(Conn *conn) {
    close(conn->fd);
    g_fd2conn[conn->fd] = NULL;
    dlist_detach(&conn->idle_node);   // remove from the idle-timeout list too
    buf_free(&conn->incoming);
    buf_free(&conn->outgoing);
    printf("closed connection, fd=%d\n", conn->fd);
    free(conn);
}

#ifndef K_IDLE_TIMEOUT_MS
#define K_IDLE_TIMEOUT_MS 30000   // 30s - overridable at compile time for testing
#endif

// How long poll() should wait: either until a socket is ready, or until
// the NEAREST timer (idle timeout or key TTL) is due, whichever is
// sooner. This is why poll()'s timeout is no longer a hardcoded -1
// (block forever) - a timer needs the loop to wake up even when nothing
// on the network has happened.
static int32_t next_timer_ms(void) {
    uint64_t now_ms = get_monotonic_msec();
    uint64_t next_ms = UINT64_MAX;

    if (!dlist_empty(&g_idle_list)) {
        Conn *conn = container_of(g_idle_list.next, Conn, idle_node);
        next_ms = conn->last_active_ms + K_IDLE_TIMEOUT_MS;
    }
    if (g_heap.size > 0 && g_heap.data[0].val < next_ms) {
        next_ms = g_heap.data[0].val;
    }

    if (next_ms == UINT64_MAX) {
        return -1;   // no timers at all - poll() can block indefinitely
    }
    if (next_ms <= now_ms) {
        return 0;    // already due - don't wait, handle it immediately
    }
    return (int32_t)(next_ms - now_ms);
}

// Called once per event loop iteration: close any connections that have
// been idle too long, and delete any keys whose TTL has expired.
static void process_timers(void) {
    uint64_t now_ms = get_monotonic_msec();

    // Idle connections are sorted by nature (see the DList comment
    // earlier) - the front of the list is always the one waiting longest,
    // so we just keep popping from the front until we hit one that's NOT
    // expired yet.
    while (!dlist_empty(&g_idle_list)) {
        Conn *conn = container_of(g_idle_list.next, Conn, idle_node);
        uint64_t deadline = conn->last_active_ms + K_IDLE_TIMEOUT_MS;
        if (deadline >= now_ms) {
            break;   // this one (and everyone after it) is still fine
        }
        fprintf(stderr, "idle timeout, closing fd=%d\n", conn->fd);
        destroy_conn(conn);
    }

    // TTL expirations, capped per iteration - same "don't freeze the
    // event loop" principle as progressive hashtable resizing. If a huge
    // batch of keys expire at once, we'd rather spread that work across
    // several loop iterations (interleaved with normal client IO) than
    // stall every connected client while we delete them all in one go.
    const size_t k_max_expirations_per_tick = 2000;
    size_t nworks = 0;
    while (g_heap.size > 0 && g_heap.data[0].val < now_ms
           && nworks < k_max_expirations_per_tick) {
        Entry *ent = container_of(g_heap.data[0].ref, Entry, heap_idx);

        // Copy the key before calling store_del(): store_del() frees
        // ent->key as part of deleting the entry, and relying on the
        // exact internal ordering of store_del() to make passing
        // ent->key directly "safe" would be a fragile assumption that
        // could break silently if store_del() is ever refactored. A
        // few bytes of copying is cheap insurance against that.
        size_t klen = ent->key_len;
        char *key_copy = malloc(klen);
        memcpy(key_copy, ent->key, klen);
        store_del((const uint8_t *)key_copy, klen);
        free(key_copy);

        nworks++;
    }
}

int main(void) {
    dlist_init(&g_idle_list);

    // 4 worker threads is a reasonable default - enough to keep multiple
    // large deletes from queueing up behind each other, without spawning
    // more threads than this workload could ever actually use at once.
    thread_pool_init(&g_thread_pool, 4);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        die("socket()");
    }

    int val = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(0);

    if (bind(listen_fd, (const struct sockaddr *)&addr, sizeof(addr)) != 0) {
        die("bind()");
    }
    if (listen(listen_fd, SOMAXCONN) != 0) {
        die("listen()");
    }
    fd_set_nonblocking(listen_fd);

    printf("listening on 0.0.0.0:1234 (event loop mode) ...\n");

    while (1) {
        // Step 1: count how many live connections we have, so we know how
        // big to make the pollfd array this iteration.
        size_t n_conns = 0;
        for (size_t i = 0; i < g_fd2conn_cap; i++) {
            if (g_fd2conn[i]) {
                n_conns++;
            }
        }

        struct pollfd *poll_args = malloc((n_conns + 1) * sizeof(struct pollfd));
        if (!poll_args) {
            die("malloc() for poll_args");
        }

        // The listening socket always goes first, and we always want to
        // know if a new client is trying to connect (POLLIN).
        poll_args[0].fd = listen_fd;
        poll_args[0].events = POLLIN;
        poll_args[0].revents = 0;

        size_t idx = 1;
        for (size_t i = 0; i < g_fd2conn_cap; i++) {
            Conn *conn = g_fd2conn[i];
            if (!conn) {
                continue;
            }
            struct pollfd pfd = {0};
            pfd.fd = conn->fd;
            pfd.events = POLLERR;   // always want to know about errors
            if (conn->want_read) {
                pfd.events |= POLLIN;
            }
            if (conn->want_write) {
                pfd.events |= POLLOUT;
            }
            poll_args[idx++] = pfd;
        }

        // Step 2: THE only blocking call in this whole program. Waits
        // until at least one socket in the list is ready, OR until the
        // nearest timer is due - whichever comes first.
        int32_t timeout_ms = next_timer_ms();
        int rv = poll(poll_args, (nfds_t)idx, timeout_ms);
        if (rv < 0) {
            free(poll_args);
            if (errno == EINTR) {
                continue;   // interrupted by a signal, not a real error
            }
            die("poll()");
        }

        // Step 3: accept a new connection if one's waiting.
        if (poll_args[0].revents) {
            Conn *conn = handle_accept(listen_fd);
            if (conn) {
                fd2conn_put(conn->fd, conn);
            }
        }

        // Step 4: service whichever client sockets are ready.
        for (size_t i = 1; i < idx; i++) {
            uint32_t ready = poll_args[i].revents;
            if (ready == 0) {
                continue;
            }

            Conn *conn = g_fd2conn[poll_args[i].fd];
            if (!conn) {
                continue;
            }

            // Any activity resets this connection's idle clock - move it
            // to the back of the line, since it's now the LEAST likely
            // to be the next one to time out.
            conn->last_active_ms = get_monotonic_msec();
            dlist_detach(&conn->idle_node);
            dlist_insert_before(&g_idle_list, &conn->idle_node);

            if (ready & POLLIN) {
                handle_read(conn);
            }
            if (!conn->want_close && conn->want_write && (ready & POLLOUT)) {
                handle_write(conn);
            }

            if ((ready & POLLERR) || conn->want_close) {
                destroy_conn(conn);
            }
        }

        // Step 5: handle any timers that came due - idle connections to
        // close, keys to expire. Runs every iteration, not just when
        // poll() timed out - a timer can become due at the same moment
        // some unrelated socket activity woke us up anyway.
        process_timers();

        free(poll_args);
    }

    return 0;
}
