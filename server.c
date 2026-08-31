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

// Entry: our actual key/value pair, with an HNode embedded directly in
// it (the intrusive part). There's no separate list-node allocation -
// the hashtable's linked list literally runs through these structs.
typedef struct {
    HNode node;
    char *key;
    size_t key_len;
    char *val;
    size_t val_len;
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

// Insert or overwrite a key. Always succeeds (memory allocation failure
// aside) - no more "store is full" limitation from Commit 4.
static int store_set(const uint8_t *key, size_t key_len,
                      const uint8_t *val, size_t val_len) {
    Entry *ent = store_find(key, key_len);
    if (ent) {
        free(ent->val);     // overwriting - release the old value
    } else {
        ent = malloc(sizeof(Entry));
        ent->key = malloc(key_len);
        memcpy(ent->key, key, key_len);
        ent->key_len = key_len;
        ent->node.hcode = fnv1a_hash(key, key_len);
        ent->node.next = NULL;
        hm_insert(&g_db, &ent->node);
    }
    ent->val = malloc(val_len);
    memcpy(ent->val, val, val_len);
    ent->val_len = val_len;
    return 0;
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
    free(ent->key);
    free(ent->val);
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
    TAG_ARR = 4,    // a count, followed by that many tagged values
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
        for (HNode *node = tab->tab[i]; node != NULL; node = node->next) {
            cb(node, arg);
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
} Conn;

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
    buf_free(&conn->incoming);
    buf_free(&conn->outgoing);
    printf("closed connection, fd=%d\n", conn->fd);
    free(conn);
}

int main(void) {
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
        // until at least one socket in the list is ready, however many
        // clients that might be.
        int rv = poll(poll_args, (nfds_t)idx, -1);
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

        free(poll_args);
    }

    return 0;
}
