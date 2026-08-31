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

// NOTE: this one has to be a real preprocessor #define, not `const size_t`
// like the others above. In C (unlike C++), a file-scope `const` is not a
// true compile-time constant, so it can't be used to size a global array
// like g_store below - the compiler rejects it as "variably modified at
// file scope". #define sidesteps this because it's a textual substitution
// that happens before compilation even starts.
#define K_MAX_ENTRIES 1024

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
// The data store (Commit 4 placeholder version)
//
// This is a deliberately dumb, linear-scan key/value table. Every get/set/
// del walks the whole array comparing keys one at a time - O(N) per
// operation. That's fine for a handful of keys, and it's genuinely useful
// as a first version: it's easy to verify by inspection that it's correct.
// The NEXT commit replaces this with a real hashtable, and you'll be able
// to directly feel why that matters once you've got more than a few dozen
// keys in here. Keeping this version around in git history is the point -
// it's a real before/after for a resume story about Big-O in practice.
// ---------------------------------------------------------------------
typedef struct {
    bool used;
    char *key;
    size_t key_len;
    char *val;
    size_t val_len;
} Entry;

static Entry g_store[K_MAX_ENTRIES];

// Returns the index of the entry with this key, or -1 if not found.
static int store_find(const uint8_t *key, size_t key_len) {
    for (size_t i = 0; i < K_MAX_ENTRIES; i++) {
        if (g_store[i].used
            && g_store[i].key_len == key_len
            && memcmp(g_store[i].key, key, key_len) == 0) {
            return (int)i;
        }
    }
    return -1;
}

// Insert or overwrite a key. Returns 0 on success, -1 if the store is full
// and this is a brand new key (updates to existing keys always succeed).
static int store_set(const uint8_t *key, size_t key_len,
                      const uint8_t *val, size_t val_len) {
    int idx = store_find(key, key_len);
    if (idx < 0) {
        // find an empty slot for a new key
        for (size_t i = 0; i < K_MAX_ENTRIES; i++) {
            if (!g_store[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            return -1;  // store is full - a real limitation of this version
        }
        g_store[idx].used = true;
        g_store[idx].key = malloc(key_len);
        memcpy(g_store[idx].key, key, key_len);
        g_store[idx].key_len = key_len;
        g_store[idx].val = NULL;
    } else {
        free(g_store[idx].val);    // overwriting - release the old value
    }
    g_store[idx].val = malloc(val_len);
    memcpy(g_store[idx].val, val, val_len);
    g_store[idx].val_len = val_len;
    return 0;
}

// Returns true if a key was found and removed.
static bool store_del(const uint8_t *key, size_t key_len) {
    int idx = store_find(key, key_len);
    if (idx < 0) {
        return false;
    }
    free(g_store[idx].key);
    free(g_store[idx].val);
    g_store[idx].used = false;
    g_store[idx].key = NULL;
    g_store[idx].val = NULL;
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

enum {
    RES_OK = 0,     // success, `data` (if any) is the result
    RES_ERR = 1,    // unrecognized command or bad arguments
    RES_NX = 2,     // key not found
};

typedef struct {
    uint32_t status;
    const uint8_t *data;
    size_t data_len;
} Response;

static bool cmd_is(Str s, const char *literal) {
    size_t n = strlen(literal);
    return s.len == n && memcmp(s.data, literal, n) == 0;
}

static void do_request(Str *cmd, size_t argc, Response *out) {
    out->data = NULL;
    out->data_len = 0;

    if (argc == 2 && cmd_is(cmd[0], "get")) {
        int idx = store_find(cmd[1].data, cmd[1].len);
        if (idx < 0) {
            out->status = RES_NX;
            return;
        }
        out->status = RES_OK;
        out->data = (const uint8_t *)g_store[idx].val;
        out->data_len = g_store[idx].val_len;

    } else if (argc == 3 && cmd_is(cmd[0], "set")) {
        if (store_set(cmd[1].data, cmd[1].len, cmd[2].data, cmd[2].len) != 0) {
            out->status = RES_ERR;
            return;
        }
        out->status = RES_OK;

    } else if (argc == 2 && cmd_is(cmd[0], "del")) {
        bool found = store_del(cmd[1].data, cmd[1].len);
        out->status = RES_OK;
        static const char one[] = "1";
        static const char zero[] = "0";
        out->data = (const uint8_t *)(found ? one : zero);
        out->data_len = 1;

    } else {
        out->status = RES_ERR;
    }
}

static void make_response(const Response *resp, Buffer *out) {
    uint32_t body_len = 4 + (uint32_t)resp->data_len;   // 4 bytes for status
    buf_append(out, (const uint8_t *)&body_len, 4);
    buf_append(out, (const uint8_t *)&resp->status, 4);
    if (resp->data_len > 0) {
        buf_append(out, resp->data, resp->data_len);
    }
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
    Response resp;

    if (parse_req(payload, len, cmd, &argc) != 0) {
        resp.status = RES_ERR;
        resp.data = NULL;
        resp.data_len = 0;
    } else {
        do_request(cmd, argc, &resp);
    }

    make_response(&resp, &conn->outgoing);

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
