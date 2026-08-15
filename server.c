// server.c
//
// Commit 2: a real request-response protocol.
//
// Commit 1's server did exactly one read() and assumed it captured the
// client's whole message. That's only true by luck for tiny messages.
// TCP gives you a raw stream of bytes with NO built-in message boundaries -
// a single read() can return less than what the client sent (arrives in
// pieces), or more than one message glued together.
//
// The fix: we define our own tiny protocol. Every message is:
//
//     [ 4-byte length (little-endian) ][ payload, that many bytes ]
//
// To read one full message we now do two "keep going until we truly have
// everything" loops: read_full() for the 4-byte header, then again for the
// payload. Same idea on the write side with write_all() - this is also what
// properly checks write()'s return value, fixing the warning from Commit 1.
//
// The server can now handle MULTIPLE messages on one connection, looping
// until the client disconnects, instead of handling exactly one and quitting.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

static void die(const char *m) {
    perror(m);
    exit(1);
}

static void msg(const char *m) {
    fprintf(stderr, "%s\n", m);
}

// The largest message body we'll accept. Anything claiming to be bigger is
// treated as a protocol error (or an attack) and we drop the connection.
const size_t k_max_msg = 4096;

// Keep calling read() until we have exactly `n` bytes, or something goes
// wrong. Returns 0 on success, -1 on error/unexpected EOF.
static int32_t read_full(int fd, char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) {
            return -1;      // error, or the peer closed the connection early
        }
        // rv is guaranteed <= n here because we only ever asked for n bytes.
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

// Same idea for writing: keep calling write() until all `n` bytes are sent.
static int32_t write_all(int fd, const char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0) {
            return -1;
        }
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

// Handle exactly one request on this connection: read a length-prefixed
// message, print it, send a length-prefixed reply.
// Returns 0 on success, -1 if the connection should be closed (error or EOF).
static int32_t one_request(int conn_fd) {
    char rbuf[4 + k_max_msg];

    // Step 1: read the 4-byte length header.
    errno = 0;
    if (read_full(conn_fd, rbuf, 4) != 0) {
        if (errno == 0) {
            msg("client closed connection (EOF)");
        } else {
            msg("read() error");
        }
        return -1;
    }

    uint32_t len = 0;
    memcpy(&len, rbuf, 4);         // assumes little-endian, true on x86/ARM
    if (len > k_max_msg) {
        msg("message too long, dropping connection");
        return -1;
    }

    // Step 2: read exactly `len` more bytes - the actual message body.
    if (read_full(conn_fd, &rbuf[4], len) != 0) {
        msg("read() error while reading message body");
        return -1;
    }

    printf("client says: %.*s\n", (int)len, &rbuf[4]);

    // Step 3: reply using the same [len][payload] format.
    const char reply[] = "world";
    uint32_t reply_len = (uint32_t)strlen(reply);

    char wbuf[4 + sizeof(reply)];
    memcpy(wbuf, &reply_len, 4);
    memcpy(&wbuf[4], reply, reply_len);

    return write_all(conn_fd, wbuf, 4 + reply_len);
}

int main(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(0);

    if (bind(fd, (const struct sockaddr *)&addr, sizeof(addr)) != 0) {
        die("bind()");
    }
    if (listen(fd, SOMAXCONN) != 0) {
        die("listen()");
    }

    printf("listening on 0.0.0.0:1234 ...\n");

    while (1) {
        struct sockaddr_in client_addr = {0};
        socklen_t addrlen = sizeof(client_addr);
        int conn_fd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
        if (conn_fd < 0) {
            continue;
        }

        // NEW: loop, handling as many requests as the client sends on this
        // one connection, instead of just handling one and moving on.
        while (1) {
            int32_t err = one_request(conn_fd);
            if (err) {
                break;
            }
        }
        close(conn_fd);
    }

    return 0;
}
