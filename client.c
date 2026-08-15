// client.c
//
// Commit 2: send several requests over ONE connection, using the same
// [4-byte length][payload] protocol as the server.
//
// This proves the server's new loop actually works - Commit 1's server
// would've only handled the first message and ignored the rest.

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

const size_t k_max_msg = 4096;

static int32_t read_full(int fd, char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) {
            return -1;
        }
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

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

// Send one request, wait for the matching reply, print it.
static int32_t query(int fd, const char *text) {
    uint32_t len = (uint32_t)strlen(text);
    if (len > k_max_msg) {
        return -1;
    }

    // build and send [len][payload]
    char wbuf[4 + k_max_msg];
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], text, len);
    if (write_all(fd, wbuf, 4 + len) != 0) {
        return -1;
    }

    // read the reply header
    char rbuf[4 + k_max_msg];
    errno = 0;
    if (read_full(fd, rbuf, 4) != 0) {
        msg(errno == 0 ? "server closed connection (EOF)" : "read() error");
        return -1;
    }

    uint32_t reply_len = 0;
    memcpy(&reply_len, rbuf, 4);
    if (reply_len > k_max_msg) {
        msg("reply too long");
        return -1;
    }

    // read the reply body
    if (read_full(fd, &rbuf[4], reply_len) != 0) {
        msg("read() error while reading reply body");
        return -1;
    }

    printf("server says: %.*s\n", (int)reply_len, &rbuf[4]);
    return 0;
}

int main(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(fd, (const struct sockaddr *)&addr, sizeof(addr)) != 0) {
        die("connect()");
    }

    // Three separate requests, ONE connection. Commit 1 could not have
    // done this - it only ever handled a single exchange.
    const char *messages[] = {"hello1", "hello2", "hello3"};
    for (size_t i = 0; i < sizeof(messages) / sizeof(messages[0]); i++) {
        if (query(fd, messages[i]) != 0) {
            break;
        }
    }

    close(fd);
    return 0;
}
