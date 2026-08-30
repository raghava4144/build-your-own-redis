// client.c
//
// Sends several requests over ONE connection, using the [4-byte
// length][payload] protocol. Blocking client - the server is what got the
// event loop upgrade in Commit 3, the client doesn't need one for our tests.

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

static int32_t query(int fd, const char *text) {
    uint32_t len = (uint32_t)strlen(text);
    if (len > k_max_msg) {
        return -1;
    }

    char wbuf[4 + k_max_msg];
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], text, len);
    if (write_all(fd, wbuf, 4 + len) != 0) {
        return -1;
    }

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

    if (read_full(fd, &rbuf[4], reply_len) != 0) {
        msg("read() error while reading reply body");
        return -1;
    }

    printf("server says: %.*s\n", (int)reply_len, &rbuf[4]);
    return 0;
}

int main(int argc, char **argv) {
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

    // Allow passing a custom "tag" via argv so we can tell apart the output
    // of two clients running at the same time (used for testing Commit 3).
    const char *tag = (argc > 1) ? argv[1] : "";

    char messages[3][64];
    snprintf(messages[0], sizeof(messages[0]), "%shello1", tag);
    snprintf(messages[1], sizeof(messages[1]), "%shello2", tag);
    snprintf(messages[2], sizeof(messages[2]), "%shello3", tag);

    for (size_t i = 0; i < 3; i++) {
        if (query(fd, messages[i]) != 0) {
            break;
        }
    }

    close(fd);
    return 0;
}
