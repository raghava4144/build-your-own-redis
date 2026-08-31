// client.c
//
// Commit 4: a real command-line client.
//
//   ./client set name raghava
//   ./client get name
//   ./client del name
//
// Instead of sending one flat string, we now send a properly structured
// command: a count, followed by each argument as its own length-prefixed
// string. This mirrors how you'd type a command in a terminal - a command
// word plus some arguments - and it's what lets the server tell "set" from
// "name" from "raghava" instead of getting one blob of bytes.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
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

// Response status codes - must match the server's enum in server.c.
enum {
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2,
};

// Send one command (argv-style: an array of strings) as:
//   [outer len:4][count:4][len:4][string]...[len:4][string]
// then read and print the server's [status][data] reply.
static int32_t send_command(int fd, char **args, int argc) {
    char wbuf[4 + k_max_msg];
    size_t pos = 4;   // leave room for the outer length, filled in later

    uint32_t nstr = (uint32_t)argc;
    memcpy(&wbuf[pos], &nstr, 4);
    pos += 4;

    for (int i = 0; i < argc; i++) {
        uint32_t len = (uint32_t)strlen(args[i]);
        if (pos + 4 + len > sizeof(wbuf)) {
            msg("command too long");
            return -1;
        }
        memcpy(&wbuf[pos], &len, 4);
        pos += 4;
        memcpy(&wbuf[pos], args[i], len);
        pos += len;
    }

    uint32_t outer_len = (uint32_t)(pos - 4);
    memcpy(wbuf, &outer_len, 4);

    if (write_all(fd, wbuf, pos) != 0) {
        return -1;
    }

    // read the reply: [outer len:4][status:4][data...]
    char rbuf[4 + k_max_msg];
    errno = 0;
    if (read_full(fd, rbuf, 4) != 0) {
        msg(errno == 0 ? "server closed connection (EOF)" : "read() error");
        return -1;
    }

    uint32_t reply_len = 0;
    memcpy(&reply_len, rbuf, 4);
    if (reply_len < 4 || reply_len > k_max_msg) {
        msg("bad reply length");
        return -1;
    }

    if (read_full(fd, &rbuf[4], reply_len) != 0) {
        msg("read() error while reading reply body");
        return -1;
    }

    uint32_t status = 0;
    memcpy(&status, &rbuf[4], 4);
    const char *data = &rbuf[8];
    size_t data_len = reply_len - 4;

    switch (status) {
    case RES_OK:
        if (data_len > 0) {
            printf("%.*s\n", (int)data_len, data);
        } else {
            printf("OK\n");
        }
        break;
    case RES_NX:
        printf("(nil)\n");
        break;
    case RES_ERR:
    default:
        printf("(error) unrecognized command or wrong number of arguments\n");
        break;
    }

    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <command> [args...]\n", argv[0]);
        fprintf(stderr, "  e.g. %s set name raghava\n", argv[0]);
        fprintf(stderr, "       %s get name\n", argv[0]);
        fprintf(stderr, "       %s del name\n", argv[0]);
        return 1;
    }

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

    int32_t err = send_command(fd, &argv[1], argc - 1);

    close(fd);
    return err ? 1 : 0;
}
