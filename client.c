// client.c
//
// Commit 6: parse the new tagged response format.
//
//   ./client set name raghava
//   ./client get name
//   ./client del name
//   ./client keys          <- new! only possible now that we can return arrays
//
// The request side (sending a command) is unchanged from Commit 4. What's
// new is print_one() below, which reads a tag byte and decides how to
// interpret what follows - including recursing into itself for arrays,
// since an array can contain any other tagged value (even another array).

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

// Tags - must match the server's enum in server.c.
enum {
    TAG_NIL = 0,
    TAG_ERR = 1,
    TAG_STR = 2,
    TAG_INT = 3,
    TAG_ARR = 4,
};

// Parse and print ONE tagged value starting at `cur`. Returns a pointer
// just past the value that was consumed, so the caller (or a recursive
// call, for arrays) knows where the next value starts.
static const uint8_t *print_one(const uint8_t *cur, const uint8_t *end, int indent) {
    if (cur >= end) {
        printf("(truncated response)\n");
        return end;
    }

    uint8_t tag = *cur++;

    switch (tag) {
    case TAG_NIL:
        printf("(nil)\n");
        break;

    case TAG_ERR: {
        if (cur + 8 > end) {
            printf("(truncated response)\n");
            return end;
        }
        uint32_t code = 0, mlen = 0;
        memcpy(&code, cur, 4); cur += 4;
        memcpy(&mlen, cur, 4); cur += 4;
        if (cur + mlen > end) {
            printf("(truncated response)\n");
            return end;
        }
        printf("(error %u) %.*s\n", code, (int)mlen, cur);
        cur += mlen;
        break;
    }

    case TAG_STR: {
        if (cur + 4 > end) {
            printf("(truncated response)\n");
            return end;
        }
        uint32_t len = 0;
        memcpy(&len, cur, 4); cur += 4;
        if (cur + len > end) {
            printf("(truncated response)\n");
            return end;
        }
        printf("%.*s\n", (int)len, cur);
        cur += len;
        break;
    }

    case TAG_INT: {
        if (cur + 8 > end) {
            printf("(truncated response)\n");
            return end;
        }
        int64_t val = 0;
        memcpy(&val, cur, 8); cur += 8;
        printf("(integer) %lld\n", (long long)val);
        break;
    }

    case TAG_ARR: {
        if (cur + 4 > end) {
            printf("(truncated response)\n");
            return end;
        }
        uint32_t n = 0;
        memcpy(&n, cur, 4); cur += 4;
        printf("(array, %u item%s)\n", n, n == 1 ? "" : "s");
        for (uint32_t i = 0; i < n && cur < end; i++) {
            printf("%*s%u) ", indent + 2, "", i + 1);
            cur = print_one(cur, end, indent + 2);
        }
        break;
    }

    default:
        printf("(unknown tag %d - client/server version mismatch?)\n", tag);
        cur = end;   // can't safely continue parsing an unknown format
        break;
    }

    return cur;
}

static int32_t send_command(int fd, char **args, int argc) {
    char wbuf[4 + k_max_msg];
    size_t pos = 4;

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

    char rbuf[4 + k_max_msg];
    errno = 0;
    if (read_full(fd, rbuf, 4) != 0) {
        msg(errno == 0 ? "server closed connection (EOF)" : "read() error");
        return -1;
    }

    uint32_t reply_len = 0;
    memcpy(&reply_len, rbuf, 4);
    if (reply_len > k_max_msg) {
        msg("bad reply length");
        return -1;
    }

    if (read_full(fd, &rbuf[4], reply_len) != 0) {
        msg("read() error while reading reply body");
        return -1;
    }

    const uint8_t *body = (const uint8_t *)&rbuf[4];
    print_one(body, body + reply_len, 0);

    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <command> [args...]\n", argv[0]);
        fprintf(stderr, "  e.g. %s set name raghava\n", argv[0]);
        fprintf(stderr, "       %s get name\n", argv[0]);
        fprintf(stderr, "       %s del name\n", argv[0]);
        fprintf(stderr, "       %s keys\n", argv[0]);
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
