// client.c
//
// Commit 1: the smallest possible TCP client.
//
// What this does:
//   1. Ask the OS for a socket, same as the server.
//   2. Instead of bind()+listen(), we connect() straight to the server's address.
//   3. Send a message, read the reply, close.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

int main(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);  // 127.0.0.1, "this machine"

    if (connect(fd, (const struct sockaddr *)&addr, sizeof(addr)) != 0) {
        die("connect()");
    }

    char msg_out[] = "hello";
    write(fd, msg_out, strlen(msg_out));

    char rbuf[64] = {0};
    ssize_t n = read(fd, rbuf, sizeof(rbuf) - 1);
    if (n < 0) {
        die("read()");
    }
    printf("server says: %s\n", rbuf);

    close(fd);
    return 0;
}
