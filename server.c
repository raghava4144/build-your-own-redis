// server.c
//
// Commit 1: the smallest possible TCP server.
//
// What this does, step by step:
//   1. Ask the OS for a "socket" - a handle we'll use to talk over the network.
//   2. Tell the OS which address/port we want to listen on ("bind").
//   3. Tell the OS we're ready to accept incoming connections ("listen").
//   4. Loop forever: accept one client, read what they send, write a reply, close.
//
// This is BLOCKING and single-client-at-a-time on purpose. It can only talk
// to one client, and if that client goes silent, this whole program freezes
// waiting on it. We fix that in later commits (event loop). For now the goal
// is just to prove two programs can exchange bytes over a socket.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>     // close()
#include <arpa/inet.h>  // htons, htonl, sockaddr_in
#include <sys/socket.h>

static void die(const char *msg) {
    // perror() prints msg plus the OS's explanation of the last error.
    perror(msg);
    exit(1);
}

static void msg(const char *m) {
    fprintf(stderr, "%s\n", m);
}

// Handle exactly one client: read one message, print it, send a reply.
static void do_something(int conn_fd) {
    char rbuf[64] = {0};

    // read() blocks until the client sends something (or closes the connection).
    ssize_t n = read(conn_fd, rbuf, sizeof(rbuf) - 1);
    if (n < 0) {
        msg("read() error");
        return;
    }

    printf("client says: %s\n", rbuf);

    char wbuf[] = "world";
    write(conn_fd, wbuf, strlen(wbuf));
}

int main(void) {
    // Step 1: get a socket handle.
    //   AF_INET    = IPv4
    //   SOCK_STREAM = TCP (a reliable, ordered byte stream)
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    // Step 2 (extra): allow us to restart the server on the same port
    // immediately, without waiting for the OS to release it.
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // Step 3: bind to an address. 0.0.0.0:1234 means "any local network
    // interface, port 1234".
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);       // host-to-network-short: fix byte order
    addr.sin_addr.s_addr = htonl(0);   // wildcard IP 0.0.0.0

    if (bind(fd, (const struct sockaddr *)&addr, sizeof(addr)) != 0) {
        die("bind()");
    }

    // Step 4: start listening. SOMAXCONN is the max queue of pending
    // connections the OS will hold for us before we call accept().
    if (listen(fd, SOMAXCONN) != 0) {
        die("listen()");
    }

    printf("listening on 0.0.0.0:1234 ...\n");

    while (1) {
        // accept() blocks until a client connects, then hands us a NEW
        // socket handle (conn_fd) representing that specific connection.
        // The original `fd` keeps listening for the next client.
        struct sockaddr_in client_addr = {0};
        socklen_t addrlen = sizeof(client_addr);
        int conn_fd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
        if (conn_fd < 0) {
            continue;   // error on this one client, keep serving others
        }

        do_something(conn_fd);
        close(conn_fd);
    }

    return 0;
}
