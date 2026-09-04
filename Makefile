CC = gcc
CFLAGS = -Wall -Wextra -O2 -g

all: server client

# -pthread on the server only: it's the one using the thread pool.
# The flag covers both compiling (enables thread-safe libc bits) and
# linking (pulls in libpthread) - gcc treats it as shorthand for both.
server: server.c
	$(CC) $(CFLAGS) -pthread server.c -o server

client: client.c
	$(CC) $(CFLAGS) client.c -o client

clean:
	rm -f server client
