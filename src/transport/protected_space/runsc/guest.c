#include <arpa/inet.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int exact_file(const char *path, const char *expected) {
    char content[64];
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;
    ssize_t size = read(fd, content, sizeof(content));
    close(fd);
    return size == (ssize_t)strlen(expected) && memcmp(content, expected, size) == 0;
}

static int launch(const char *sdk, const char *entry, int port) {
    if (strcmp(sdk, "gradio") || strcmp(entry, "/inputs/source/app.py")) return 1;
    if (!exact_file(entry, "verified source") ||
        !exact_file("/inputs/wheels/fixture-1-py3-none-any.whl", "verified wheel") ||
        !exact_file("/inputs/models/0/model.bin", "verified model")) return 2;
    if (open(entry, O_WRONLY) >= 0) return 3;
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_addr.s_addr=htonl(INADDR_LOOPBACK), .sin_port=htons(port)};
    int server = socket(AF_INET, SOCK_STREAM, 0);
    if (server < 0 || bind(server, (struct sockaddr *)&addr, sizeof(addr)) || listen(server, 1)) return 4;
    int peer = accept(server, NULL, NULL);
    if (peer < 0 || write(peer, "verified source", 15) != 15) return 5;
    char next;
    return read(peer, &next, 1) != 0;
}

static int relay(const char *endpoint) {
    int port;
    if (sscanf(endpoint, "127.0.0.1:%d", &port) != 1) return 6;
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_addr.s_addr=htonl(INADDR_LOOPBACK), .sin_port=htons(port)};
    for (int retry = 0; retry < 30; retry++) {
        int peer = socket(AF_INET, SOCK_STREAM, 0);
        if (peer < 0) return 7;
        if (connect(peer, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            char buf[64];
            ssize_t size;
            while ((size = read(peer, buf, sizeof(buf))) > 0) {
                if (write(STDOUT_FILENO, buf, size) != size) return 8;
            }
            return size != 0;
        }
        close(peer);
        usleep(50000);
    }
    return 9;
}

int main(int argc, char **argv) {
    if (argc == 4) return launch(argv[1], argv[2], atoi(argv[3]));
    if (argc == 2) return relay(argv[1]);
    return 10;
}
