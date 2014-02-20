#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define PORT "5000"
#define HOST "localhost"

int
main(void)
{
    printf("hello client\n");

    int result;
    int sockfd;
    struct addrinfo hints, *servinfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    result = getaddrinfo(HOST, PORT, &hints, &servinfo);
    if (result != 0) {
        perror("getaddrinfo");
        result = -1;
        goto done;
    }

    // create a socket to connect to the server
    sockfd = socket(servinfo->ai_family, servinfo->ai_socktype,
                    servinfo->ai_protocol);
    if (sockfd == -1) {
        perror("socket");
        result = -1;
        goto done;
    }
    freeaddrinfo(servinfo);

    // wait for a connection to the server
    result = connect(sockfd, servinfo->ai_addr, servinfo->ai_addrlen);
    if (result == -1) {
        perror("connect");
        goto cleanup_sockfd;
    }

    // TODO: send query and read response
    char buf[1024];
    read(sockfd, buf, sizeof(buf));
    printf("%s\n", buf);
    result = 0;
    goto cleanup_sockfd;

  cleanup_sockfd:
    assert(close(sockfd) == 0);
  done:
    return result;
}
