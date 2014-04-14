#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "src/common/include/dberror.h"
#include "src/common/include/try.h"
#include "src/client/include/client.h"

#define PORT 5000
#define HOST "127.0.0.1"
#define LOADDIR "p2tests"

struct client_options client_options = {
    .copt_port = PORT,
    .copt_interactive = 0,
    .copt_host = HOST,
    .copt_loaddir = LOADDIR,
};

const char *short_options = "h";

const struct option long_options[] = {
    {"help", no_argument, NULL, 'h'},
    {"port", required_argument, &client_options.copt_port, 0},
    {"host", required_argument, NULL, 0},
    {"loaddir", required_argument,  NULL, 0},
    {"interactive", no_argument, &client_options.copt_interactive, 1},
    {NULL, 0, NULL, 0}
};

static
int
parse_options(int argc, char **argv)
{
    while (1) {
        int option_index;
        int c = getopt_long(argc, argv, short_options,
                           long_options, &option_index);
        if (c == -1) {
            break;
        }
        switch (c) {
        case 0:
            if (optarg) {
                if (strcmp(long_options[option_index].name, "host") == 0) {
                    strcpy(client_options.copt_host, optarg);
                } else if (strcmp(long_options[option_index].name, "loaddir") == 0) {
                    strcpy(client_options.copt_loaddir, optarg);
                } else {
                    *(long_options[option_index].flag) = atoi(optarg);
                }
            }
            break;
        case 'h':
            printf("Usage: %s\n", argv[0]);
            printf("--help -h\n");
            printf("--port P         [default=%d]\n", PORT);
            printf("--host H         [default=%s]\n", HOST);
            printf("--loaddir dir    [default=%s]\n", LOADDIR);
            printf("--interactive\n");
            return 1;
        }
    }
    if (client_options.copt_interactive) {
        printf("port: %d, host: %s, loaddir: %s, interactive: %d\n",
               client_options.copt_port, client_options.copt_host,
               client_options.copt_loaddir, client_options.copt_interactive);
    }
    return 0;
}

int
main(int argc, char **argv)
{
    if (parse_options(argc, argv) != 0) {
        return 0;
    }

    int result;
    struct client *c;
    TRYNULL(result, DBENOMEM, c, client_create(&client_options), done);
    TRY(result, client_start(c), cleanup_client);
  cleanup_client:
    client_destroy(c);
  done:
    return result;
}
