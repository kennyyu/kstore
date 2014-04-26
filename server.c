#include <assert.h>
#include <getopt.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <db/common/dberror.h>
#include <db/common/try.h>
#include <db/server/server.h>

#define PORT 5000
#define BACKLOG 16
#define NTHREADS 16
#define DBDIR "db"

struct server_options server_options = {
    .sopt_port = PORT,
    .sopt_backlog = BACKLOG,
    .sopt_nthreads = NTHREADS,
    .sopt_dbdir = DBDIR,
};

const char *short_options = "h";

const struct option long_options[] = {
    {"help", no_argument, NULL, 'h'},
    {"port", required_argument, &server_options.sopt_port, 0},
    {"backlog", required_argument, &server_options.sopt_backlog, 0},
    {"nthreads", required_argument,  &server_options.sopt_nthreads, 0},
    {"dbdir", required_argument, NULL, 0},
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
                if (strcmp(long_options[option_index].name, "dbdir") == 0) {
                    strcpy(server_options.sopt_dbdir, optarg);
                } else {
                    *(long_options[option_index].flag) = atoi(optarg);
                }
            }
            break;
        case 'h':
            printf("Usage: %s\n", argv[0]);
            printf("--help -h\n");
            printf("--port P         [default=%d]\n", PORT);
            printf("--backlog B      [default=%d]\n", BACKLOG);
            printf("--nthreads T     [default=%d]\n", NTHREADS);
            printf("--dbdir dir      [default=%s]\n", DBDIR);
            return 1;
        }
    }
    printf("port: %d, backlog: %d, nthreads: %d, dbdir: %s\n",
            server_options.sopt_port, server_options.sopt_backlog,
            server_options.sopt_nthreads, server_options.sopt_dbdir);
    return 0;
}

int
main(int argc, char **argv)
{
    if (parse_options(argc, argv) != 0) {
        return 0;
    }

    int result;
    struct server *s;
    TRYNULL(result, DBENOMEM, s, server_create(&server_options), done);
    TRY(result, server_start(s), cleanup_server);
  cleanup_server:
    server_destroy(s);
  done:
    return result;
}
