#ifndef _SERVER_H_
#define _SERVER_H_

struct server_options {
    int sopt_port;
    int sopt_backlog;
    int sopt_nthreads;
    char sopt_dbdir[128];
};

struct server;

struct server *server_create(struct server_options *options);
int server_start(struct server *s);
void server_destroy(struct server *s);

#endif
