#ifndef _CLIENT_H_
#define _CLIENT_H_

struct client_options {
    int copt_port;
    char copt_host[128];
    char copt_loaddir[128];
    int copt_interactive;
};

struct client;

struct client *client_create(struct client_options *options);
int client_start(struct client *c);
void client_destroy(struct client *c);

#endif
