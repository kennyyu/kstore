#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include "include/dberror.h"

const char *
dberror_string(enum dberror result)
{
    switch (result) {
    case DBSUCCESS: return "Success";
    case DBENOMEM: return "Out of memory";
    case DBEPARSE: return "Parse error";
    case DBENOVAR: return "Variable undefined";
    case DBEVARTYPE: return "Variable wrong type";
    case DBEIOEARLYEOF: return "IO unexpected EOF";
    case DBEIOCHECKERRNO: return "IO error, check errno";
    case DBEIONOFILE: return "IO file does not exist";
    case DBEFILE: return "Couldn't open file";
    case DBESOCKET: return "Socket error";
    case DBESERVERTERM: return "Server terminated connection";
    case DBEGETADDRINFO: return "getaddrinfo error";
    case DBECONNECT: return "connect error";
    case DBESELECT: return "select error";
    case DBESIGACTION: return "sigaction error";
    case DBECOLOPEN: return "couldn't open column";
    case DBELISTEN: return "listen error";
    case DBEBIND: return "bind error";
    case DBEACCEPT: return "accept error";
    case DBESETSOCKOPT: return "setsockopt error";
    case DBELSEEK: return "lseek error";
    case DBECSV: return "csv error";
    case DBECOLSELECT: return "column select error";
    case DBECOLFETCH: return "column fetch error";
    default:
        assert(0);
        return NULL;
    }
}

void
dberror_log(char *msg,
            const char *file,
            int line,
            const char *func)
{
    fprintf(stderr, "[ERROR: %s] %s:%d:%s",
            msg, file, line, func);
}
