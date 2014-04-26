#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>
#include <db/common/dberror.h>

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
    case DBECOLEXISTS: return "column doesn't exist";
    case DBELISTEN: return "listen error";
    case DBEBIND: return "bind error";
    case DBEACCEPT: return "accept error";
    case DBESETSOCKOPT: return "setsockopt error";
    case DBELSEEK: return "lseek error";
    case DBECSV: return "csv error";
    case DBECOLSELECT: return "column select error";
    case DBECOLFETCH: return "column fetch error";
    case DBECLIENTTERM: return "client terminated connection";
    case DBEAGG: return "aggregation error";
    case DBEINTERMDIFFLEN: return "intermediates have different lengths";
    case DBEDIVZERO: return "division by zero";
    case DBECOLDIFFLEN: return "select intermediate and column have different lengths";
    case DBENOTREE: return "no btree on join input tree column";
    case DBEUNSUPPORTED: return "unsupported operation on this column";
    case DBEDUPCOL: return "duplicate column";
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
    char *errnomsg = strerror(errno);
    fprintf(stderr, "[ERROR: %s, errno: %s] %s:%d:%s\n",
            msg, errnomsg, file, line, func);
}

bool
dberror_server_is_fatal(enum dberror result)
{
    switch (result) {
    case DBECLIENTTERM:
    case DBESIGACTION:
    case DBELISTEN:
    case DBEBIND:
    case DBEACCEPT:
    case DBESETSOCKOPT:
    case DBEGETADDRINFO:
    case DBESOCKET:
    case DBEIOCHECKERRNO:
        return true;
    default:
        return false;
    }
}

bool
dberror_client_is_fatal(enum dberror result)
{
    switch (result) {
    case DBEIOCHECKERRNO:
    case DBESOCKET:
    case DBESERVERTERM:
    case DBEGETADDRINFO:
    case DBECONNECT:
    case DBESELECT:
    case DBESIGACTION:
    case DBEIOEARLYEOF:
        return true;
    default:
        return false;
    }
}
