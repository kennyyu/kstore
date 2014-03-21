#ifndef _DBERROR_H_
#define _DBERROR_H_

#include <stdio.h>
#include <stdbool.h>
#include "cassert.h"

enum dberror {
    DBSUCCESS = 0,
    DBENOMEM,
    DBEPARSE,
    DBENOVAR,
    DBEVARTYPE,
    DBEIOEARLYEOF,
    DBEIOCHECKERRNO,
    DBEIONOFILE,
    DBEFILE,
    DBESOCKET,
    DBESERVERTERM,
    DBEGETADDRINFO,
    DBECONNECT,
    DBESELECT,
    DBESIGACTION,
    DBECOLOPEN,
    DBELISTEN,
    DBEBIND,
    DBEACCEPT,
    DBESETSOCKOPT,
    DBELSEEK,
    DBECSV,
    DBECOLSELECT,
    DBECOLFETCH,
    DBECLIENTTERM,
    DBEAGG,
};

const char *dberror_string(enum dberror result);

bool dberror_server_is_fatal(enum dberror result);
bool dberror_client_is_fatal(enum dberror result);

void dberror_log(char *msg,
                 const char *file,
                 int line,
                 const char *func);

#define DBLOG(result) dberror_log((char *) dberror_string((result)), __FILE__, __LINE__, __func__);

CASSERT(DBSUCCESS == 0);

#endif
