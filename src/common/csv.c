#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <db/common/csv.h>
#include <db/common/array.h>
#include <db/common/dberror.h>
#include <db/common/try.h>

DEFARRAY_BYTYPE(intarray, int, /* no inline */);

DEFARRAY(csv_result, /* no inline */);

#define BUFSIZE 1024
#define DELIMITERS ",\n"

void
csv_destroy(struct csv_resultarray *results)
{
    assert(results != NULL);
    while (csv_resultarray_num(results) > 0) {
        struct csv_result *header = csv_resultarray_get(results, 0);
        header->csv_vals->arr.num = 0;
        intarray_destroy(header->csv_vals);
        free(header);
        csv_resultarray_remove(results, 0);
    }
    csv_resultarray_destroy(results);
}

struct csv_resultarray *
csv_parse(int fd)
{
    int result;
    struct csv_resultarray *results = NULL;
    result = lseek(fd, 0, SEEK_SET);
    if (result) {
        result = DBELSEEK;
        DBLOG(result);
        goto done;
    }
    FILE *file;
    TRYNULL(result, DBEIONOFILE, file, fdopen(fd, "r"), done);
    TRYNULL(result, DBENOMEM, results, csv_resultarray_create(), cleanup_file);

    char buf[BUFSIZE];
    bzero(buf, BUFSIZE);
    char c;
    unsigned ix = 0;
    while ((c = fgetc(file)) != EOF) {
        if (c == '\n' || c == ',') {
            buf[ix] = '\0';
            struct csv_result *header;
            TRYNULL(result, DBENOMEM, header, malloc(sizeof(struct csv_result)), free_csv_resultarray);
            TRYNULL(result, DBENOMEM, header->csv_vals, intarray_create(), free_csv_resultarray);
            TRY(result, csv_resultarray_add(results, header, NULL), free_csv_resultarray);
            strcpy(header->csv_colname, buf);
            ix = 0;
            bzero(buf, BUFSIZE);
            if (c == '\n') {
                break;
            }
        } else {
            buf[ix++] = c;
        }
    }
    if (c == EOF) {
        result = DBEIOEARLYEOF;
        DBLOG(result);
        goto free_csv_resultarray;
    }

    // loop until we have no more rows
    ix = 0;
    unsigned colnum = 0;
    while ((c = fgetc(file)) != EOF) {
        if (c == '\n' || c == ',') {
            buf[ix] = '\0';
            int val = atoi(buf);
            struct csv_result *header = csv_resultarray_get(results, colnum);
            TRY(result, intarray_add(header->csv_vals, (void *) val, NULL), free_csv_resultarray);
            ix = 0;
            bzero(buf, BUFSIZE);
            if (c == '\n') {
                colnum = 0;
            } else {
                colnum++;
            }
        } else {
            buf[ix++] = c;
        }
    }
    // success
    goto cleanup_file;

  free_csv_resultarray:
    csv_destroy(results);
    results = NULL;
  cleanup_file:
    assert(fclose(file) == 0);
  done:
    return results;
}
