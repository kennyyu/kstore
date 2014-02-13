#include <stdlib.h>
#include "../include/threadpool.h"

struct threadpool {
  unsigned tp_nthreads;
};

struct threadpool *
threadpool_create(unsigned nthreads)
{
  struct threadpool *tpool = malloc(sizeof(struct threadpool));
  if (tpool == NULL) {
    return NULL;
  }
  tpool->tp_nthreads = nthreads;
  return tpool;
}

void
threadpool_destroy(struct threadpool *tpool)
{
  free(tpool);
}
