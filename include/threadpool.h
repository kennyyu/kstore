#ifndef _THREADPOOL_H_
#define _THREADPOOL_H_

struct threadpool;

struct threadpool *threadpool_create(unsigned nthreads);
void threadpool_destroy(struct threadpool *tpool);

#endif
