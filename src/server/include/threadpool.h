#ifndef _SERVER_THREADPOOL_H_
#define _SERVER_THREADPOOL_H_

struct threadpool;

struct job {
    void *j_arg;
    void (*j_routine)(void *arg);
};

struct threadpool *threadpool_create(unsigned nthreads);
void threadpool_destroy(struct threadpool *tpool);

// return 0 on success, otherwise error.
// the threadpool will create its own copy of job
int threadpool_add_job(struct threadpool *tpool, struct job *job);

#endif
