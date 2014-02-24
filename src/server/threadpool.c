#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "../common/include/synch.h"
#include "../common/include/list.h"
#include "include/threadpool.h"

DECLLIST(job);
DEFLIST(job);

struct threadpool {
    unsigned tp_nthreads;
    pthread_t *tp_threads;
    struct joblist *tp_jobs;
    struct lock *tp_lock;
    struct cv *tp_cv_job_queue;
    volatile bool tp_shutdown;
    struct semaphore *tp_shutdown_sem;
};

struct thread_worker_args {
    struct threadpool *tw_tpool;
    unsigned int tw_tnum;
};

static
struct job *
job_copy(struct job *joborig)
{
    struct job *jobcopy = malloc(sizeof(struct job));
    if (jobcopy == NULL) {
        goto done;
    }
    *jobcopy = *joborig;
  done:
    return jobcopy;
}

static
void
job_destroy(struct job *job) {
    assert(job != NULL);
    free(job);
}

// This will handle each job. This is responsible for cleaning up
// any errors and closing the file descriptor.
static
void
job_handle(struct job *job) {
    assert(job != NULL);
    void *arg = job->j_arg;
    void (*routine)(void *) = job->j_routine;
    routine(arg);
    job_destroy(job);
}

static
void *
thread_worker(void *arg)
{
    struct thread_worker_args *twargs = (struct thread_worker_args*) arg;
    struct threadpool *tpool = twargs->tw_tpool;
    unsigned int tnum = twargs->tw_tnum;
    free(twargs);
    printf("starting worker thread %d\n", tnum);

    // The worker thread sleeps until
    // (1) there is a job in the queue, OR
    // (2) the main thread has initiated a shutdown. To handle a shutdown,
    //     the main thread waits on a semaphore for all the worker
    //     threads to exit. The main thread then cleans up any remaining
    //     jobs on the queue.
    while (1) {
        lock_acquire(tpool->tp_lock);
        while (joblist_size(tpool->tp_jobs) == 0 && !tpool->tp_shutdown) {
            cv_wait(tpool->tp_cv_job_queue, tpool->tp_lock);
        }
        if (tpool->tp_shutdown) {
            lock_release(tpool->tp_lock);
            goto shutdown;
        }
        assert(joblist_size(tpool->tp_jobs) != 0);
        struct job *job = joblist_remhead(tpool->tp_jobs);
        lock_release(tpool->tp_lock);

        // TODO: do something with the job
        printf("thread %d handling job...\n", tnum);
        sleep(random() % 10);
        printf("thread %d handling job done.\n", tnum);
        job_handle(job);
    }

  shutdown:
    V(tpool->tp_shutdown_sem);
    printf("shutting down worker thread %d\n", tnum);
    return NULL;
}

struct threadpool *
threadpool_create(unsigned nthreads)
{
    struct threadpool *tpool = malloc(sizeof(struct threadpool));
    if (tpool == NULL) {
        return NULL;
    }
    tpool->tp_threads = malloc(nthreads * sizeof(pthread_t));
    if (tpool->tp_threads == NULL) {
        goto cleanup_tpool;
    }
    tpool->tp_jobs = joblist_create();
    if (tpool->tp_jobs == NULL) {
        goto cleanup_threads;
    }
    tpool->tp_lock = lock_create();
    if (tpool->tp_lock == NULL) {
        goto cleanup_joblist;
    }
    tpool->tp_cv_job_queue = cv_create();
    if (tpool->tp_cv_job_queue == NULL) {
        goto cleanup_lock;
    }
    tpool->tp_shutdown_sem = semaphore_create(0);
    if (tpool->tp_shutdown_sem == NULL) {
        goto cleanup_cv;
    }
    tpool->tp_shutdown = false;
    tpool->tp_nthreads = nthreads;

    // start and detach all the threads
    int result;
    for (unsigned i = 0; i < nthreads; i++) {
        // TODO: right now, we will crash if we can't spawn and detach
        // the threads. In the future, we should handle this more elegantly

        // the args struct will be freed by the worker
        struct thread_worker_args *twargs =
            malloc(sizeof(struct thread_worker_args));
        assert(twargs != NULL);
        twargs->tw_tnum = i;
        twargs->tw_tpool = tpool;

        result = pthread_create(&tpool->tp_threads[i], NULL,
                                thread_worker, twargs);
        assert(result == 0);
        result = pthread_detach(tpool->tp_threads[i]);
        assert(result == 0);
    }
    goto done;

  cleanup_cv:
    cv_destroy(tpool->tp_cv_job_queue);
  cleanup_lock:
    lock_destroy(tpool->tp_lock);
  cleanup_joblist:
    joblist_destroy(tpool->tp_jobs);
  cleanup_threads:
    free(tpool->tp_threads);
  cleanup_tpool:
    free(tpool);
    tpool = NULL;
  done:
    return tpool;
}

void
threadpool_destroy(struct threadpool *tpool)
{
    assert(tpool != NULL);

    // The main thread initiates a shutdown, and waits for all the
    // worker threads to exit.
    lock_acquire(tpool->tp_lock);
    tpool->tp_shutdown = true;
    cv_broadcast(tpool->tp_cv_job_queue);
    lock_release(tpool->tp_lock);
    for (unsigned i = 0; i < tpool->tp_nthreads; i++) {
        P(tpool->tp_shutdown_sem);
    }

    // The main thread must then cleanup any remaining jobs on the queue
    while (joblist_size(tpool->tp_jobs) > 0) {
        struct job *job = joblist_remhead(tpool->tp_jobs);
        job_destroy(job);
    }

    semaphore_destroy(tpool->tp_shutdown_sem);
    cv_destroy(tpool->tp_cv_job_queue);
    lock_destroy(tpool->tp_lock);
    joblist_destroy(tpool->tp_jobs);
    free(tpool->tp_threads);
    free(tpool);
}

int
threadpool_add_job(struct threadpool *tpool, struct job *job)
{
    assert(tpool != NULL);
    assert(job != NULL);

    // the threadpool maintains its own copy of the job
    int result;
    struct job *jobinternal = job_copy(job);
    if (job == NULL) {
        goto done;
    }

    // Add a job to the queue and signal a thread to wake up to handle it
    lock_acquire(tpool->tp_lock);
    result = joblist_addtail(tpool->tp_jobs, jobinternal);
    if (result) {
        lock_release(tpool->tp_lock);
        goto cleanup_job;
    }
    cv_signal(tpool->tp_cv_job_queue);
    lock_release(tpool->tp_lock);
    result = 0;
    goto done;

  cleanup_job:
    job_destroy(jobinternal);
  done:
    return result;
}
