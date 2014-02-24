#include <assert.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdlib.h>
#include "include/synch.h"

struct lock {
    pthread_mutex_t lk_mutex;
};

struct lock *
lock_create(void)
{
    struct lock *lk = malloc(sizeof(struct lock));
    if (lk == NULL) {
        goto done;
    }
    int result = pthread_mutex_init(&lk->lk_mutex, NULL);
    if (result == -1) {
        goto cleanup_lock;
    }
    goto done;

  cleanup_lock:
    free(lk);
    lk = NULL;
  done:
    return lk;
}

void
lock_destroy(struct lock *lk)
{
    assert(lk != NULL);
    assert(pthread_mutex_destroy(&lk->lk_mutex) == 0);
    free(lk);
}

void
lock_acquire(struct lock *lk)
{
    assert(lk != NULL);
    assert(pthread_mutex_lock(&lk->lk_mutex) == 0);
}

void
lock_release(struct lock *lk)
{
    assert(lk != NULL);
    assert(pthread_mutex_unlock(&lk->lk_mutex) == 0);
}

struct semaphore {
    sem_t sm_sem;
};

struct semaphore *
semaphore_create(unsigned int count)
{
    struct semaphore *sem = malloc(sizeof(struct semaphore));
    if (sem == NULL) {
        goto done;
    }
    int result = sem_init(&sem->sm_sem, 0 /* not shared between procs */,
                          count);
    if (result == -1) {
        goto cleanup_sem;
    }
    goto done;

  cleanup_sem:
    free(sem);
    sem = NULL;
  done:
    return sem;
}

void
semaphore_destroy(struct semaphore *sem)
{
    assert(sem != NULL);
    assert(sem_destroy(&sem->sm_sem) == 0);
    free(sem);
}

void P(struct semaphore *sem)
{
    assert(sem != NULL);
    assert(sem_wait(&sem->sm_sem) == 0);
}

void V(struct semaphore *sem) {
    assert(sem != NULL);
    assert(sem_post(&sem->sm_sem) == 0);
}

struct cv {
    pthread_cond_t cv_cond;
};

struct cv *
cv_create(void)
{
    struct cv *cv = malloc(sizeof(struct cv));
    if (cv == NULL) {
        goto done;
    }
    int result = pthread_cond_init(&cv->cv_cond, NULL);
    if (result == -1) {
        goto cleanup_cv;
    }
    goto done;

  cleanup_cv:
    free(cv);
    cv = NULL;
  done:
    return cv;
}

void
cv_destroy(struct cv *cv)
{
    assert(cv != NULL);
    assert(pthread_cond_destroy(&cv->cv_cond) == 0);
    free(cv);
}

void
cv_wait(struct cv *cv, struct lock *lk)
{
    assert(cv != NULL);
    assert(lk != NULL);
    assert(pthread_cond_wait(&cv->cv_cond, &lk->lk_mutex) == 0);
}

void
cv_broadcast(struct cv *cv)
{
    assert(cv != NULL);
    assert(pthread_cond_broadcast(&cv->cv_cond) == 0);
}

void
cv_signal(struct cv *cv)
{
    assert(cv != NULL);
    assert(pthread_cond_signal(&cv->cv_cond) == 0);
}