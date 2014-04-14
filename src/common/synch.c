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
#ifdef __APPLE__
    sem_t *sm_sem;
#else
    sem_t sm_sem;
#endif
};

struct semaphore *
semaphore_create(unsigned int count)
{
    int result;
    struct semaphore *sem = malloc(sizeof(struct semaphore));
    if (sem == NULL) {
        goto done;
    }
#ifdef __APPLE__
    sem->sm_sem = sem_open("/cs165/threadpool_sem/", O_CREAT, S_IRWXU, 0);
    if (sem->sm_sem == NULL) {
        result = -1;
        goto cleanup_sem;
    }
#else
    result = sem_init(&sem->sm_sem, 0 /* not shared between procs */,
                      count);
#endif
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
#ifdef __APPLE__
    assert(sem_close(sem->sm_sem) == 0);
#else
    assert(sem_destroy(&sem->sm_sem) == 0);
#endif
    free(sem);
}

void P(struct semaphore *sem)
{
    assert(sem != NULL);
#ifdef __APPLE__
    assert(sem_wait(sem->sm_sem) == 0);
#else
    assert(sem_wait(&sem->sm_sem) == 0);
#endif
}

void V(struct semaphore *sem) {
    assert(sem != NULL);
#ifdef __APPLE__
    assert(sem_post(sem->sm_sem) == 0);
#else
    assert(sem_post(&sem->sm_sem) == 0);
#endif
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

struct rwlock {
    pthread_rwlock_t rwlk_lock;
};

struct rwlock *
rwlock_create(void)
{
    struct rwlock *rwlk = malloc(sizeof(struct rwlock));
    if (rwlk == NULL) {
        goto done;
    }
    int result = pthread_rwlock_init(&rwlk->rwlk_lock, NULL);
    if (result) {
        goto cleanup_malloc;
    }
    result = 0;
    goto done;
  cleanup_malloc:
    free(rwlk);
    rwlk = NULL;
  done:
    return rwlk;
}

void
rwlock_destroy(struct rwlock *rwlk)
{
    assert(rwlk != NULL);
    assert(pthread_rwlock_destroy(&rwlk->rwlk_lock) == 0);
    free(rwlk);
}

void
rwlock_acquire_read(struct rwlock *rwlk)
{
    assert(rwlk != NULL);
    assert(pthread_rwlock_rdlock(&rwlk->rwlk_lock) == 0);
}

void
rwlock_acquire_write(struct rwlock *rwlk)
{
    assert(rwlk != NULL);
    assert(pthread_rwlock_wrlock(&rwlk->rwlk_lock) == 0);
}

void
rwlock_release(struct rwlock *rwlk)
{
    assert(rwlk != NULL);
    assert(pthread_rwlock_unlock(&rwlk->rwlk_lock) == 0);
}
