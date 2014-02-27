#ifndef _SYNCH_H
#define _SYNCH_H

#include <stdbool.h>

// Contains convenience wrappers for synch primitives

struct lock;
struct lock *lock_create(void);
void lock_destroy(struct lock *lk);
void lock_acquire(struct lock *lk);
void lock_release(struct lock *lk);

struct semaphore;
struct semaphore *semaphore_create(unsigned int count);
void semaphore_destroy(struct semaphore *sem);
void P(struct semaphore *sem);
void V(struct semaphore *sem);

struct cv;
struct cv *cv_create(void);
void cv_destroy(struct cv *cv);
void cv_wait(struct cv *cv, struct lock *lk);
void cv_broadcast(struct cv *cv);
void cv_signal(struct cv *cv);

#endif
