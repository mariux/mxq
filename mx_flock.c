
#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>

#include "mx_log.h"
#include "mx_flock.h"

#define FLOCK_MODE 0600

#ifndef free_null
#define free_null(a) do { free((a)); (a) = NULL; } while(0);
#endif

static inline int _flock_open(struct mx_flock *lock, mode_t mode)
{
    int fd;

    assert(lock);
    assert(lock->fname);

    fd = open(lock->fname, O_RDONLY|O_CREAT|O_NOCTTY|O_CLOEXEC, mode);
    if (fd < 0) {
        perror("open");
        return -1;
    }

    lock->fd = fd;

    return fd;
}

static inline int _flock_close(struct mx_flock *lock)
{
    int res;

    res = close(lock->fd);
    if (res < 0)
        perror("close");
    lock->fd = -1;

    return res;
}

static inline void _flock_free(struct mx_flock *lock)
{
    if (!lock)
        return;

    if (lock->fname)
        free_null(lock->fname);

    if (lock->fd >= 0)
        _flock_close(lock);

    free(lock);
}


struct mx_flock *mx_flock(int operation, char *fmt, ...)
{
    struct mx_flock *lock;
    va_list ap;
    char *fname;
    int res;
    struct stat s0, s1;

    lock = malloc(sizeof(*lock));
    if (!lock) {
        perror("malloc");
        return NULL;
    }

    lock->fd = -1;
    lock->operation = -1;
    lock->locked = 0;

    va_start(ap, fmt);
    res = vasprintf(&fname, fmt, ap);
    va_end(ap);

    if (res == -1) {
       perror("vasprintf");
       _flock_free(lock);
       return NULL;
    }

    lock->fname = fname;

    while (1) {
        res = _flock_open(lock, FLOCK_MODE);
        if (res < 0) {
            _flock_free(lock);
            return NULL;
        }

        res = flock(lock->fd, operation|LOCK_NB);
        if (res < 0) {
            if (errno == EWOULDBLOCK)
                return lock;
            perror("flock");
            _flock_free(lock);
            return NULL;
        }

        fstat(lock->fd,   &s0);
        stat(lock->fname, &s1);
        if (s0.st_ino == s1.st_ino) {
            lock->operation = operation;
            break;
        }

        mx_log_warning("failed to acquire lock on %s - file changed on disk - retrying", lock->fname);

        _flock_close(lock);
    }

    lock->locked = 1;
    return lock;
}

int mx_funlock(struct mx_flock *lock)
{
    int res;

    assert(lock->fname);
    assert(lock->fd >= 0);
    assert(lock->operation >=0);
    assert(lock->locked);

    res = unlink(lock->fname);
    if (res < 0)
        perror("unlink");

    res = flock(lock->fd, LOCK_UN);
    if (res < 0)
        perror("flock");

    _flock_close(lock);
    _flock_free(lock);

    return res;
}
