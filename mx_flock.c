
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

#if !defined mx_free_null
#   define mx_free_null(a) do { free((a)); (a) = NULL; } while(0)
#endif

static int _flock_open(struct mx_flock *lock, mode_t mode)
{
    int fd;

    assert(lock);
    assert(lock->fname);

    fd = open(lock->fname, O_RDONLY|O_CREAT|O_NOCTTY|O_CLOEXEC, mode);
    if (fd < 0) {
        mx_log_err("%s: %m",lock->fname);
        return -1;
    }

    lock->fd = fd;

    return fd;
}

static int _flock_close(struct mx_flock *lock)
{
    int res;

    assert(lock);

    res = close(lock->fd);
    if (res < 0)
        mx_log_debug("close(): %m");
    lock->fd = -1;

    return res;
}

static void _flock_free(struct mx_flock *lock)
{
    if (!lock)
        return;

    mx_free_null(lock->fname);

    free(lock);
}


struct mx_flock *mx_flock(int operation, char *fmt, ...)
{
    struct mx_flock *lock;
    va_list ap;
    char *fname;
    int res;
    struct stat s0, s1;

    assert(fmt);

    lock = malloc(sizeof(*lock));
    if (!lock) {
        mx_log_err("malloc(): %m");
        return NULL;
    }

    lock->fd = -1;
    lock->operation = -1;
    lock->locked = 0;

    va_start(ap, fmt);
    res = vasprintf(&fname, fmt, ap);
    va_end(ap);

    if (res == -1) {
       mx_log_err("vasprintf(): %m");
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
            mx_log_err("flock(): %m");
            _flock_close(lock);
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

    if (!lock)
        return 0;

    if (!lock->locked) {
        _flock_free(lock);
        return 0;
    }

    assert(lock->fname);
    assert(lock->fd >= 0);
    assert(lock->operation >= 0);

    res = unlink(lock->fname);
    if (res < 0)
        mx_log_warning("unlink(): %m");

    res = flock(lock->fd, LOCK_UN);
    if (res < 0)
        mx_log_warning("flock(): %m");

    _flock_close(lock);
    _flock_free(lock);

    return res;
}

void mx_flock_free(struct mx_flock *lock)
{
    _flock_free(lock);
}
