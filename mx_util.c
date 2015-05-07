
#define _GNU_SOURCE

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <libgen.h>
#include <unistd.h>

//#include <sys/types.h>
//#include <sys/stat.h>
#include <fcntl.h>

#include "mx_util.h"

static inline int _mx_strbeginswith(char *str, const char *start, char **endptr, short ignore_case)
{
    size_t len;
    int res;

    assert(str);
    assert(start);

    len = strlen(start);
    if (ignore_case)
        res = strncasecmp(str, start, len);
    else
        res = strncmp(str, start, len);

    if (res != 0 || !endptr)
        return !res;

    *endptr  = str + len;

    return 1;
}

inline int mx_strbeginswith(char *str, const char *start, char **endptr)
{
    return _mx_strbeginswith(str, start, endptr, 0);
}

inline int mx_stribeginswith(char *str, const char *start, char **endptr)
{
    return _mx_strbeginswith(str, start, endptr, 1);
}

static inline int _mx_strbeginswithany(char *str, char **starts, char **endptr, short ignore_case)
{
    char **s;
    char *end;
    char *longestmatch = NULL;
    int res;

    for (s = starts; *s; s++) {
        res = _mx_strbeginswith(str, *s, &end, ignore_case);
        if (res && (!longestmatch || end > longestmatch))
            longestmatch = end;
    }

    if (longestmatch) {
        *endptr = longestmatch;
        return 1;
    }

    return 0;
}

inline int mx_strbeginswithany(char *str, char **starts, char **endptr)
{
    return _mx_strbeginswithany(str, starts, endptr, 0);
}

inline int mx_stribeginswithany(char *str, char **starts, char **endptr)
{
    return _mx_strbeginswithany(str, starts, endptr, 1);
}

inline char *mx_strskipwhitespaces(char *str)
{
    char *s;

    assert(str);

    for (s = str; *s && *s == ' '; s++)
        /* empty */;

    return s;
}

/* wrapper unsigned */

inline int mx_strtoul(char *str, unsigned long int *to)
{
    unsigned long int ul;
    char *end;

    assert(str);
    assert(to);

    errno = 0;

    ul = strtoul(str, &end, 0);

    if (errno)
        return -errno;

    end = mx_strskipwhitespaces(end);

    if (!end || str == end || *end)
        return -(errno=EINVAL);

    if (strchr(str, '-'))
        return -(errno=ERANGE);

    *to = ul;

    return 0;
}

inline int mx_strtoull(char *str, unsigned long long int *to)
{
    unsigned long long int ull;
    char *end;

    assert(str);
    assert(to);

    errno = 0;

    ull = strtoull(str, &end, 0);

    if (errno)
        return -errno;

    end = mx_strskipwhitespaces(end);

    if (!end || str == end || *end)
        return -(errno=EINVAL);

    if (strchr(str, '-'))
        return -(errno=ERANGE);

    *to = ull;

    return 0;
}

/* wrapper signed */

inline int mx_strtol(char *str, signed long int *to)
{
    long int l;
    char *end;

    assert(str);
    assert(to);

    errno = 0;

    l = strtoul(str, &end, 0);

    if (errno)
        return -errno;

    end = mx_strskipwhitespaces(end);

    if (!end || str == end || *end)
        return -(errno=EINVAL);

    *to = l;

    return 0;
}

inline int mx_strtoll(char *str, signed long long int *to)
{
    long long int ll;
    char *end;

    assert(str);
    assert(to);

    errno = 0;

    ll = strtoll(str, &end, 0);

    if (errno)
        return -errno;

    end = mx_strskipwhitespaces(end);

    if (!end || str == end || *end)
        return -(errno=EINVAL);

    *to = ll;

    return 0;
}

/* unsigned */

int mx_strtoui(char *str, unsigned int *to)
{
    unsigned long int ul;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtoul(str, &ul);
    if (res < 0)
        return res;

    if ((unsigned long int)(unsigned int)ul != ul)
        return -(errno=ERANGE);

    *to = (unsigned int)ul;

    return 0;
}

int mx_strtou8(char *str, uint8_t *to)
{
    unsigned long int ul;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtoul(str, &ul);
    if (res < 0)
        return res;

    if ((unsigned long int)(uint8_t)ul != ul)
        return -(errno=ERANGE);

    *to = (uint8_t)ul;

    return 0;
}

int mx_strtou16(char *str, uint16_t *to)
{
    unsigned long int ul;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtoul(str, &ul);
    if (res < 0)
        return res;

    if ((unsigned long int)(uint16_t)ul != ul)
        return -(errno=ERANGE);

    *to = (uint16_t)ul;

    return 0;
}

int mx_strtou32(char *str, uint32_t *to)
{
    unsigned long int ul;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtoul(str, &ul);
    if (res < 0)
        return res;

    if ((unsigned long int)(uint32_t)ul != ul)
        return -(errno=ERANGE);

    *to = (uint32_t)ul;

    return 0;
}

int mx_strtou64(char *str, uint64_t *to)
{
    unsigned long long int ull;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtoull(str, &ull);
    if (res < 0)
        return res;

    if ((unsigned long long int)(uint64_t)ull != ull)
        return -(errno=ERANGE);

    *to = (uint64_t)ull;

    return 0;
}

/* signed */

int mx_strtoi(char *str, signed int *to)
{
    signed long int l;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtol(str, &l);
    if (res < 0)
        return res;

    if ((signed long int)(signed int)l != l)
        return -(errno=ERANGE);

    *to = (signed int)l;

    return 0;
}

int mx_strtoi8(char *str, int8_t *to)
{
    signed long int l;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtol(str, &l);
    if (res < 0)
        return res;

    if ((signed long int)(int8_t)l != l)
        return -(errno=ERANGE);

    *to = (uint8_t)l;

    return 0;
}

int mx_strtoi16(char *str, int16_t *to)
{
    signed long int l;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtol(str, &l);
    if (res < 0)
        return res;

    if ((signed long int)(int16_t)l != l)
        return -(errno=ERANGE);

    *to = (uint16_t)l;

    return 0;
}

int mx_strtoi32(char *str, int32_t *to)
{
    signed long int l;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtol(str, &l);
    if (res < 0)
        return res;

    if ((signed long int)(int32_t)l != l)
        return -(errno=ERANGE);

    *to = (int32_t)l;

    return 0;
}

int mx_strtoi64(char *str, int64_t *to)
{
    signed long long int ll;
    char *end;
    int res;

    assert(str);
    assert(to);

    res = mx_strtoll(str, &ll);
    if (res < 0)
        return res;

    if ((signed long long int)(int64_t)ll != ll)
        return -(errno=ERANGE);

    *to = (int64_t)ll;

    return 0;
}

char *mx_strdup_forever(char *str)
{
    char *dup;

    do {
        dup = strdup(str);
        assert(dup || (!dup && errno == ENOMEM));
    } while(!dup);

    return dup;
}

int mx_vasprintf_forever(char **strp, const char *fmt, va_list ap)
{
    int len;

    do {
        len = vasprintf(strp, fmt, ap);
    } while (len < 0);

    return len;
}


int mx_asprintf_forever(char **strp, const char *fmt, ...)
{
    va_list ap;
    int len;

    va_start(ap, fmt);
    len = mx_vasprintf_forever(strp, fmt, ap);
    va_end(ap);

    return len;
}

char *mx_dirname(char *path)
{
    char *tmp;
    char *dname;
    char *result;

    assert(path);

    tmp = strdup(path);
    if (!tmp)
        return NULL;

    dname = dirname(tmp);

    assert(dname);

    result = strdup(dname);

    free(tmp);

    return result;
}

char *mx_dirname_forever(char *path)
{
    char *dname;

    assert(path);

    do {
        dname = mx_dirname(path);
    } while (!dname);

    return dname;
}

int mx_dup2_close_new(int oldfd, int newfd)
{
    int res;

    if (oldfd == newfd)
        return 0;

    res = close(newfd);
    if (res == -1 && errno == EBADF)
        return -errno;

    res = dup2(oldfd, newfd);
    if(res == -1)
        return -errno;

    return res;
}

int mx_dup2_close_both(int oldfd, int newfd)
{
    int res;

    if (oldfd == newfd)
        return 0;

    res = mx_dup2_close_new(oldfd, newfd);
    if (res < 0)
        return res;

    assert(res == newfd);

    res = close(oldfd);
    if (res == -1 && errno == EBADF)
        return -errno;

    return newfd;
}

int mx_setenv_forever(const char *name, const char *value)
{
    assert(name);
    assert(*name);

    int res;

    do {
        res = setenv(name, value, 1);
        if (!res)
            return 0;
        assert(errno != EINVAL);
    } while (errno == ENOMEM);

    assert(errno == ENOMEM);
    return -errno;
}

int mx_setenvf_forever(const char *name, char *fmt, ...)
{
    assert(name);
    assert(*name);
    assert(fmt);

    va_list ap;
    char *value = NULL;
    int res;

    va_start(ap, fmt);
    mx_vasprintf_forever(&value, fmt, ap);
    va_end(ap);

    res = mx_setenv_forever(name, value);

    free(value);

    return res;
}

int mx_open_newfile(char *fname)
{
    int fh;
    int res;

    int    flags = 0;
    mode_t mode  = 0;

    flags |= O_CREAT|O_WRONLY|O_TRUNC;
    flags |= O_NOFOLLOW;

    mode |= S_IRUSR|S_IWUSR;
    mode |= S_IRGRP|S_IWGRP;
    mode |= S_IROTH|S_IWOTH;

    if (!fname) {
        fname = "/dev/null";
    } else if (strcmp(fname, "/dev/null") != 0) {
        res = unlink(fname);
        if (res == -1 && errno != ENOENT)
            return -errno;

        flags |= O_EXCL;
    }

    fh = open(fname, flags, mode);
    if (fh == -1)
        return -errno;

    return fh;
}
