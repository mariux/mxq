
#define _GNU_SOURCE

#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <errno.h>
#include <assert.h>

#include <stdarg.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <time.h>
#include "mx_log.h"
#include "mx_util.h"

#include "mxq_util.h"


mode_t getumask(void)
{
    mode_t mask = umask( 0 );
    umask(mask);
    return mask;
}

size_t timetag(char *buf, size_t max)
{
    time_t t;
    struct tm *ltime;

    *buf = 0;

    t = time(NULL);
    if (t == ((time_t) -1)) {
        perror("timetag::time");
        return 0;
    }

    ltime = localtime(&t);
    if (ltime == NULL) {
        perror("timetag::localtime");
        return 0;
    }

    return strftime(buf, max, "%F %T %z", ltime);
}

char *mxq_hostname(void)
{
    static char hostname[1024] = "";
    int res;

    if (*hostname)
        return hostname;

    res = gethostname(hostname, 1024);
    if (res == -1) {
        if (errno != ENAMETOOLONG)
            assert_perror(errno);
        hostname[1024-1] = 0;
    }

    return hostname;
}

void *realloc_or_free(void *ptr, size_t size)
{
    void *new_ptr;

    new_ptr = realloc(ptr, size);
    if (new_ptr)
        return new_ptr;

    free(ptr);
    return NULL;
}

void *realloc_forever(void *ptr, size_t size)
{
    void *new_ptr;

    assert(size > 0);

    do {
        new_ptr = realloc(ptr, size);
        if (new_ptr)
            return new_ptr;

        sleep(1);
    } while (1);
}

void mxq_free_job(struct mxq_job_full *job)
{
    if (!job)
        return;

    free(job);
}

char **strvec_new(void)
{
    char **strvec;

    strvec = calloc(sizeof(*strvec), 1);

    return strvec;
}

static inline size_t strvec_length_cache(char **strvec, int32_t len)
{
    static char ** sv = NULL;
    static size_t l = 0;

    if (likely(len == -1)) {
        if (likely(sv == strvec)) {
            return l;
        }
        return -1;
    }

    if (likely(sv == strvec)) {
        l = len;
    } else {
        sv = strvec;
        l = len;
    }
    return l;
}

size_t strvec_length(char ** strvec)
{
    char ** sv;
    size_t len;

    assert(strvec);

    sv = strvec;

    len = strvec_length_cache(sv, -1);
    if (len != -1)
        return len;

    for (; *sv; sv++);

    len = sv-strvec;
    strvec_length_cache(sv, len);
    return len;
}

int strvec_push_str(char *** strvecp, char * str)
{
    char ** sv;

    size_t len;

    assert(strvecp);
    assert(*strvecp);
    assert(str);

    len = strvec_length(*strvecp);

    sv = realloc(*strvecp, sizeof(**strvecp) * (len + 2));
    if (!sv) {
       return 0;
    }

    sv[len++] = str;
    sv[len] = NULL;

    strvec_length_cache(sv, len);

    *strvecp = sv;

    return 1;
}

int strvec_push_strvec(char ***strvecp, char **strvec)
{
    char **sv;

    size_t len1;
    size_t len2;

    assert(strvecp);
    assert(*strvecp);
    assert(strvec);

    len1 = strvec_length(*strvecp);
    len2 = strvec_length(strvec);

    sv = realloc(*strvecp, sizeof(**strvecp) * (len1 + len2 + 1));
    if (!sv) {
       return 0;
    }

    memcpy(sv+len1, strvec, sizeof(*strvec) * (len2 + 1));

    strvec_length_cache(sv, len1+len2);

    *strvecp = sv;

    return 1;
}

char *strvec_to_str(char **strvec)
{
    char **sv;
    char*  buf;
    char*  s;
    size_t totallen;
    char*  str;

    assert(strvec);

    totallen = 0;
    for (sv = strvec; *sv; sv++) {
        totallen += strlen(*sv);
    }

    buf = malloc(sizeof(*buf) * (totallen * 2 + 2) + 1);
    if (!buf)
        return NULL;

    str  = buf;
    *str = 0;

    for (sv = strvec; *sv; sv++) {
        for (s=*sv; *s; s++) {
            switch (*s) {
                case '\\':
                    *(str++) = '\\';
                    *(str++) = '\\';
                    break;

                default:
                    *(str++) = *s;
            }
        }
        *(str++) = '\\';
        *(str++) = '0';
    }

    *str = '\0';

    return buf;
}

void strvec_free(char **strvec)
{
    char **sv;
    char*  buf;
    char*  s;
    size_t totallen;
    char*  str;

    if (!strvec)
        return;

    for (sv = strvec; *sv; sv++) {
        free(sv);
    }
    free(strvec);
}

char **str_to_strvec(char *str)
{
    int res;
    char* s;
    char* p;
    char** strvec;

    strvec = strvec_new();
    if (!strvec)
        return NULL;

    for (s=str; *s; s=p+2) {
       p = strstr(s, "\\0");
       if (!p) {
           free(strvec);
           errno = EINVAL;
           return NULL;
       }
       *p = 0;

       res = strvec_push_str(&strvec, s);
       if (!res) {
          free(strvec);
          return NULL;
       }
    }

    return strvec;
}

char *stringvectostring(int argc, char *argv[])
{
    int     i,j,k;
    char   *buf;
    char   *s;
    size_t  len = 1;

    for (i=0; i < argc; i++) {
        len += strlen(argv[i]);
        len += chrcnt(argv[i], '\\');
        len += 2;
    }

    buf = malloc(len);
    if (!buf)
        return NULL;

    for (i=0, k=0; i < argc; i++) {
        s = argv[i];
        for (j=0; j < strlen(s); j++) {
             buf[k++] = s[j];
             if (s[j] == '\\')
                 buf[k++] = '\\';
        }
        buf[k++] = '\\';
        buf[k++] = '0';
    }

    assert(k == len-1);
    buf[k] = 0;

    return buf;
}

char **stringtostringvec(int argc, char *s)
{
    int i;
    char *p;
    char **argv;

    argv = calloc(argc+1, sizeof(*argv));
    if (!argv)
        return NULL;

    for (i=0, p=s; i < argc; i++) {
        argv[i] = p;
        p = strstr(p, "\\0");  /* search "\0" */
        if (!p) {
            errno = EINVAL; /* "\0" need to be there or string is invalid */
            return NULL;
        }
        *p = 0;     /* add end of string */
        p += 2;     /* skip "\0" */
    }

    return argv;
}

int mxq_setenv(const char *name, const char *value)
{
    int res;

    res = setenv(name, value, 1);
    if (res == -1) {
        mx_log_err("mxq_setenv(%s, %s) failed! (%s)", name, value, strerror(errno));
        return 0;
    }

    return 1;
}


int mxq_setenvf(const char *name, char *fmt, ...)
{
    va_list ap;
    _mx_cleanup_free_ char *value = NULL;
    size_t len;
    int res;

    assert(name);
    assert(*name);
    assert(fmt);

    va_start(ap, fmt);
    len = vasprintf(&value, fmt, ap);
    va_end(ap);

    if (len == -1) {
        mx_log_err("mxq_setenvf(%s, %s, ...) failed! (%s)", name, fmt, strerror(errno));
        return 0;
    }

    return mxq_setenv(name, value);
}

int chrcnt(char *s, char c)
{
    int i = 0;
    char *p;

    p = s;

    while ((p = strchr(p, c))) {
        i++;
        p++;
    }

    return i;
}
