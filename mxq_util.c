
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
