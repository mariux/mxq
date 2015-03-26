#define _GNU_SOURCE
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <time.h>

#include <sys/types.h>
#include <unistd.h>

#include "mx_log.h"
#ifndef mx_free_null
#include <stdlib.h>
#define mx_free_null(a) do { free((a)); (a) = NULL; } while(0)
#endif


static int timetag(char *buf, size_t size)
{
    time_t t;
    struct tm *ltime;
    size_t len;

    if(!size)
        return -(errno=EINVAL);

    *buf = 0;

    t = time(NULL);
    if (t == ((time_t) -1))
        return -errno;

    ltime = localtime(&t);
    if (ltime == NULL)
        return -(errno=EINVAL);

    len = strftime(buf, size, "%F %T %z", ltime);
    if (!len)
        *buf = 0;

    return len;
}

int mx_log_print(char *msg, size_t len)
{
    int res;
    char timebuf[1024];

    static char *lastmsg = NULL;
    static size_t lastlen = 0;
    static int cnt = 0;

    if (!msg) {
        mx_free_null(lastmsg);
        return 0;
    }

    if (!len)
        return 0;

    if (!*msg)
        return -(errno=EINVAL);

    if (lastmsg && lastlen == len) {
        res = strcmp(msg, lastmsg);
        if (res == 0) {
            cnt++;
            mx_free_null(msg);
            return 2;
        }
    }

    timetag(timebuf, sizeof(timebuf));

    if (cnt > 1)
        printf("%s %s[%d]: last message repeated %d times\n", timebuf, program_invocation_short_name, getpid(), cnt);
    else if (cnt == 1)
        printf("%s %s[%d]: %s\n", timebuf, program_invocation_short_name, getpid(), lastmsg);

    if (lastmsg)
        mx_free_null(lastmsg);

    lastmsg = msg;
    cnt     = 0;

    printf("%s %s[%d]: %s\n", timebuf, program_invocation_short_name, getpid(), msg);
    fflush(stdout);

    return 1;
}
