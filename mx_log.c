#define _GNU_SOURCE

#include <stdio.h>
#include <stdarg.h>
#include <errno.h>

#include "mx_log.h"

#define MX_LOG_GET (MX_LOG_NONE-1)

int mx_log_level_set(int level)
{
    static int loglevel = MX_LOG_WARNING;
    int oldloglevel = loglevel;

    switch (level) {
        case MX_LOG_GET:
            return loglevel;

        case MX_LOG_NONE:
        case MX_LOG_EMERG:
        case MX_LOG_ALERT:
        case MX_LOG_CRIT:
        case MX_LOG_ERR:
        case MX_LOG_WARNING:
        case MX_LOG_NOTICE:
        case MX_LOG_INFO:
        case MX_LOG_DEBUG:
            loglevel=level;
            return oldloglevel;
    }

    return -(errno=EINVAL);
}

int mx_log_level_mxlog_to_syslog(int level)
{
    level = MX_LOG_MXLOG_TO_SYSLOG(level);

    if (level < LOG_EMERG || level > LOG_DEBUG)
        return -(errno=EINVAL);

    return level;
}

int mx_log_level_syslog_to_mxlog(int level)
{
    if (level < LOG_EMERG || level > LOG_DEBUG)
        return -(errno=EINVAL);

    level = MX_LOG_SYSLOG_TO_MXLOG(level);

    return level;
}

int mx_log_level_get(void)
{
    return mx_log_level_set(MX_LOG_GET);
}

int mx_log_printf(const char *fmt, ...)
{
    int len;
    int len2;
    int res;
    char *msg = NULL;
    va_list ap;

    va_start(ap, fmt);
    len = vasprintf(&msg, fmt, ap);
    va_end(ap);

    if (len == -1)
        return -(errno=ENOMEM);

    if (mx_log_print)
        return mx_log_print(msg, len);

    if (len == 0) {
        mx_free_null(msg);
        return 0;
    }

    len2 = fprintf(stderr, "%s\n", msg);
    res  = fflush(stderr);
    mx_free_null(msg);

    if (len2 != len)
        return -(errno=EIO);

    if (!res)
        res = 0;

    return len;
}


static int log_log(int level, int loglevel, char *file, unsigned long line, const char *func, const char *msg)
{
    char *prefix = "";

    if (*msg == 0)
        return 0;

    switch (level) {
        case MX_LOG_EMERG:
            prefix = "EMERGENCY: ";
            break;
        case MX_LOG_ALERT:
            prefix = "AKERT: ";
            break;
        case MX_LOG_CRIT:
            prefix = "CRITCAL ERROR: ";
            break;
        case MX_LOG_ERR:
            prefix = "ERROR: ";
            break;
        case MX_LOG_WARNING:
            prefix = "WARNING: ";
            break;
        case MX_LOG_NOTICE:
        case MX_LOG_INFO:
            prefix = "";
            break;
        case MX_LOG_DEBUG:
            prefix = "DEBUG: ";
        default:
            return -(errno=EINVAL);
    }

    if (loglevel >= MX_LOG_DEBUG)
        return mx_log_printf("%s %s:%lu:%s(): %s%s", program_invocation_short_name, file, line, func, prefix, msg);

    return mx_log_printf("%s%s", prefix, msg);
}

int mx_log_do(int level, char *file, unsigned long line, const char *func, const char *fmt, ...)
{
    int loglevel;
    int len;
    char *msg = NULL;
    va_list ap;
    int res;

    loglevel = mx_log_level_get();

    if (level > loglevel)
        return 0;

    va_start(ap, fmt);
    len = vasprintf(&msg, fmt, ap);
    va_end(ap);

    if (len == -1)
        return -(errno=ENOMEM);

    if (mx_log_log)
        res = mx_log_log(level, loglevel, file, line, func, msg);
    else
        res = log_log(level, loglevel, file, line, func, msg);

    mx_free_null(msg);
    return res;
}

int mx_log_finish(void)
{
    if (mx_log_log)
        mx_log_log(MX_LOG_NONE, MX_LOG_NONE, NULL, 0, NULL, NULL);

    if (mx_log_print)
        mx_log_print(NULL, 0);

    return 0;
}
