#ifndef __MX_LOG_H__
#define __MX_LOG_H__ 1

#include <syslog.h>

#define MX_LOG_SYSLOG_TO_MXLOG(x)    ((x)+1)
#define MX_LOG_MXLOG_TO_SYSLOG(x)    ((x)-1)

#if !defined mx_free_null
#   include <stdlib.h>
#   define mx_free_null(a) do { free((a)); (a) = NULL; } while(0)
#endif

#define MX_LOG_NONE    0
#define MX_LOG_EMERG   MX_LOG_SYSLOG_TO_MXLOG(LOG_EMERG)   /* system is unusable */
#define MX_LOG_ALERT   MX_LOG_SYSLOG_TO_MXLOG(LOG_ALERT)   /* action must be taken immediately */
#define MX_LOG_CRIT    MX_LOG_SYSLOG_TO_MXLOG(LOG_CRIT)    /* critical conditions */
#define MX_LOG_ERR     MX_LOG_SYSLOG_TO_MXLOG(LOG_ERR)     /* error conditions */
#define MX_LOG_WARNING MX_LOG_SYSLOG_TO_MXLOG(LOG_WARNING) /* warning conditions */
#define MX_LOG_NOTICE  MX_LOG_SYSLOG_TO_MXLOG(LOG_NOTICE)  /* normal but significant condition */
#define MX_LOG_INFO    MX_LOG_SYSLOG_TO_MXLOG(LOG_INFO)    /* informational */
#define MX_LOG_DEBUG   MX_LOG_SYSLOG_TO_MXLOG(LOG_DEBUG)   /* debug-level messages */

#define mx_log(level, fmt, ...)  mx_log_do((level), __FILE__, __LINE__, __func__, (fmt), ##__VA_ARGS__)
#define mx_logva(level, fmt, ap) mx_logva_do((level), __FILE__, __LINE__, __func__, (fmt), (ap))

#define mx_log_fatal(fmt, ...)   mx_log(MX_LOG_EMERG,   (fmt), ##__VA_ARGS__)
#define mx_log_emerg(fmt, ...)   mx_log(MX_LOG_EMERG,   (fmt), ##__VA_ARGS__)
#define mx_log_alert(fmt, ...)   mx_log(MX_LOG_ALERT,   (fmt), ##__VA_ARGS__)
#define mx_log_crit(fmt, ...)    mx_log(MX_LOG_CRIT,    (fmt), ##__VA_ARGS__)
#define mx_log_err(fmt, ...)     mx_log(MX_LOG_ERR,     (fmt), ##__VA_ARGS__)
#define mx_log_warning(fmt, ...) mx_log(MX_LOG_WARNING, (fmt), ##__VA_ARGS__)
#define mx_log_notice(fmt, ...)  mx_log(MX_LOG_NOTICE,  (fmt), ##__VA_ARGS__)
#define mx_log_info(fmt, ...)    mx_log(MX_LOG_INFO,    (fmt), ##__VA_ARGS__)
#define mx_log_debug(fmt, ...)   mx_log(MX_LOG_DEBUG,   (fmt), ##__VA_ARGS__)

#define mx_logva_fatal(fmt, ap)   mx_log(MX_LOG_EMERG,   (fmt), (ap))
#define mx_logva_emerg(fmt, ap)   mx_log(MX_LOG_EMERG,   (fmt), (ap))
#define mx_logva_alert(fmt, ap)   mx_log(MX_LOG_ALERT,   (fmt), (ap))
#define mx_logva_crit(fmt, ap)    mx_log(MX_LOG_CRIT,    (fmt), (ap))
#define mx_logva_err(fmt, ap)     mx_log(MX_LOG_ERR,     (fmt), (ap))
#define mx_logva_warning(fmt, ap) mx_log(MX_LOG_WARNING, (fmt), (ap))
#define mx_logva_notice(fmt, ap)  mx_log(MX_LOG_NOTICE,  (fmt), (ap))
#define mx_logva_info(fmt, ap)    mx_log(MX_LOG_INFO,    (fmt), (ap))
#define mx_logva_debug(fmt, ap)   mx_log(MX_LOG_DEBUG,   (fmt), (ap))

int mx_log_log(int level, int loglevel, char *file, unsigned long line, const char *func, const char *msg) __attribute__ ((weak));
int mx_log_print(char *msg, size_t len) __attribute__ ((weak));

int mx_log_level_set(int level);
int mx_log_level_get(void);

int mx_log_level_mxlog_to_syslog(int level);
int mx_log_level_syslog_to_mxlog(int level);

int mx_log_do(int level, char *file, unsigned long line, const char *func, const char *fmt, ...) __attribute__ ((format(printf, 5, 6)));;
int mx_log_printf(const char *fmt, ...);

int mx_log_finish(void);

#endif
