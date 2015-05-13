#include <stdio.h>
#include <assert.h>
#include <errno.h>

#include <syslog.h>

#include "mx_log.h"

int mx_log_print(char *msg, size_t len)
{
    mx_free_null(msg);
    return len;
}

static int test_mx_log(void)
{
    int cnt = 0;

    cnt += !!mx_log(MX_LOG_EMERG,   "emerg %s",   "emerg");
    cnt += !!mx_log(MX_LOG_ALERT,   "alert %s",   "alert");
    cnt += !!mx_log(MX_LOG_CRIT,    "crit %s",    "crit");
    cnt += !!mx_log(MX_LOG_ERR,     "err %s",     "err");
    cnt += !!mx_log(MX_LOG_WARNING, "warning %s", "warning");
    cnt += !!mx_log(MX_LOG_NOTICE,  "notice %s",  "notice");
    cnt += !!mx_log(MX_LOG_INFO,    "info %s",    "info");
    cnt += !!mx_log(MX_LOG_DEBUG,   "debug %s",   "debug");

    return cnt;
}

static int test_mx_log_shortname(void)
{
    int cnt = 0;

    cnt += !!mx_log_fatal("_emerg %s",   "fatal");
    cnt += !!mx_log_emerg("_emerg %s",   "emerg");
    cnt += !!mx_log_alert("_alert %s",   "alert");
    cnt += !!mx_log_crit("_crit %s",    "crit");
    cnt += !!mx_log_err("_err %s",     "err");
    cnt += !!mx_log_warning("_warning %s", "warning");
    cnt += !!mx_log_notice("_notice %s",  "notice");
    cnt += !!mx_log_info("_info %s",    "info");
    cnt += !!mx_log_debug("_debug %s",   "debug");

    return cnt;
}

static int test_mx_log_level(int level)
{
    int x;
    int curr;

    curr = mx_log_level_get();

    assert(mx_log_level_set(level) == curr);
    assert(mx_log_level_get() == level);
    assert(test_mx_log_shortname() == level+(level>MX_LOG_NONE));
    assert((x = test_mx_log()) == level);

    return x;
}

static void test_mx_log_level_mxlog_to_syslog(void)
{
    errno=0;
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_NONE) == -EINVAL);
    assert(errno == 0);
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_EMERG) == LOG_EMERG);
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_ALERT) == LOG_ALERT);
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_CRIT) == LOG_CRIT);
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_ERR) == LOG_ERR);
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_WARNING) == LOG_WARNING);
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_NOTICE) == LOG_NOTICE);
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_INFO) == LOG_INFO);
    assert(mx_log_level_mxlog_to_syslog(MX_LOG_DEBUG) == LOG_DEBUG);
}

static void test_mx_log_level_syslog_to_mxlog(void)
{
    errno=0;
    assert(mx_log_level_syslog_to_mxlog(LOG_EMERG-1) == -EINVAL);
    assert(errno == 0);
    errno=0;
    assert(mx_log_level_syslog_to_mxlog(LOG_DEBUG+1) == -EINVAL);
    assert(errno == 0);
    assert(mx_log_level_syslog_to_mxlog(LOG_EMERG) == MX_LOG_EMERG);
    assert(mx_log_level_syslog_to_mxlog(LOG_ALERT) == MX_LOG_ALERT);
    assert(mx_log_level_syslog_to_mxlog(LOG_CRIT) == MX_LOG_CRIT);
    assert(mx_log_level_syslog_to_mxlog(LOG_ERR) == MX_LOG_ERR);
    assert(mx_log_level_syslog_to_mxlog(LOG_WARNING) == MX_LOG_WARNING);
    assert(mx_log_level_syslog_to_mxlog(LOG_NOTICE) == MX_LOG_NOTICE);
    assert(mx_log_level_syslog_to_mxlog(LOG_INFO) == MX_LOG_INFO);
    assert(mx_log_level_syslog_to_mxlog(LOG_DEBUG) == MX_LOG_DEBUG);
}

int main(int argc, char *argv[])
{
    test_mx_log_level_syslog_to_mxlog();
    test_mx_log_level_mxlog_to_syslog();

    assert(mx_log_level_get() == MX_LOG_WARNING);
    assert(mx_log_level_set(1000) == -EINVAL);
    assert(mx_log_level_get() == MX_LOG_WARNING);
    assert(test_mx_log() == 5);

    assert(mx_log(MX_LOG_EMERG, "") == 0);

    assert(test_mx_log_level(MX_LOG_NONE)    == 0);
    assert(test_mx_log_level(MX_LOG_EMERG)   == 1);
    assert(test_mx_log_level(MX_LOG_ALERT)   == 2);
    assert(test_mx_log_level(MX_LOG_CRIT)    == 3);
    assert(test_mx_log_level(MX_LOG_ERR)     == 4);
    assert(test_mx_log_level(MX_LOG_WARNING) == 5);
    assert(test_mx_log_level(MX_LOG_NOTICE)  == 6);
    assert(test_mx_log_level(MX_LOG_INFO)    == 7);
    assert(test_mx_log_level(MX_LOG_DEBUG)   == 8);

    return 0;
}
