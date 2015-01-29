
#define _GNU_SOURCE

#include <stdio.h>
#include <mysql.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <libgen.h>

#include <sys/resource.h>

#include "mx_util.h"

#include "mxq_group.h"
#include "mxq_job.h"
#include "mxq_mysql.h"
#include "mxq.h"

#define MXQ_JOB_FIELDS "job_id, " \
                "job_status, " \
                "job_priority, " \
                "group_id, " \
                "job_workdir, " \
                "job_argc, " \
                "job_argv, " \
                "job_stdout, " \
                "job_stderr, " \
                "job_umask, " \
                "host_submit, " \
                "server_id, " \
                "host_hostname, " \
                "host_pid, " \
                "host_slots, " \
                "UNIX_TIMESTAMP(date_submit) as date_submit, " \
                "UNIX_TIMESTAMP(date_start) as date_start, " \
                "UNIX_TIMESTAMP(date_end) as date_end, " \
                "stats_status, " \
                "stats_utime_sec, " \
                "stats_utime_usec, " \
                "stats_stime_sec, " \
                "stats_stime_usec, " \
                "stats_real_sec, " \
                "stats_real_usec, " \
                "stats_maxrss, " \
                "stats_minflt, " \
                "stats_majflt, " \
                "stats_nswap, " \
                "stats_inblock, " \
                "stats_oublock, " \
                "stats_nvcsw, " \
                "stats_nivcsw"

enum mxq_job_columns {
    MXQ_JOB_COL_JOB_ID,
    MXQ_JOB_COL_JOB_STATUS,
    MXQ_JOB_COL_JOB_PRIORITY,
    MXQ_JOB_COL_GROUP_ID,
    MXQ_JOB_COL_JOB_WORKDIR,
    MXQ_JOB_COL_JOB_ARGC,
    MXQ_JOB_COL_JOB_ARGV,
    MXQ_JOB_COL_JOB_STDOUT,
    MXQ_JOB_COL_JOB_STDERR,
    MXQ_JOB_COL_JOB_UMASK,
    MXQ_JOB_COL_HOST_SUBMIT,
    MXQ_JOB_COL_SERVER_ID,
    MXQ_JOB_COL_HOST_HOSTNAME,
    MXQ_JOB_COL_HOST_PID,
    MXQ_JOB_COL_HOST_SLOTS,
    MXQ_JOB_COL_DATE_SUBMIT,
    MXQ_JOB_COL_DATE_START,
    MXQ_JOB_COL_DATE_END,
    MXQ_JOB_COL_STATS_STATUS,
    MXQ_JOB_COL_STATS_UTIME_SEC,
    MXQ_JOB_COL_STATS_UTIME_USEC,
    MXQ_JOB_COL_STATS_STIME_SEC,
    MXQ_JOB_COL_STATS_STIME_USEC,
    MXQ_JOB_COL_STATS_REAL_SEC,
    MXQ_JOB_COL_STATS_REAL_USEC,
    MXQ_JOB_COL_STATS_MAXRSS,
    MXQ_JOB_COL_STATS_MINFLT,
    MXQ_JOB_COL_STATS_MAJFLT,
    MXQ_JOB_COL_STATS_NSWAP,
    MXQ_JOB_COL_STATS_INBLOCK,
    MXQ_JOB_COL_STATS_OUBLOCK,
    MXQ_JOB_COL_STATS_NVCSW,
    MXQ_JOB_COL_STATS_NIVCSW,
    MXQ_JOB_COL__END
};

static inline int mxq_job_bind_results(MYSQL_BIND *bind, struct mxq_job *j)
{
    memset(bind, 0, sizeof(*bind)*MXQ_JOB_COL__END);

    MXQ_MYSQL_BIND_UINT64(bind, MXQ_JOB_COL_JOB_ID,       &j->job_id);
    MXQ_MYSQL_BIND_UINT16(bind, MXQ_JOB_COL_JOB_STATUS,   &j->job_status);
    MXQ_MYSQL_BIND_UINT16(bind, MXQ_JOB_COL_JOB_PRIORITY, &j->job_priority);

    MXQ_MYSQL_BIND_UINT64(bind, MXQ_JOB_COL_GROUP_ID, &j->group_id);

    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_JOB_COL_JOB_WORKDIR, &j->_job_workdir_length);

    MXQ_MYSQL_BIND_UINT16(bind, MXQ_JOB_COL_JOB_ARGC, &j->job_argc);
    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_JOB_COL_JOB_ARGV, &j->_job_argv_str_length);

    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_JOB_COL_JOB_STDOUT, &j->_job_stdout_length);
    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_JOB_COL_JOB_STDERR, &j->_job_stderr_length);

    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_JOB_UMASK, &j->job_umask);

    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_JOB_COL_HOST_SUBMIT, &j->_host_submit_length);
    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_JOB_COL_SERVER_ID,   &j->_server_id_length);

    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_JOB_COL_HOST_HOSTNAME, &j->_host_hostname_length);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_HOST_PID,      &j->host_pid);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_HOST_SLOTS,    &j->host_slots);

    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_DATE_SUBMIT, &j->date_submit);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_DATE_START,  &j->date_start);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_DATE_END,    &j->date_end);

    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_STATS_UTIME_SEC,  &j->stats_rusage.ru_utime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_STATS_UTIME_USEC, &j->stats_rusage.ru_utime.tv_usec);

    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_STATS_STIME_SEC,  &j->stats_rusage.ru_stime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_STATS_STIME_USEC,  &j->stats_rusage.ru_stime.tv_usec);

    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_STATS_REAL_SEC,   &j->stats_realtime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_JOB_COL_STATS_REAL_USEC,  &j->stats_realtime.tv_usec);

    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_STATUS,  &j->stats_status);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_MAXRSS,  &j->stats_rusage.ru_maxrss);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_MINFLT,  &j->stats_rusage.ru_minflt);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_MAJFLT,  &j->stats_rusage.ru_majflt);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_NSWAP,   &j->stats_rusage.ru_nswap);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_INBLOCK, &j->stats_rusage.ru_inblock);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_OUBLOCK, &j->stats_rusage.ru_oublock);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_NVCSW,   &j->stats_rusage.ru_nvcsw);
    MXQ_MYSQL_BIND_INT32(bind, MXQ_JOB_COL_STATS_NIVCSW,  &j->stats_rusage.ru_nivcsw);

    return 1;
}

int mxq_job_fetch_results(MYSQL_STMT *stmt, MYSQL_BIND *bind, struct mxq_job *j)
{
    int res;

    memset(j, 0, sizeof(*j));

    res = mxq_mysql_stmt_fetch_row(stmt);
    if (!res) {
        if (errno == ENOENT)
            return 0;
        perror("xxxx0");
        return -1;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_JOB_WORKDIR, &(j->job_workdir), j->_job_workdir_length);
    if (!res) {
        return -1;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_JOB_ARGV, &(j->job_argv_str), j->_job_argv_str_length);
    if (!res) {
        return -1;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_JOB_STDOUT, &(j->job_stdout), j->_job_stdout_length);
    if (!res) {
        return -1;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_JOB_STDERR, &(j->job_stderr), j->_job_stderr_length);
    if (!res) {
        return -1;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_HOST_SUBMIT, &(j->host_submit), j->_host_submit_length);
    if (!res) {
        return -1;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_SERVER_ID, &(j->server_id), j->_server_id_length);
    if (!res) {
        return -1;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_HOST_HOSTNAME, &(j->host_hostname), j->_host_hostname_length);
    if (!res) {
        return -1;
    }

    return 1;
}

void mxq_job_free_content(struct mxq_job *j)
{
        free_null(j->job_workdir);
        j->_job_workdir_length = 0;

        free_null(j->job_argv_str);
        j->_job_argv_str_length = 0;

        free_null(j->job_stdout);
        j->_job_stdout_length = 0;

        free_null(j->job_stderr);
        j->_job_stderr_length = 0;

        if (j->tmp_stderr == j->tmp_stdout) {
            j->tmp_stdout = NULL;
        } else {
            free_null(j->tmp_stdout);
        }
        free_null(j->tmp_stderr);

        free_null(j->host_submit);
        j->_host_submit_length = 0;

        free_null(j->server_id);
        j->_server_id_length = 0;

        free_null(j->host_hostname);
        j->_host_hostname_length = 0;

        free_null(j->job_argv);
        j->job_argv = NULL;
}

int mxq_job_update_status_assigned(MYSQL *mysql, struct mxq_job *job)
{
    char *query;
    MYSQL_BIND param[3];
    int   res;

    memset(param, 0, sizeof(param));

    query = "UPDATE mxq_job SET"
            " job_status = " status_str(MXQ_JOB_STATUS_ASSIGNED) ","
            " host_hostname = ?,"
            " server_id = ?"
            " WHERE group_id = ?"
            " AND job_status = " status_str(MXQ_JOB_STATUS_INQ)
            " AND host_hostname = ''"
            " AND server_id = ''"
            " AND host_pid = 0"
            " ORDER BY job_priority, job_id"
            " LIMIT 1";

    MXQ_MYSQL_BIND_STRING(param, 0, job->host_hostname);
    MXQ_MYSQL_BIND_STRING(param, 1, job->server_id);
    MXQ_MYSQL_BIND_UINT64(param, 2, &job->group_id);

    res = mxq_mysql_do_update(mysql, query, param);

    job->job_status = MXQ_JOB_STATUS_ASSIGNED;

    return res;
}

int mxq_job_update_status_loaded(MYSQL *mysql, struct mxq_job *job)
{
    char *query;
    MYSQL_BIND param[3];
    int   res;

    memset(param, 0, sizeof(param));

    query = "UPDATE mxq_job SET"
            " job_status = " status_str(MXQ_JOB_STATUS_LOADED)
            " WHERE job_id = ?"
            " AND job_status = " status_str(MXQ_JOB_STATUS_ASSIGNED)
            " AND host_hostname = ?"
            " AND server_id = ?"
            " AND host_pid = 0";

    MXQ_MYSQL_BIND_UINT64(param, 0, &job->job_id);
    MXQ_MYSQL_BIND_STRING(param, 1, job->host_hostname);
    MXQ_MYSQL_BIND_STRING(param, 2, job->server_id);

    res = mxq_mysql_do_update(mysql, query, param);

    job->job_status = MXQ_JOB_STATUS_LOADED;

    return res;
}

int mxq_job_update_status_running(MYSQL *mysql, struct mxq_job *job)
{
    char *query;
    MYSQL_BIND param[5];
    int   res;

    memset(param, 0, sizeof(param));

    if (job->job_status != MXQ_JOB_STATUS_LOADED) {
        MXQ_LOG_WARNING("new status==runnning but old status(=%d) is != loaded ", job->job_status);
        if (job->job_status != MXQ_JOB_STATUS_ASSIGNED) {
            MXQ_LOG_ERROR("new status==runnning but old status(=%d) is != (loaded || assigned). Aborting Status change.", job->job_status);
            errno = EINVAL;
            return -1;
        }
    }
    query = "UPDATE mxq_job SET"
            " job_status = " status_str(MXQ_JOB_STATUS_RUNNING) ","
            " date_start = NULL,"
            " host_pid = ?,"
            " host_slots = ?"
            " WHERE job_id = ?"
            " AND job_status IN (" status_str(MXQ_JOB_STATUS_ASSIGNED) ", " status_str(MXQ_JOB_STATUS_LOADED) ")"
            " AND host_hostname = ?"
            " AND server_id = ?"
            " AND host_pid = 0";

    MXQ_MYSQL_BIND_UINT32(param, 0, &job->host_pid);
    MXQ_MYSQL_BIND_UINT32(param, 1, &job->host_slots);
    MXQ_MYSQL_BIND_UINT64(param, 2, &job->job_id);
    MXQ_MYSQL_BIND_STRING(param, 3, job->host_hostname);
    MXQ_MYSQL_BIND_STRING(param, 4, job->server_id);

    res = mxq_mysql_do_update(mysql, query, param);

    job->job_status = MXQ_JOB_STATUS_RUNNING;

    return res;
}

int mxq_job_update_status_exit(MYSQL *mysql, struct mxq_job *job)
{
    char *query;
    MYSQL_BIND param[20];
    uint16_t newstatus;
    int   res;

    memset(param, 0, sizeof(param));

    if (WIFEXITED(job->stats_status)) {
        if (WEXITSTATUS(job->stats_status)) {
            newstatus = MXQ_JOB_STATUS_FAILED;
        } else {
            newstatus = MXQ_JOB_STATUS_FINISHED;
        }
    } else if(WIFSIGNALED(job->stats_status)) {
        newstatus = MXQ_JOB_STATUS_KILLED;
    } else {
        MXQ_LOG_ERROR("Status change to status_exit called with unknown stats_status (%d). Aborting Status change.", job->stats_status);
        errno = EINVAL;
        return -1;
    }
    query = "UPDATE mxq_job SET"
            " job_status = ?,"
            " date_end = NULL,"
            " stats_status = ?, "
            " stats_utime_sec = ?, "
            " stats_utime_usec = ?, "
            " stats_stime_sec = ?, "
            " stats_stime_usec = ?, "
            " stats_real_sec = ?, "
            " stats_real_usec = ?, "
            " stats_maxrss = ?, "
            " stats_minflt = ?, "
            " stats_majflt = ?, "
            " stats_nswap = ?, "
            " stats_inblock = ?, "
            " stats_oublock = ?, "
            " stats_nvcsw = ?, "
            " stats_nivcsw = ?"
            " WHERE job_id = ?"
            " AND job_status = " status_str(MXQ_JOB_STATUS_RUNNING)
            " AND host_hostname = ?"
            " AND server_id = ?"
            " AND host_pid = ?";

    MXQ_MYSQL_BIND_UINT16(param, 0, &newstatus);

    MXQ_MYSQL_BIND_INT32(param,  1, &job->stats_status);

    MXQ_MYSQL_BIND_UINT32(param, 2, &job->stats_rusage.ru_utime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(param, 3, &job->stats_rusage.ru_utime.tv_usec);

    MXQ_MYSQL_BIND_UINT32(param, 4, &job->stats_rusage.ru_stime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(param, 5, &job->stats_rusage.ru_stime.tv_usec);

    MXQ_MYSQL_BIND_UINT32(param, 6, &job->stats_realtime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(param, 7, &job->stats_realtime.tv_usec);

    MXQ_MYSQL_BIND_INT32(param,  8, &job->stats_rusage.ru_maxrss);
    MXQ_MYSQL_BIND_INT32(param,  9, &job->stats_rusage.ru_minflt);
    MXQ_MYSQL_BIND_INT32(param, 10, &job->stats_rusage.ru_majflt);
    MXQ_MYSQL_BIND_INT32(param, 11, &job->stats_rusage.ru_nswap);
    MXQ_MYSQL_BIND_INT32(param, 12, &job->stats_rusage.ru_inblock);
    MXQ_MYSQL_BIND_INT32(param, 13, &job->stats_rusage.ru_oublock);
    MXQ_MYSQL_BIND_INT32(param, 14, &job->stats_rusage.ru_nvcsw);
    MXQ_MYSQL_BIND_INT32(param, 15, &job->stats_rusage.ru_nivcsw);

    MXQ_MYSQL_BIND_UINT64(param, 16, &job->job_id);
    MXQ_MYSQL_BIND_STRING(param, 17,  job->host_hostname);
    MXQ_MYSQL_BIND_STRING(param, 18,  job->server_id);
    MXQ_MYSQL_BIND_UINT32(param, 19, &job->host_pid);

    res = mxq_mysql_do_update(mysql, query, param);

    job->job_status = newstatus;

    return res;
}

int mxq_job_set_tmpfilenames(struct mxq_group *g, struct mxq_job *j)
{
    int res;

    if (!streq(j->job_stdout, "/dev/null")) {
        _cleanup_free_ char *dir = NULL;

        dir = mx_dirname_forever(j->job_stdout);

        mx_asprintf_forever(&j->tmp_stdout, "%s/mxq.%u.%lu.%lu.%s.%s.%d.stdout.tmp",
            dir, g->user_uid, g->group_id, j->job_id, j->host_hostname,
            j->server_id, j->host_pid);
    }

    if (!streq(j->job_stderr, "/dev/null")) {
        _cleanup_free_ char *dir = NULL;

        if (streq(j->job_stderr, j->job_stdout)) {
            j->tmp_stderr = j->tmp_stdout;
            return 1;
        }
        dir = mx_dirname_forever(j->job_stderr);

        mx_asprintf_forever(&j->tmp_stderr, "%s/mxq.%u.%lu.%lu.%s.%s.%d.stderr.tmp",
            dir, g->user_uid, g->group_id, j->job_id, j->host_hostname,
            j->server_id, j->host_pid);
    }
    return 1;
}


int mxq_job_load_assigned(MYSQL *mysql, struct mxq_job *job, char *hostname, char *server_id)
{
    MYSQL_STMT *stmt;
    MYSQL_BIND result[MXQ_JOB_COL__END];
    char *query;
    MYSQL_BIND param[2];
    int res;

    assert(hostname);
    assert(server_id);

    memset(param, 0, sizeof(param));
    MXQ_MYSQL_BIND_STRING(param, 0, hostname);
    MXQ_MYSQL_BIND_STRING(param, 1, server_id);

    mxq_job_bind_results(result, job);

    query = "SELECT " MXQ_JOB_FIELDS " FROM mxq_job"
            " WHERE job_status = " status_str(MXQ_JOB_STATUS_ASSIGNED)
            " AND host_hostname = ?"
            " AND server_id  = ?"
            " LIMIT 1";

    stmt = mxq_mysql_stmt_do_query(mysql, query, MXQ_JOB_COL__END, param, result);
    if (!stmt) {
        MXQ_LOG_ERROR("mxq_job_load_assigned(mysql=%p, stmt_str=\"%s\", field_count=%d, param=%p, result=%p)\n", mysql, query, MXQ_JOB_COL__END, param, result);
        return -1;
    }

    res = mxq_job_fetch_results(stmt, result, job);
    if (res < 0) {
        mxq_mysql_print_error(mysql);
        mxq_mysql_stmt_print_error(stmt);
        MXQ_LOG_ERROR("mxq_job_fetch_results..\n");
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_close(stmt);
    return res;
}

int mxq_job_load(MYSQL *mysql, struct mxq_job *mxqjob, uint64_t group_id, char *hostname, char *server_id)
{
    int res;

    memset(mxqjob, 0, sizeof(*mxqjob));

    do {
        res = mxq_job_load_assigned(mysql, mxqjob, hostname, server_id);
        if(res < 0) {
            MXQ_LOG_ERROR("  group_id=%lu job_id=%lu :: mxq_job_load_assigned: %m\n", group_id, mxqjob->job_id);
            return 0;
        }
        if(res == 1) {
            break;
        }

        mxqjob->host_hostname = hostname;
        mxqjob->server_id     = server_id;
        mxqjob->group_id      = group_id;
        mxqjob->job_status    = MXQ_JOB_STATUS_INQ;

        res = mxq_job_update_status_assigned(mysql, mxqjob);
        if (res < 0) {
            if (errno == ENOENT) {
                MXQ_LOG_WARNING("  group_id=%lu :: mxq_job_update_status_assigned(): No matching job found - maybe another server was a bit faster. ;)\n", group_id);
            } else {
                MXQ_LOG_ERROR("  group_id=%lu :: mxq_job_update_status_assigned(): %m\n", group_id);
            }
            return 0;
        }
    } while (1);

    res = mxq_job_update_status_loaded(mysql, mxqjob);
    if (res < 0) {
        MXQ_LOG_ERROR("  group_id=%lu job_id=%lu :: mxq_job_update_status_loaded(): %m\n", group_id, mxqjob->job_id);
        return 0;
    }

    return 1;
}

int mxq_job_update_status_cancelled_by_group(MYSQL *mysql, struct mxq_group *group)
{
    char *query;
    MYSQL_BIND param[1];
    int   res;

    assert(group->group_id);

    memset(param, 0, sizeof(param));

    query = "UPDATE mxq_job SET"
            " job_status = " status_str(MXQ_JOB_STATUS_CANCELLED)
            " WHERE group_id = ?"
            " AND job_status = " status_str(MXQ_JOB_STATUS_INQ)
            " AND host_hostname = ''"
            " AND server_id = ''"
            " AND host_pid = 0";

    MXQ_MYSQL_BIND_UINT64(param, 0, &group->group_id);

    res = mxq_mysql_do_update(mysql, query, param);

    return res;
}
