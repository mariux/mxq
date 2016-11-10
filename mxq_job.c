#define _GNU_SOURCE

#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <libgen.h>
#include <string.h>

#include <sys/resource.h>

#include "mx_util.h"
#include "mx_log.h"
#include "mx_util.h"

#include "mxq_daemon.h"
#include "mxq_group.h"
#include "mxq_job.h"

#define JOB_FIELDS_CNT 38
#define JOB_FIELDS \
                " job_id, " \
                " job_status, " \
                " job_flags, " \
                " job_priority, " \
                " group_id, " \
                " job_workdir, " \
                " job_argc, " \
                " job_argv, " \
                " job_stdout, " \
                " job_stderr, " \
                " job_umask, " \
                " host_submit, " \
                " host_id, " \
                " daemon_id, " \
                " server_id, " \
                " host_hostname, " \
                " host_pid, " \
                " host_slots, " \
                " host_cpu_set, " \
                " UNIX_TIMESTAMP(date_submit) as date_submit, " \
                " UNIX_TIMESTAMP(date_start) as date_start, " \
                " UNIX_TIMESTAMP(date_end) as date_end, " \
                " stats_max_sumrss, " \
                " stats_status, " \
                " stats_utime_sec, " \
                " stats_utime_usec, " \
                " stats_stime_sec, " \
                " stats_stime_usec, " \
                " stats_real_sec, " \
                " stats_real_usec, " \
                " stats_maxrss, " \
                " stats_minflt, " \
                " stats_majflt, " \
                " stats_nswap, " \
                " stats_inblock, " \
                " stats_oublock, " \
                " stats_nvcsw, " \
                " stats_nivcsw"

static int bind_result_job_fields(struct mx_mysql_bind *result, struct mxq_job *j)
{
    int res = 0;
    int idx = 0;

    res = mx_mysql_bind_init_result(result, JOB_FIELDS_CNT);
    assert(res >= 0);

    res += mx_mysql_bind_var(result, idx++, uint64, &(j->job_id));
    res += mx_mysql_bind_var(result, idx++, uint16, &(j->job_status));
    res += mx_mysql_bind_var(result, idx++, uint64, &(j->job_flags));
    res += mx_mysql_bind_var(result, idx++, uint16, &(j->job_priority));
    res += mx_mysql_bind_var(result, idx++, uint64, &(j->group_id));
    res += mx_mysql_bind_var(result, idx++, string, &(j->job_workdir));
    res += mx_mysql_bind_var(result, idx++, uint32, &(j->job_argc));
    res += mx_mysql_bind_var(result, idx++, string, &(j->job_argv_str));
    res += mx_mysql_bind_var(result, idx++, string, &(j->job_stdout));
    res += mx_mysql_bind_var(result, idx++, string, &(j->job_stderr));
    res += mx_mysql_bind_var(result, idx++, uint32, &(j->job_umask));
    res += mx_mysql_bind_var(result, idx++, string, &(j->host_submit));
    res += mx_mysql_bind_var(result, idx++, string, &(j->host_id));
    res += mx_mysql_bind_var(result, idx++, uint32, &(j->daemon_id));
    res += mx_mysql_bind_var(result, idx++, string, &(j->daemon_name));
    res += mx_mysql_bind_var(result, idx++, string, &(j->host_hostname));
    res += mx_mysql_bind_var(result, idx++, uint32, &(j->host_pid));
    res += mx_mysql_bind_var(result, idx++, uint32, &(j->host_slots));
    res += mx_mysql_bind_var(result, idx++, string, &(j->host_cpu_set_str));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->date_submit));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->date_start));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->date_end));
    res += mx_mysql_bind_var(result, idx++, uint64, &(j->stats_max_sumrss));
    res += mx_mysql_bind_var(result, idx++,  int32, &(j->stats_status));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_utime.tv_sec));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_utime.tv_usec));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_stime.tv_sec));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_stime.tv_usec));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_realtime.tv_sec));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_realtime.tv_usec));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_maxrss));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_minflt));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_majflt));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_nswap));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_inblock));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_oublock));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_nvcsw));
    res += mx_mysql_bind_var(result, idx++,  int64, &(j->stats_rusage.ru_nivcsw));

    return res;
}

char *mxq_job_status_to_name(uint64_t status)
{
    switch (status) {
        case MXQ_JOB_STATUS_INQ:
            return "inq";
        case MXQ_JOB_STATUS_ASSIGNED:
            return "assigned";
        case MXQ_JOB_STATUS_LOADED:
            return "loaded";
        case MXQ_JOB_STATUS_RUNNING:
            return "running";
        case MXQ_JOB_STATUS_UNKNOWN_RUN:
            return "running-unknown";
        case MXQ_JOB_STATUS_EXTRUNNING:
            return "running-external";
        case MXQ_JOB_STATUS_STOPPED:
            return "stopped";
        case MXQ_JOB_STATUS_EXIT:
            return "exited";
        case MXQ_JOB_STATUS_KILLING:
            return "killing";
        case MXQ_JOB_STATUS_KILLED:
            return "killed";
        case MXQ_JOB_STATUS_FAILED:
            return "failed";
        case MXQ_JOB_STATUS_UNKNOWN_PRE:
            return "unknownpre";
        case MXQ_JOB_STATUS_CANCELLED:
            return "cancelled";
        case MXQ_JOB_STATUS_CANCELLING:
            return "cancelling";
        case MXQ_JOB_STATUS_UNKNOWN:
            return "unknown";
        case MXQ_JOB_STATUS_FINISHED:
            return "finished";
    }

    return "invalid";
}

void mxq_job_free_content(struct mxq_job *j)
{
        mx_free_null(j->job_workdir);
        mx_free_null(j->job_argv);
        mx_free_null(j->job_argv_str);
        mx_free_null(j->job_stdout);
        mx_free_null(j->job_stderr);

        if (j->tmp_stderr == j->tmp_stdout) {
            j->tmp_stdout = NULL;
        } else {
            mx_free_null(j->tmp_stdout);
        }
        mx_free_null(j->tmp_stderr);
        mx_free_null(j->host_submit);
        mx_free_null(j->host_id);
        mx_free_null(j->daemon_name);
        mx_free_null(j->host_hostname);
        mx_free_null(j->host_cpu_set_str);
}

static int do_jobs_statement(struct mx_mysql *mysql, char *query, struct mx_mysql_bind *param, struct mxq_job **jobs)
{
    int res,i;
    struct mxq_job j = {0};
    struct mx_mysql_bind result = {0};

    res = bind_result_job_fields(&result, &j);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, param, &result, &j, (void **)jobs, sizeof(**jobs));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }
    for (i=0;i<res;i++)
        mx_str_to_cpuset(&(*jobs)[i].host_cpu_set,(*jobs)[i].host_cpu_set_str);
    return res;
}

int mxq_load_job(struct mx_mysql *mysql, struct mxq_job **jobs_result, uint64_t job_id)
{
    struct mxq_job *jobs_tmp = NULL;
    struct mx_mysql_bind param = {0};
    int idx;
    int res;

    assert(mysql);
    assert(jobs_result);
    assert(!(*jobs_result));

    char *query =
            "SELECT"
                JOB_FIELDS
            " FROM"
                " mxq_job"
            " WHERE job_id = ?"
            " LIMIT 1";

    res = mx_mysql_bind_init_param(&param, 1);
    assert(res == 0);

    idx  = 0;
    res  = 0;
    res += mx_mysql_bind_var(&param, idx++, uint64, &job_id);
    assert(res == 0);

    res = do_jobs_statement(mysql, query, &param, &jobs_tmp);
    if (res >= 0)
        *jobs_result = jobs_tmp;

    return res;
}

int mxq_load_jobs_in_group(struct mx_mysql *mysql, struct mxq_job **jobs_result, struct mxq_group *grp)
{
    int res;
    struct mxq_job *jobs_tmp = NULL;
    struct mx_mysql_bind param = {0};

    assert(mysql);
    assert(jobs_result);
    assert(!(*jobs_result));

    char *query =
            "SELECT"
                JOB_FIELDS
            " FROM mxq_job"
            " WHERE group_id = ? OR 1 = 0"
            " ORDER BY server_id, host_hostname, job_id";

    res = mx_mysql_bind_init_param(&param, 1);
    assert(res == 0);

    res = mx_mysql_bind_var(&param, 0, uint64, &(grp->group_id));
    assert(res == 0);

    res = do_jobs_statement(mysql, query, &param, &jobs_tmp);
    if (res >= 0)
        *jobs_result = jobs_tmp;
    return res;
}

int mxq_load_jobs_in_group_with_status(struct mx_mysql *mysql, struct mxq_job **jobs_result, struct mxq_group *grp, uint64_t job_status)
{
    struct mxq_job *jobs_tmp = NULL;
    struct mx_mysql_bind param = {0};
    int idx;
    int res;

    assert(mysql);
    assert(jobs_result);
    assert(!(*jobs_result));

    char *query =
            "SELECT"
                JOB_FIELDS
            " FROM"
                " mxq_job"
            " WHERE group_id = ?"
              " AND job_status = ?"
            " ORDER BY"
                " server_id,"
                " host_hostname,"
                " job_id";

    res = mx_mysql_bind_init_param(&param, 2);
    assert(res == 0);

    idx  = 0;
    res  = 0;
    res += mx_mysql_bind_var(&param, idx++, uint64, &(grp->group_id));
    res += mx_mysql_bind_var(&param, idx++, uint64, &job_status);
    assert(res == 0);

    res = do_jobs_statement(mysql, query, &param, &jobs_tmp);
    if (res >= 0)
        *jobs_result = jobs_tmp;

    return res;
}

int mxq_assign_job_from_group_to_daemon(struct mx_mysql *mysql, uint64_t group_id, struct mxq_daemon *daemon)
{
    struct mx_mysql_bind param = {0};
    int res;
    int idx;

    assert(mysql);
    assert(daemon);
    assert(daemon->hostname);
    assert(*daemon->hostname);
    assert(daemon->daemon_name);
    assert(*daemon->daemon_name);
    assert(daemon->daemon_id);

    char *query =
            "UPDATE "
                " mxq_job"
            " SET"
                " daemon_id     = ?,"
                " host_hostname = ?,"
                " server_id     = ?,"
                " job_status    = " status_str(MXQ_JOB_STATUS_ASSIGNED)
            " WHERE group_id      = ?"
              " AND job_status    = " status_str(MXQ_JOB_STATUS_INQ)
              " AND daemon_id     = 0"
              " AND host_hostname = ''"
              " AND server_id     = ''"
              " AND host_pid      = 0"
            " ORDER BY"
              " job_priority,"
              " job_id"
            " LIMIT 1";

    res = mx_mysql_bind_init_param(&param, 4);
    assert(res == 0);

    idx = 0;
    res = 0;
    res += mx_mysql_bind_var(&param, idx++, uint32, &daemon->daemon_id);
    res += mx_mysql_bind_var(&param, idx++, string, &daemon->hostname);
    res += mx_mysql_bind_var(&param, idx++, string, &daemon->daemon_name);
    res += mx_mysql_bind_var(&param, idx++, uint64, &group_id);
    assert(res == 0);

    res = mx_mysql_do_statement_noresult(mysql, query, &param);
    if (res < 0)
        mx_log_err("mx_mysql_do_statement(): %m");

    return res;
}

int mxq_unassign_jobs_of_server(struct mx_mysql *mysql, struct mxq_daemon *daemon)
{
    struct mx_mysql_bind param = {0};
    int res;
    int idx;

    assert(mysql);

    assert(daemon->hostname);
    assert(daemon->daemon_name);

    assert(*daemon->hostname);
    assert(*daemon->daemon_name);

    char *query =
            "UPDATE"
                " mxq_job"
            " SET"
                " daemon_id  = 0,"
                " job_status = " status_str(MXQ_JOB_STATUS_INQ)
            " WHERE job_status    = " status_str(MXQ_JOB_STATUS_ASSIGNED)
              " AND host_pid      = 0"
              " AND host_hostname = ?"
              " AND server_id     = ?";

    res = mx_mysql_bind_init_param(&param, 2);
    assert(res == 0);

    idx = 0;
    res = 0;
    res += mx_mysql_bind_var(&param, idx++, string, &daemon->hostname);
    res += mx_mysql_bind_var(&param, idx++, string, &daemon->daemon_name);
    assert(res == 0);

    res = mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    return res;
}

int mxq_set_job_status_loaded(struct mx_mysql *mysql, struct mxq_job *job)
{
    struct mx_mysql_bind param = {0};
    char *host_id;
    int res;
    int idx;

    assert(mysql);
    assert(job);
    assert(job->job_id);
    assert(job->daemon_id);

    mx_asprintf_forever(&host_id, "%u", job->daemon_id);

    char *query =
            "UPDATE"
                " mxq_job"
            " SET"
              " host_id    = ?,"
              " job_status = " status_str(MXQ_JOB_STATUS_LOADED)
            " WHERE job_status = " status_str(MXQ_JOB_STATUS_ASSIGNED)
              " AND job_id     = ?"
              " AND daemon_id  = ?"
              " AND host_pid   = 0";

    res = mx_mysql_bind_init_param(&param, 3);
    assert(res == 0);

    idx = 0;
    res = 0;
    res += mx_mysql_bind_var(&param, idx++, string, &host_id);
    res += mx_mysql_bind_var(&param, idx++, uint64, &(job->job_id));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(job->daemon_id));
    assert(res == 0);

    res = mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    mx_free_null(job->host_id);

    job->host_id    = host_id;
    job->job_status = MXQ_JOB_STATUS_LOADED;

    return res;
}

int mxq_set_job_status_running(struct mx_mysql *mysql, struct mxq_job *job)
{
    assert(mysql);

    assert(job);
    assert(job->job_id);
    assert(job->daemon_id);
    assert(job->host_pid);
    assert(job->host_slots);
    assert(job->host_cpu_set_str);

    assert(*job->host_cpu_set_str);

    struct mx_mysql_bind param = {0};
    int res;
    int idx;

    char *query =
            "UPDATE"
                " mxq_job"
            " SET"
                " host_pid     = ?,"
                " host_slots   = ?,"
                " host_cpu_set = ?,"
                " job_status   = " status_str(MXQ_JOB_STATUS_RUNNING)
            " WHERE job_status    = " status_str(MXQ_JOB_STATUS_LOADED)
              " AND job_id        = ?"
              " AND daemon_id     = ?"
              " AND host_pid      = 0";

    res = mx_mysql_bind_init_param(&param, 5);
    assert(res == 0);

    idx  = 0;
    res  = 0;
    res += mx_mysql_bind_var(&param, idx++, uint32, &(job->host_pid));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(job->host_slots));
    res += mx_mysql_bind_var(&param, idx++, string, &(job->host_cpu_set_str));

    res += mx_mysql_bind_var(&param, idx++, uint64, &(job->job_id));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(job->daemon_id));
    assert(res == 0);

    res = mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    job->job_status = MXQ_JOB_STATUS_RUNNING;

    return res;
}

int mxq_set_job_status_exited(struct mx_mysql *mysql, struct mxq_job *job)
{
    int res;
    int idx;
    uint16_t newstatus;
    struct mx_mysql_bind param = {0};

    assert(mysql);
    assert(job);
    assert(job->host_pid);

    if (WIFEXITED(job->stats_status)) {
        if (WEXITSTATUS(job->stats_status)) {
            newstatus = MXQ_JOB_STATUS_FAILED;
        } else {
            newstatus = MXQ_JOB_STATUS_FINISHED;
        }
    } else if(WIFSIGNALED(job->stats_status)) {
        newstatus = MXQ_JOB_STATUS_KILLED;
    } else {
        mx_log_err("Status change to status_exit called with unknown stats_status (%d). Aborting Status change.", job->stats_status);
        errno = EINVAL;
        return -1;
    }

    char *query =
            "UPDATE"
                " mxq_job"
            " SET"
                " stats_max_sumrss = ?,"
                " stats_status     = ?,"
                " stats_utime_sec  = ?,"
                " stats_utime_usec = ?,"
                " stats_stime_sec  = ?,"
                " stats_stime_usec = ?,"
                " stats_real_sec   = ?,"
                " stats_real_usec  = ?,"
                " stats_maxrss     = ?,"
                " stats_minflt     = ?,"
                " stats_majflt     = ?,"
                " stats_nswap      = ?,"
                " stats_inblock    = ?,"
                " stats_oublock    = ?,"
                " stats_nvcsw      = ?,"
                " stats_nivcsw     = ?,"
                " job_status       = ?,"
                " date_end         = NULL"
            " WHERE job_status IN ("
                    status_str(MXQ_JOB_STATUS_LOADED)  ","
                    status_str(MXQ_JOB_STATUS_RUNNING) ","
                    status_str(MXQ_JOB_STATUS_KILLING) ")"
              " AND job_id    = ?"
              " AND daemon_id = ?"
              " AND host_pid  = ?";

    res = mx_mysql_bind_init_param(&param, 20);
    assert(res == 0);

    idx = 0;
    res = 0;
    res += mx_mysql_bind_var(&param, idx++, uint64, &(job->stats_max_sumrss));
    res += mx_mysql_bind_var(&param, idx++,  int32, &(job->stats_status));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_utime.tv_sec));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_utime.tv_usec));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_stime.tv_sec));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_stime.tv_usec));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_realtime.tv_sec));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_realtime.tv_usec));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_maxrss));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_minflt));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_majflt));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_nswap));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_inblock));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_oublock));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_nvcsw));
    res += mx_mysql_bind_var(&param, idx++,  int64, &(job->stats_rusage.ru_nivcsw));
    res += mx_mysql_bind_var(&param, idx++, uint16, &(newstatus));
    res += mx_mysql_bind_var(&param, idx++, uint64, &(job->job_id));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(job->daemon_id));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(job->host_pid));
    assert(res == 0);

    res = mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    job->job_status = newstatus;

    return res;
}

int mxq_set_job_status_unknown(struct mx_mysql *mysql, struct mxq_job *job)
{
    struct mx_mysql_bind param = {0};
    int idx;
    int res;

    char *query =
            "UPDATE"
                " mxq_job"
            " SET"
                " job_status = " status_str(MXQ_JOB_STATUS_UNKNOWN)
            " WHERE job_id = ?";

    res = mx_mysql_bind_init_param(&param, 1);
    assert(res == 0);

    idx  = 0;
    res  = 0;
    res += mx_mysql_bind_var(&param, idx++, uint64, &job->job_id);
    assert(res == 0);

    res += mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    return res;
}

int mxq_job_set_tmpfilenames(struct mxq_group *g, struct mxq_job *j)
{
    if (!mx_streq(j->job_stdout, "/dev/null")) {
        _mx_cleanup_free_ char *dir = NULL;

        dir = mx_dirname_forever(j->job_stdout);

        mx_asprintf_forever(&j->tmp_stdout, "%s/mxq.%u.%lu.%lu.%s.%s.%d.stdout.tmp",
            dir, g->user_uid, g->group_id, j->job_id, j->host_hostname,
            j->daemon_name, j->host_pid);
    }

    if (!mx_streq(j->job_stderr, "/dev/null")) {
        _mx_cleanup_free_ char *dir = NULL;

        if (mx_streq(j->job_stderr, j->job_stdout)) {
            j->tmp_stderr = j->tmp_stdout;
            return 1;
        }
        dir = mx_dirname_forever(j->job_stderr);

        mx_asprintf_forever(&j->tmp_stderr, "%s/mxq.%u.%lu.%lu.%s.%s.%d.stderr.tmp",
            dir, g->user_uid, g->group_id, j->job_id, j->host_hostname,
            j->daemon_name, j->host_pid);
    }
    return 1;
}

int mxq_load_job_from_group_assigned_to_daemon(struct mx_mysql *mysql, struct mxq_job **jobs_result, uint64_t group_id, struct mxq_daemon *daemon)
{
    struct mxq_job *jobs_tmp = NULL;
    struct mx_mysql_bind param = {0};
    int res;
    int idx;

    assert(mysql);
    assert(jobs_result);
    assert(!(*jobs_result));
    assert(daemon);
    assert(daemon->daemon_id);

    char *query =
            "SELECT"
                JOB_FIELDS
            " FROM"
                " mxq_job"
            " WHERE job_status = " status_str(MXQ_JOB_STATUS_ASSIGNED)
              " AND group_id  = ?"
              " AND daemon_id = ?"
            " LIMIT 1";

    res = mx_mysql_bind_init_param(&param, 2);
    assert(res == 0);

    idx = 0;
    res = 0;
    res += mx_mysql_bind_var(&param, idx++, uint64, &group_id);
    res += mx_mysql_bind_var(&param, idx++, uint32, &daemon->daemon_id);
    assert(res == 0);

    res = do_jobs_statement(mysql, query, &param, &jobs_tmp);
    if (res >= 0)
        *jobs_result = jobs_tmp;

    return res;
}

int mxq_load_job_from_group_for_daemon(struct mx_mysql *mysql, struct mxq_job *job, uint64_t group_id, struct mxq_daemon *daemon)
{
    int res;
    struct mxq_job *jobs_tmp = NULL;

    assert(mysql);
    assert(job);
    assert(daemon);

    do {
        res = mxq_load_job_from_group_assigned_to_daemon(mysql, &jobs_tmp, group_id, daemon);

        if(res < 0) {
            mx_log_err("  group_id=%lu :: mxq_load_job_from_group_assigned_to_daemon: %m", group_id);
            return 0;
        }
        if(res == 1) {
            memcpy(job, &jobs_tmp[0], sizeof(*job));
            mx_free_null(jobs_tmp);
            break;
        }

        res = mxq_assign_job_from_group_to_daemon(mysql, group_id, daemon);
        if (res < 0) {
            mx_log_err("  group_id=%lu :: mxq_assign_job_from_group_to_daemon(): %m", group_id);
            return 0;
        }
        if (res == 0) {
            mx_log_warning("  group_id=%lu :: mxq_assign_job_from_group_to_daemon(): No matching job found - maybe another server was a bit faster. ;)", group_id);
            return 0;
        }
    } while (1);

    res = mxq_set_job_status_loaded(mysql, job);
    if (res < 0) {
        mx_log_err("  group_id=%lu job_id=%lu :: mxq_set_job_status_loaded_on_server(): %m", group_id, job->job_id);
        return 0;
    }
    if (res == 0) {
        mx_log_err("  group_id=%lu job_id=%lu :: mxq_set_job_status_loaded_on_server(): Job not found", group_id, job->job_id);
        return 0;
    }

    job->job_status = MXQ_JOB_STATUS_LOADED;

    return 1;
}

int mxq_load_jobs_running_on_server(struct mx_mysql *mysql, struct mxq_job **jobs_result, struct mxq_daemon *daemon)
{
    struct mxq_job *jobs_tmp = NULL;
    struct mx_mysql_bind param = {0};
    int idx;
    int res;

    assert(daemon);
    assert(daemon->hostname);
    assert(daemon->daemon_name);
    assert(*daemon->hostname);
    assert(*daemon->daemon_name);

    char *query =
            "SELECT"
                JOB_FIELDS
            " FROM"
                " mxq_job"
            " WHERE job_status IN ("
                            status_str(MXQ_JOB_STATUS_LOADED) ","
                            status_str(MXQ_JOB_STATUS_RUNNING) ")"
              " AND host_hostname = ?"
              " AND server_id     = ?";
    res = mx_mysql_bind_init_param(&param, 2);
    assert(res == 0);

    idx = 0;
    res = 0;
    res += mx_mysql_bind_var(&param, idx++, string, &daemon->hostname);
    res += mx_mysql_bind_var(&param, idx++, string, &daemon->daemon_name);
    assert(res == 0);

    res=do_jobs_statement(mysql, query, &param, &jobs_tmp);
    if (res >= 0)
        *jobs_result = jobs_tmp;

    return res;
}
