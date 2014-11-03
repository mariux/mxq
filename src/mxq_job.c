
#include <stdio.h>
#include <mysql.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>

#include <sys/resource.h>

#include "mxq_group.h"
#include "mxq_job.h"
#include "mxq_mysql.h"

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
    MXQ_MYSQL_BIND_UINT8(bind,  MXQ_JOB_COL_JOB_STATUS,   &j->job_status);
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
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_JOB_WORKDIR, &(j->job_workdir), j->_job_workdir_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_JOB_ARGV, &(j->job_argv_str), j->_job_argv_str_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_JOB_STDOUT, &(j->job_stdout), j->_job_stdout_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_JOB_STDERR, &(j->job_stderr), j->_job_stderr_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_HOST_SUBMIT, &(j->host_submit), j->_host_submit_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_SERVER_ID, &(j->server_id), j->_server_id_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_JOB_COL_HOST_HOSTNAME, &(j->host_hostname), j->_host_hostname_length);
    if (!res) {
        return 0;
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

        free_null(j->host_submit);
        j->_host_submit_length = 0;

        free_null(j->server_id);
        j->_server_id_length = 0;

        free_null(j->host_hostname);
        j->_host_hostname_length = 0;

        strvec_free(j->job_argv);
        j->job_argv = NULL;
}

int mxq_job_reserve(MYSQL *mysql, uint64_t group_id, char *hostname, char *server_id)
{
    assert(mysql);
    assert(hostname);
    assert(server_id);

    int   len;
    int   res;
    int   i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_fields;
    unsigned int num_rows;

    _cleanup_free_ char *q_hostname  = NULL;
    _cleanup_free_ char *q_server_id = NULL;

    if (!(q_hostname  = mxq_mysql_escape_string(mysql, hostname)  )) return -1;
    if (!(q_server_id = mxq_mysql_escape_string(mysql, server_id) )) return -1;

    // update v_tasks set task_status=1,host_hostname='localhost',host_server_id='localhost-1'
    // where task_status = 0 AND host_hostname='localhost' AND host_pid IS NULL order by job_id limit 1;

    res = mxq_mysql_query(mysql, "UPDATE mxq_job SET"
                " job_status = 10,"
                " host_hostname = '%s',"
                " server_id = '%s'"
                " WHERE group_id = %ld"
                " AND job_status = 0"
                " AND host_hostname=''"
                " AND server_id = ''"
                " AND host_pid = 0"
                " ORDER BY job_priority, job_id"
                " LIMIT 1",
                q_hostname, q_server_id, group_id);
    if (res) {
        log_msg(0, "mxq_job_reserve: Failed to query database: Error: %s\n", mysql_error(mysql));
        sleep(10);
        errno = EIO;
        return -1;
    }

    return mysql_affected_rows(mysql);
}

int mxq_job_markloaded(MYSQL *mysql, uint64_t job_id, char *hostname, char *server_id)
{
    assert(mysql);
    assert(hostname);
    assert(server_id);

    int   len;
    int   res;
    int   i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_fields;
    unsigned int num_rows;

    _cleanup_free_ char *q_hostname  = NULL;
    _cleanup_free_ char *q_server_id = NULL;

    if (!(q_hostname  = mxq_mysql_escape_string(mysql, hostname)  )) return -1;
    if (!(q_server_id = mxq_mysql_escape_string(mysql, server_id) )) return -1;

    // update v_tasks set task_status=1,host_hostname='localhost',host_server_id='localhost-1'
    // where task_status = 0 AND host_hostname='localhost' AND host_pid IS NULL order by job_id limit 1;

    res = mxq_mysql_query(mysql, "UPDATE mxq_job SET"
                " job_status = 15"
                " WHERE job_id = %ld"
                " AND job_status = 10"
                " AND host_hostname = '%s'"
                " AND server_id = '%s'"
                " AND host_pid = 0",
                job_id, q_hostname, q_server_id);
    if (res) {
        log_msg(0, "mxq_job_markloaded: Failed to query database: Error: %s\n", mysql_error(mysql));
        errno = EIO;
        return -1;
    }

    res = mysql_affected_rows(mysql);
    if (res == 0) {
        errno = ENOENT;
        return -1;
    }

    return res;
}

int mxq_job_markrunning(MYSQL *mysql, uint64_t job_id, char *hostname, char *server_id, pid_t pid, uint32_t slots)
{
    assert(mysql);
    assert(hostname);
    assert(server_id);

    int   len;
    int   res;
    int   i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_fields;
    unsigned int num_rows;

    _cleanup_free_ char *q_hostname  = NULL;
    _cleanup_free_ char *q_server_id = NULL;

    if (!(q_hostname  = mxq_mysql_escape_string(mysql, hostname)  )) return -1;
    if (!(q_server_id = mxq_mysql_escape_string(mysql, server_id) )) return -1;

    res = mxq_mysql_query(mysql, "UPDATE mxq_job SET"
                " job_status = 20,"
                " host_pid = %d,"
                " host_slots = %d"
                " WHERE job_id = %ld"
                " AND job_status IN (10, 15)"
                " AND host_hostname = '%s'"
                " AND server_id = '%s'"
                " AND host_pid = 0",
                pid, slots, job_id, q_hostname, q_server_id);
    if (res) {
        log_msg(0, "mxq_job_markrunning: Failed to query database: Error: %s\n", mysql_error(mysql));
        errno = EIO;
        return -1;
    }

    res = mysql_affected_rows(mysql);
    if (res == 0) {
        errno = ENOENT;
        return -1;
    }

    return res;
}


int mxq_job_load_reserved(MYSQL *mysql, struct mxq_job *job)
{
    MYSQL_STMT *stmt;
    MYSQL_BIND result[MXQ_JOB_COL__END];
    char *query;
    int cnt;
    MYSQL_BIND param[3];

    assert(job->host_hostname);
    assert(job->server_id);

    memset(param, 0, sizeof(param));
    MXQ_MYSQL_BIND_UINT8(param,  0, &job->job_status);
    MXQ_MYSQL_BIND_STRING(param, 1, job->host_hostname);
    MXQ_MYSQL_BIND_STRING(param, 2, job->server_id);

    mxq_job_bind_results(result, job);

    query = "SELECT " MXQ_JOB_FIELDS " FROM mxq_job"
            " WHERE job_status = ?"
            " AND host_hostname = ?"
            " AND server_id  = ?";

    stmt = mxq_mysql_stmt_do_query(mysql, query, MXQ_JOB_COL__END, param, result);
    if (!stmt) {
        print_error("mxq_job_load_reserved(mysql=%p, stmt_str=\"%s\", field_count=%d, param=%p, result=%p)\n", mysql, query, MXQ_JOB_COL__END, param, result);
        return -1;
    }

    if (!mxq_job_fetch_results(stmt, result, job)) {
        printf("mxq_job_fetch_results FAILED..\n");
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_close(stmt);
    return 1;
}


// TODO !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
int mxq_job_load(MYSQL *mysql, struct mxq_job *mxqjob, uint64_t group_id, char *hostname, char *server_id)
{
    int res;

    res = mxq_job_reserve(mysql, group_id, hostname, server_id);

    if (res < 0) {
        perror("mxq_job_reserve");
        return 0;
    }

    if (res == 0) {
//        printf("mxq_job_reserve: NO job reserved\n");
        return 0;
    }

//    printf("mxq_job_reserve: job reserved\n");

    mxqjob->host_hostname = hostname;
    mxqjob->server_id = server_id;
    mxqjob->job_status = MXQ_JOB_STATUS_ASSIGNED;

    res = mxq_job_load_reserved(mysql, mxqjob);

    if(res <= 0) {
        printf("*** COULD NOT LOAD RESERVED JOB\n");
        return 0;
    }

//    printf("loaded job job_id=%ld %s\n", mxqjob->job_id, mxqjob->job_argv_str);

    res = mxq_job_markloaded(mysql, mxqjob->job_id, hostname, server_id);
    if (res < 0) {
        perror("mxq_job_markloaded");
        return 0;
    }

    mxqjob->job_status = MXQ_JOB_STATUS_LOADED;
    return 1;
}
