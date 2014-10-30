
#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>

#include <mysql.h>

#include "mxq_util.h"
#include "mxq_mysql.h"


struct result {
    struct mxq_job job;

    unsigned long job_workdir_length;
    unsigned long job_argv_length;
    unsigned long job_stdout_length;
    unsigned long job_stderr_length;
    unsigned long host_submit_length;
    unsigned long server_id_length;
    unsigned long host_hostname_length;
};


//XXX
#define COLUMNS "job_id, " \
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

//XXX
enum result_columns {
    COL_JOB_ID,
    COL_JOB_STATUS,
    COL_JOB_PRIORITY,
    COL_GROUP_ID,
    COL_JOB_WORKDIR,
    COL_JOB_ARGC,
    COL_JOB_ARGV,
    COL_JOB_STDOUT,
    COL_JOB_STDERR,
    COL_JOB_UMASK,
    COL_HOST_SUBMIT,
    COL_SERVER_ID,
    COL_HOST_HOSTNAME,
    COL_HOST_PID,
    COL_DATE_SUBMIT,
    COL_DATE_START,
    COL_DATE_END,
    COL_STATS_STATUS,
    COL_STATS_UTIME_SEC,
    COL_STATS_UTIME_USEC,
    COL_STATS_STIME_SEC,
    COL_STATS_STIME_USEC,
    COL_STATS_REAL_SEC,
    COL_STATS_REAL_USEC,
    COL_STATS_MAXRSS,
    COL_STATS_MINFLT,
    COL_STATS_MAJFLT,
    COL_STATS_NSWAP,
    COL_STATS_INBLOCK,
    COL_STATS_OUBLOCK,
    COL_STATS_NVCSW,
    COL_STATS_NIVCSW,
    COL__END
};

/*
    struct timeval stats_starttime;

    int            stats_status;
    struct timeval stats_realtime;
    struct rusage  stats_rusage;
};
*/

//XXX
static inline int prepare_result_bindings(MYSQL_BIND *bind, struct result *g)
{
    memset(bind, 0, sizeof(*bind)*COL__END);

    MXQ_MYSQL_BIND_UINT64(bind, COL_JOB_ID,       &g->job.job_id);
    MXQ_MYSQL_BIND_UINT8(bind,  COL_JOB_STATUS,   &g->job.job_status);
    MXQ_MYSQL_BIND_UINT16(bind, COL_JOB_PRIORITY, &g->job.job_priority);

    MXQ_MYSQL_BIND_UINT64(bind, COL_GROUP_ID, &g->job.group_id);

    MXQ_MYSQL_BIND_VARSTR(bind, COL_JOB_WORKDIR, &g->job_workdir_length);

    MXQ_MYSQL_BIND_UINT16(bind, COL_JOB_ARGC, &g->job.job_argc);
    MXQ_MYSQL_BIND_VARSTR(bind, COL_JOB_ARGV, &g->job_argv_length);

    MXQ_MYSQL_BIND_VARSTR(bind, COL_JOB_STDOUT, &g->job_stdout_length);
    MXQ_MYSQL_BIND_VARSTR(bind, COL_JOB_STDERR, &g->job_stderr_length);

    MXQ_MYSQL_BIND_UINT32(bind, COL_JOB_UMASK, &g->job.job_umask);

    MXQ_MYSQL_BIND_VARSTR(bind, COL_HOST_SUBMIT, &g->host_submit_length);
    MXQ_MYSQL_BIND_VARSTR(bind, COL_SERVER_ID,   &g->server_id_length);

    MXQ_MYSQL_BIND_VARSTR(bind, COL_HOST_HOSTNAME, &g->host_hostname_length);
    MXQ_MYSQL_BIND_UINT32(bind, COL_HOST_PID,      &g->job.host_pid);

    MXQ_MYSQL_BIND_INT32(bind, COL_DATE_SUBMIT, &g->job.date_submit);
    MXQ_MYSQL_BIND_INT32(bind, COL_DATE_START,  &g->job.date_start);
    MXQ_MYSQL_BIND_INT32(bind, COL_DATE_END,    &g->job.date_end);

    MXQ_MYSQL_BIND_UINT32(bind, COL_STATS_UTIME_SEC,  &g->job.stats_rusage.ru_utime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(bind, COL_STATS_UTIME_USEC, &g->job.stats_rusage.ru_utime.tv_usec);

    MXQ_MYSQL_BIND_UINT32(bind, COL_STATS_STIME_SEC,  &g->job.stats_rusage.ru_stime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(bind, COL_STATS_STIME_USEC, &g->job.stats_rusage.ru_stime.tv_usec);

    MXQ_MYSQL_BIND_UINT32(bind, COL_STATS_REAL_SEC,  &g->job.stats_realtime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(bind, COL_STATS_REAL_USEC, &g->job.stats_realtime.tv_usec);

    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_STATUS, &g->job.stats_status);
    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_MAXRSS, &g->job.stats_rusage.ru_maxrss);
    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_MINFLT, &g->job.stats_rusage.ru_minflt);
    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_MAJFLT, &g->job.stats_rusage.ru_majflt);
    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_NSWAP, &g->job.stats_rusage.ru_nswap);
    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_INBLOCK, &g->job.stats_rusage.ru_inblock);
    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_OUBLOCK, &g->job.stats_rusage.ru_oublock);
    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_NVCSW, &g->job.stats_rusage.ru_nvcsw);
    MXQ_MYSQL_BIND_INT32(bind, COL_STATS_NIVCSW, &g->job.stats_rusage.ru_nivcsw);


    return 1;
}

int fetch_results(MYSQL_STMT *stmt, MYSQL_BIND *bind, struct result *g)
{
    int res;

    res = mxq_mysql_stmt_fetch_row(stmt);
    if (!res) {
        if (errno == ENOENT)
            return 0;
        perror("xxxx0");
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, COL_JOB_WORKDIR, &(g->job.job_workdir), g->job_workdir_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, COL_JOB_ARGV, &(g->job.job_argv_str), g->job_argv_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, COL_JOB_STDOUT, &(g->job.job_stdout), g->job_stdout_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, COL_JOB_STDERR, &(g->job.job_stderr), g->job_stderr_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, COL_HOST_SUBMIT, &(g->job.host_submit), g->host_submit_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, COL_SERVER_ID, &(g->job.server_id), g->server_id_length);
    if (!res) {
        return 0;
    }

    res = mxq_mysql_stmt_fetch_string(stmt, bind, COL_HOST_HOSTNAME, &(g->job.host_hostname), g->host_hostname_length);
    if (!res) {
        return 0;
    }

    return 1;
}

int mxq_group_load_jobs(MYSQL *mysql, struct mxq_job **mxq_job)
{
    MYSQL_STMT *stmt;
    MYSQL_BIND result[COL__END];
    struct result g;
    char *query;
    struct mxq_job *job;
    int cnt;

    *mxq_job = NULL;

    prepare_result_bindings(result, &g);

    query = "SELECT " COLUMNS " FROM mxq_job";

    stmt = mxq_mysql_stmt_do_query(mysql, query, COL__END, NULL, result);
    if (!stmt) {
        print_error("mxq_mysql_stmt_do_query(mysql=%p, stmt_str=\"%s\", field_count=%d, param=%p, result=%p)\n", mysql, query, COL__END, NULL, result);
        return 0;
    }

    //XXX
    cnt = 0;
    job = NULL;
    while (fetch_results(stmt, result, &g)) {
        job = realloc_forever(job, sizeof(*job)*(cnt+1));
        memcpy(job+cnt, &g.job, sizeof(*job));
        cnt++;
    }

    *mxq_job = job;

    mysql_stmt_close(stmt);
    return cnt;
}

int main(int argc, char *argv[])
{
    struct mxq_mysql mmysql;
    MYSQL *mysql;
    struct mxq_job *jobs;
    int cnt;
    int i;

    mmysql.default_file  = NULL;
    mmysql.default_group = "mxq_submit";

    mysql = mxq_mysql_connect(&mmysql);

    cnt = mxq_group_load_jobs(mysql, &jobs);

//    printf("cnt=%d\n", cnt);

    for (i=0;i<cnt; i++) {
        printf("%ld\t%s\n", jobs[i].job_id, jobs[i].job_argv_str);
    }

    free(jobs);

    mxq_mysql_close(mysql);

    return 1;
};

