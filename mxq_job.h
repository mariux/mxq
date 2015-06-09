#ifndef __MXQ_JOB_H__
#define __MXQ_JOB_H__ 1

#include <stdint.h>
#include <mysql.h>

#include <sys/time.h>
#include <sys/resource.h>

#include "mxq_group.h"

struct mxq_job {
    uint64_t  job_id;
    uint16_t  job_status;
    uint64_t  job_flags;
    uint16_t  job_priority;

    uint64_t  group_id;
    struct mxq_group * group_ptr;

    char *     job_workdir;
    unsigned long _job_workdir_length;

    uint16_t   job_argc;

    char **    job_argv;
    char *     job_argv_str;
    unsigned long _job_argv_str_length;

    char *     job_stdout;
    unsigned long _job_stdout_length;
    char *     job_stderr;
    unsigned long _job_stderr_length;

    char *     tmp_stdout;
    char *     tmp_stderr;

    uint32_t   job_umask;

    char *     host_submit;
    unsigned long _host_submit_length;

    char *     server_id;
    unsigned long _server_id_length;

    char *     host_hostname;
    unsigned long _host_hostname_length;

    uint32_t   host_pid;
    uint32_t   host_slots;

    int64_t    date_submit;
    int64_t    date_start;
    int64_t    date_end;

    struct timeval stats_starttime;

    int32_t        stats_status;
    struct timeval stats_realtime;
    struct rusage  stats_rusage;
};

#define MXQ_JOB_STATUS_INQ            0
#define MXQ_JOB_STATUS_ASSIGNED     100
#define MXQ_JOB_STATUS_LOADED       150
#define MXQ_JOB_STATUS_RUNNING      200

#define MXQ_JOB_STATUS_UNKNOWN_RUN  250
#define MXQ_JOB_STATUS_EXTRUNNING   300
#define MXQ_JOB_STATUS_STOPPED      350

#define MXQ_JOB_STATUS_EXIT        1024
#define MXQ_JOB_STATUS_KILLED       400
#define MXQ_JOB_STATUS_FAILED       750
#define MXQ_JOB_STATUS_CANCELLED    990
#define MXQ_JOB_STATUS_UNKNOWN      999
#define MXQ_JOB_STATUS_FINISHED    1000

#define MXQ_JOB_FLAGS_RESTART_ON_HOSTFAIL (1<<0)
#define MXQ_JOB_FLAGS_REQUEUE_ON_HOSTFAIL (1<<1)

#define MXQ_JOB_FLAGS_AUTORESTART         (1<<62)
#define MXQ_JOB_FLAGS_HOSTFAIL            (1<<63)

#define _to_string(s) #s
#define status_str(x) _to_string(x)

char *mxq_job_status_to_name(uint64_t status);

int mxq_load_job(struct mx_mysql *mysql, struct mxq_job **mxq_jobs, uint64_t job_id);
int mxq_load_jobs_in_group(struct mx_mysql *mysql, struct mxq_job **mxq_jobs, struct mxq_group *grp);
int mxq_load_jobs_in_group_with_status(struct mx_mysql *mysql, struct mxq_job **mxq_jobs, struct mxq_group *grp, uint64_t job_status);

int mxq_job_load_assigned(MYSQL *mysql, struct mxq_job *job, char *hostname, char *server_id);
void mxq_job_free_content(struct mxq_job *j);
int mxq_job_load(MYSQL *mysql, struct mxq_job *mxqjob, uint64_t group_id, char *hostname, char *server_id);
int mxq_job_update_status_assigned(MYSQL *mysql, struct mxq_job *job);
int mxq_job_update_status_loaded(MYSQL *mysql, struct mxq_job *job);
int mxq_job_update_status_running(MYSQL *mysql, struct mxq_job *job);
int mxq_job_update_status_exit(MYSQL *mysql, struct mxq_job *job);
int mxq_job_set_tmpfilenames(struct mxq_group *g, struct mxq_job *j);

int mxq_job_update_status_cancelled_by_group(MYSQL *mysql, struct mxq_group *group);

#endif
