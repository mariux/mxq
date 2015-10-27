#ifndef __MXQ_JOB_H__
#define __MXQ_JOB_H__ 1

#include <stdint.h>
#include <mysql.h>

#include <sys/time.h>
#include <sys/resource.h>

#include <sched.h>

#include "mxq_group.h"

struct mxq_job {
    uint64_t  job_id;
    uint16_t  job_status;
    uint64_t  job_flags;
    uint16_t  job_priority;

    uint64_t  group_id;
    struct mxq_group * group_ptr;

    char *     job_workdir;

    uint16_t   job_argc;

    char **    job_argv;
    char *     job_argv_str;

    char *     job_stdout;
    char *     job_stderr;

    char *     tmp_stdout;
    char *     tmp_stderr;

    uint32_t   job_umask;

    char *     host_submit;

    char *     host_id;
    char *     server_id;

    char *     host_hostname;

    uint32_t   host_pid;
    uint32_t   host_slots;
    cpu_set_t   host_cpu_set;

    int64_t    date_submit;
    int64_t    date_start;
    int64_t    date_end;

    struct timeval stats_starttime;

    uint64_t   stats_max_sumrss;

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

#define MXQ_JOB_STATUS_KILLING      399

#define MXQ_JOB_STATUS_KILLED       400
#define MXQ_JOB_STATUS_FAILED       750
#define MXQ_JOB_STATUS_UNKNOWN_PRE  755

#define MXQ_JOB_STATUS_CANCELLING   989

#define MXQ_JOB_STATUS_CANCELLED    990
#define MXQ_JOB_STATUS_UNKNOWN      999
#define MXQ_JOB_STATUS_FINISHED    1000

#define MXQ_JOB_STATUS_EXIT        1024

#define MXQ_JOB_FLAGS_RESTART_ON_HOSTFAIL (1<<0)
#define MXQ_JOB_FLAGS_REQUEUE_ON_HOSTFAIL (1<<1)

#define MXQ_JOB_FLAGS_AUTORESTART         (1<<62)
#define MXQ_JOB_FLAGS_HOSTFAIL            (1<<63)

#define _to_string(s) #s
#define status_str(x) _to_string(x)

char *mxq_job_status_to_name(uint64_t status);

void mxq_job_free_content(struct mxq_job *j);

int mxq_load_job(struct mx_mysql *mysql, struct mxq_job **mxq_jobs, uint64_t job_id);
int mxq_load_jobs_in_group(struct mx_mysql *mysql, struct mxq_job **mxq_jobs, struct mxq_group *grp);
int mxq_load_jobs_in_group_with_status(struct mx_mysql *mysql, struct mxq_job **mxq_jobs, struct mxq_group *grp, uint64_t job_status);
int mxq_assign_job_from_group_to_server(struct mx_mysql *mysql, uint64_t group_id, char *hostname, char *server_id);
int mxq_unassign_jobs_of_server(struct mx_mysql *mysql, char *hostname, char *server_id);
int mxq_set_job_status_loaded_on_server(struct mx_mysql *mysql, struct mxq_job *job);
int mxq_set_job_status_running(struct mx_mysql *mysql, struct mxq_job *job);
int mxq_set_job_status_exited(struct mx_mysql *mysql, struct mxq_job *job);
int mxq_set_job_status_unknown_for_server(struct mx_mysql *mysql, char *hostname, char *server_id);
int mxq_job_set_tmpfilenames(struct mxq_group *g, struct mxq_job *j);
int mxq_load_job_from_group_assigned_to_server(struct mx_mysql *mysql, struct mxq_job **mxq_jobs, uint64_t group_id, char *hostname, char *server_id);
int mxq_load_job_from_group_for_server(struct mx_mysql *mysql, struct mxq_job *mxqjob, uint64_t group_id, char *hostname, char *server_id, char *host_id);

#endif
