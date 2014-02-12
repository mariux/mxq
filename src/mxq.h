#ifndef __MXQ_H__
#define __MXQ_H__ 1

#include <sys/types.h>
#include <sys/resource.h>

struct mxq_job_full {
    u_int64_t  job_id;
    u_int8_t   job_status;
    u_int16_t  job_priority;

    char       group_id[512];
    u_int8_t   group_status;
    u_int16_t  group_priority;

    uid_t      user_uid;
    char       user_name[256];
    gid_t      user_gid;
    char       user_group[256];

    u_int16_t  job_threads;
    u_int64_t  job_memory;
    u_int32_t  job_time;

    char       job_workdir[4096];
    char       job_command[4096];
    u_int16_t  job_argc;
    char       job_argv[40960];

    char       job_stdout[4096];
    char       job_stderr[4096];

    char       tmp_stdout[4096];
    char       tmp_stderr[4096];

    mode_t     job_umask;

    char       host_submit[1024];

    char       server_id[1024];

    char       host_hostname[1014];
    pid_t      host_pid;

    char       date_submit[256];
    char       date_start[256];
    char       date_end[256];

    struct timeval stats_starttime;

    int            stats_status;
    struct timeval stats_realtime;
    struct rusage  stats_rusage;
};

#ifndef MXQ_MYSQL_DEFAULT_FILE
#define MXQ_MYSQL_DEFAULT_FILE NULL
#endif

struct mxq_job_full_list {
    struct mxq_job_full_list_item *first;
    struct mxq_job_full_list_item *last;

    int count;
};

struct mxq_job_full_list_item {
    struct mxq_job_full_list_item *next;
    struct mxq_job_full_list_item *prev;

    struct mxq_job_full *job;
};

#endif
