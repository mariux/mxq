#ifndef __MXQ_JOB_H__
#define __MXQ_JOB_H__ 1

#include <stdint.h>

struct mxq_job {
    uint64_t  job_id;
    uint8_t   job_status;
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

    int64_t    date_submit;
    int64_t    date_start;
    int64_t    date_end;

    struct timeval stats_starttime;

    int32_t        stats_status;
    struct timeval stats_realtime;
    struct rusage  stats_rusage;
};

#endif
