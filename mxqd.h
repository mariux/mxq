#ifndef __MXQ_SERVER_H__
#define __MXQ_SERVER_H__ 1

#include "mx_mysql.h"

#include "mxq_job.h"
#include "mxq_group.h"
#include "mxq_daemon.h"

#include <sched.h>

struct mxq_job_list {
    struct mxq_group_list *group;
    struct mxq_job_list   *next;

    struct mxq_job job;

    unsigned long long int max_sumrss;
};

struct mxq_group_list {
    struct mxq_user_list  *user;
    struct mxq_group_list *next;

    struct mxq_group group;

    struct mxq_job_list *jobs;

    unsigned long job_cnt;

    long double   memory_per_job_thread;
    unsigned long slots_per_job;
    long double   memory_available_for_group;
    unsigned long memory_max;
    unsigned long slots_max;
    unsigned long jobs_max;

    unsigned long jobs_running;
    unsigned long threads_running;
    unsigned long slots_running;
    unsigned long memory_used;

    unsigned long global_threads_running;
    unsigned long global_slots_running;

    short orphaned;
};

struct mxq_user_list {
    struct mxq_server    *server;
    struct mxq_user_list *next;

    struct mxq_group_list *groups;
    unsigned long group_cnt;
    unsigned long job_cnt;

    unsigned long jobs_running;
    unsigned long threads_running;
    unsigned long slots_running;
    unsigned long memory_used;

    unsigned long global_threads_running;
    unsigned long global_slots_running;
};

struct mxq_server {
    struct mxq_user_list *users;

    struct mxq_daemon daemon;

    unsigned long user_cnt;
    unsigned long group_cnt;
    unsigned long job_cnt;

    unsigned long jobs_running;
    unsigned long threads_running;
    unsigned long slots_running;
    unsigned long memory_used;
    cpu_set_t      cpu_set_running;

    unsigned long global_threads_running;
    unsigned long global_slots_running;

    unsigned long slots;
    unsigned long memory_total;
    long double   memory_avg_per_slot;
    unsigned long memory_limit_slot_soft;
    unsigned long memory_limit_slot_hard;
    unsigned long maxtime;
    cpu_set_t      cpu_set_available;

    struct mx_mysql *mysql;

    char *boot_id;
    unsigned long long int starttime;
    char *host_id;
    char *hostname;
    char *daemon_name;
    char *pidfilename;
    char *finished_jobsdir;
    struct mx_flock *flock;

    char *initial_path;
    char *initial_tmpdir;
    char recoveronly;

    size_t supgid_cnt;
    gid_t *supgid;
};


#endif
