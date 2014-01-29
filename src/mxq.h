#ifndef __MXQ_H__
#define __MXQ_H__ 1

#include <sys/resource.h>

#ifndef MXQ_MYSQL_DEFAULT_FILE
#define MXQ_MYSQL_DEFAULT_FILE NULL
#endif

struct mxq_task_stats {
    pid_t          pid;
    int            exit_status;
    struct rusage  rusage;
    struct timeval starttime;
    struct timeval elapsed;
};

struct mxq_job {
    int   id;

    char *jobname;

    int   status;    
    
    int   uid;
    char *username;

    int   priority;
};

struct mxq_task {
    int   id;

    struct mxq_job *job;

    int   status;

    int   gid;
    char *groupname;

    int   priority;
    
    char *command;
    int   argc;
    char *argv;
    
    char *workdir;

    char *stdout;
    char *stdouttmp;
    char *stderr;
    char *stderrtmp;
    
    mode_t umask;
    
    char *submit_host;

    struct mxq_task_stats stats;

};

struct mxq_task_list_item {
    struct mxq_task_list_item *next;
    struct mxq_task *task;
};

#endif