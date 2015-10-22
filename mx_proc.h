#ifndef __MX_PROC_H__
#define __MX_PROC_H__ 1

#include <sys/types.h>

struct mx_proc_pid_stat {
    long long int pid;     /* 1 */
    char *comm;            /* 2 (comm) */
    char state;            /* 3 "RSDZTW" */
    long long int ppid;    /* 4 */
    long long int pgrp;    /* 5 */
    long long int session; /* 6 */
    long long int tty_nr;  /* 7 */
    long long int tpgid;   /* 8 */
    unsigned long long int flags;   /*  9 */
    unsigned long long int minflt;  /* 10 */
    unsigned long long int cminflt; /* 11 */
    unsigned long long int majflt;  /* 12 */
    unsigned long long int cmajflt; /* 13 */
    unsigned long long int utime;   /* 14 */
    unsigned long long int stime;   /* 15 */
    long long int cutime;   /* 16 */
    long long int cstime;   /* 17 */
    long long int priority; /* 18 */
    long long int nice;     /* 19 */
    long long int num_threads;          /* 20 */
    long long int itrealvalue;          /* 21 */
    unsigned long long int starttime;   /* 22 */
    unsigned long long int vsize;       /* 23 */
    long long int rss;                  /* 24 */
    unsigned long long int rsslim;      /* 25 */
    unsigned long long int startcode;   /* 26 */
    unsigned long long int endcode;     /* 27 */
    unsigned long long int startstack;  /* 28 */
    unsigned long long int kstkesp;     /* 29 */
    unsigned long long int kstkeip;     /* 30 */
    unsigned long long int signal;      /* 31 */
    unsigned long long int blocked;     /* 32 */
    unsigned long long int sigignore;   /* 33 */
    unsigned long long int sigcatch;    /* 34 */
    unsigned long long int wchan;       /* 35 */
    unsigned long long int nswap;       /* 36 */
    unsigned long long int cnswap;      /* 37 */
    long long int exit_signal;          /* 38 */
    long long int processor;            /* 39 */
    unsigned long long int rt_priority; /* 40 */
    unsigned long long int policy;      /* 41 */
    unsigned long long int delayacct_blkio_ticks; /* 42 */
    unsigned long long int guest_time;  /* 43 */
    long long int cguest_time;          /* 44 */
};

int mx_proc_pid_stat_read(struct mx_proc_pid_stat *pps, char *fmt, ...);
int mx_proc_pid_stat(struct mx_proc_pid_stat **pps, pid_t pid);

void mx_proc_pid_stat_free_content(struct mx_proc_pid_stat *pps);

#endif
