#ifndef __MXQ_DAEMON_H__
#define __MXQ_DAEMON_H__ 1

#include <stdint.h>
#include <mysql.h>

#include "mx_mysql.h"

struct mxq_daemon {
    uint32_t  daemon_id;

    char *    daemon_name;

    uint8_t   status;

    char *    hostname;
    char *    mxq_version;
    char *    boot_id;

    uint64_t  pid_starttime;
    uint32_t  daemon_pid;

    uint32_t  daemon_slots;
    uint64_t  daemon_memory;
    uint32_t  daemon_time;

    uint32_t  daemon_jobs_running;
    uint32_t  daemon_slots_running;
    uint32_t  daemon_threads_running;
    uint64_t  daemon_memory_used;

    struct timeval mtime;

    struct timeval daemon_start;
    struct timeval daemon_stop;
};

void mxq_daemon_free_content(struct mxq_daemon *daemon);

#endif
