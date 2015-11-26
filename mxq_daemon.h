#ifndef __MXQ_DAEMON_H__
#define __MXQ_DAEMON_H__ 1

#include <stdint.h>
#include <mysql.h>

#include "mx_mysql.h"

#define MXQ_DAEMON_STATUS_IDLE              0 /* no jobs are running */
#define MXQ_DAEMON_STATUS_RUNNING          10 /* some jobs are running */
#define MXQ_DAEMON_STATUS_WAITING          20 /* waiting for more slots to return */
#define MXQ_DAEMON_STATUS_FULL             30 /* all slots are running/occupied */
#define MXQ_DAEMON_STATUS_BACKFILL         40 /* more slots are running */
#define MXQ_DAEMON_STATUS_CPUOPTIMAL       50 /* all cpus are running */
#define MXQ_DAEMON_STATUS_TERMINATING     200 /* server is going down */
#define MXQ_DAEMON_STATUS_EXITED          250 /* server exited */
#define MXQ_DAEMON_STATUS_CRASHED         255 /* server exited with unknown status */

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

    uint64_t  daemon_memory_limit_slot_soft;
    uint64_t  daemon_memory_limit_slot_hard;

    uint32_t  daemon_jobs_running;
    uint32_t  daemon_slots_running;
    uint32_t  daemon_threads_running;
    uint64_t  daemon_memory_used;

    struct timeval mtime;

    struct timeval daemon_start;
    struct timeval daemon_stop;
};

void mxq_daemon_free_content(struct mxq_daemon *daemon);
int mxq_daemon_register(struct mx_mysql *mysql, struct mxq_daemon *daemon);
int mxq_daemon_mark_crashed(struct mx_mysql *mysql, struct mxq_daemon *daemon);
int mxq_daemon_shutdown(struct mx_mysql *mysql, struct mxq_daemon *daemon);
int mxq_daemon_set_status(struct mx_mysql *mysql, struct mxq_daemon *daemon, uint8_t status);
#endif
