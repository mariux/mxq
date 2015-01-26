
#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>

#include <mysql.h>

#include "mxq_util.h"
#include "mxq_mysql.h"

int main(int argc, char *argv[])
{
    struct mxq_mysql mmysql;
    MYSQL *mysql;
    struct mxq_group *groups;
    int group_cnt;
    int i;
    char *arg_mysql_default_group;
    char *arg_mysql_default_file;

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = "mxq_submit";

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    mmysql.default_file  = arg_mysql_default_file;
    mmysql.default_group = arg_mysql_default_group;

    mysql = mxq_mysql_connect(&mmysql);

    group_cnt = mxq_group_load_groups(mysql, &groups);

    for (i=0; i<group_cnt; i++) {
        struct mxq_group *g;

        g = &groups[i];

        printf("user=%s uid=%u group_id=%lu pri=%d jobs_total=%lu run_jobs=%lu run_slots=%lu failed=%lu finished=%lu inq=%lu job_threads=%u job_memory=%lu job_time=%u stats_max_utime=%lu stats_max_real=%lu job_command=%s group_name=%s\n",
                g->user_name, g->user_uid, g->group_id, g->group_priority, g->group_jobs,
                g->group_jobs_running, g->group_slots_running, g->group_jobs_failed, g->group_jobs_finished,
                g->group_jobs-g->group_jobs_running-g->group_jobs_failed-g->group_jobs_finished,
                g->job_threads, g->job_memory, g->job_time, g->stats_max_utime.tv_sec, g->stats_max_real.tv_sec,
                g->job_command, g->group_name);

        mxq_group_free_content(&groups[i]);
    }

    free(groups);

    mxq_mysql_close(mysql);

    return 1;
};

