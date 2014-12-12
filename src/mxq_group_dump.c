
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

    mmysql.default_file  = MXQ_MYSQL_DEFAULT_FILE;
    mmysql.default_group = "mxq_submit";

    mysql = mxq_mysql_connect(&mmysql);

    group_cnt = mxq_group_load_groups(mysql, &groups);

    for (i=0; i<group_cnt; i++) {
        struct mxq_group *g;

        g = &groups[i];

        printf("user=%s uid=%u group_id=%lu pri=%d jobs_total=%lu run_jobs=%lu run_slots=%lu failed=%lu finished=%lu inq=%lu job_threads=%u job_memory=%lu group_name=%s\n",
                g->user_name, g->user_uid, g->group_id, g->group_priority, g->group_jobs,
                g->group_jobs_running, g->group_slots_running, g->group_jobs_failed, g->group_jobs_finished,
                g->group_jobs-g->group_jobs_running-g->group_jobs_failed-g->group_jobs_finished,
                g->job_threads, g->job_memory, g->group_name);

        mxq_group_free_content(&groups[i]);
    }

    free(groups);

    mxq_mysql_close(mysql);

    return 1;
};

