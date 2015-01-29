
#include <stdio.h>

#include <assert.h>

#include <mysql.h>


#include "mxq_group.h"
#include "mxq_mysql.h"

#define MXQ_GROUP_FIELDS "group_id," \
                "group_name," \
                "group_status," \
                "group_priority," \
                "user_uid," \
                "user_name," \
                "user_gid," \
                "user_group," \
                "job_command," \
                "job_threads," \
                "job_memory," \
                "job_time," \
                "group_jobs," \
                "group_jobs_running," \
                "group_jobs_finished," \
                "group_jobs_failed," \
                "group_jobs_cancelled," \
                "group_slots_running," \
                "stats_max_maxrss," \
                "stats_max_utime_sec," \
                "stats_max_stime_sec," \
                "stats_max_real_sec"

enum mxq_group_columns {
    MXQ_GROUP_COL_GROUP_ID=0,
    MXQ_GROUP_COL_GROUP_NAME,
    MXQ_GROUP_COL_GROUP_STATUS,
    MXQ_GROUP_COL_GROUP_PRIORITY,
    MXQ_GROUP_COL_USER_UID,
    MXQ_GROUP_COL_USER_NAME,
    MXQ_GROUP_COL_USER_GID,
    MXQ_GROUP_COL_USER_GROUP,
    MXQ_GROUP_COL_JOB_COMMAND,
    MXQ_GROUP_COL_JOB_THREADS,
    MXQ_GROUP_COL_JOB_MEMORY,
    MXQ_GROUP_COL_JOB_TIME,
    MXQ_GROUP_COL_GROUP_JOBS,
    MXQ_GROUP_COL_GROUP_JOBS_RUNNING,
    MXQ_GROUP_COL_GROUP_JOBS_FINISHED,
    MXQ_GROUP_COL_GROUP_JOBS_FAILED,
    MXQ_GROUP_COL_GROUP_JOBS_CANCELLED,
    MXQ_GROUP_COL_GROUP_SLOTS_RUNNING,
    MXQ_GROUP_COL_STATS_MAX_MAXRSS,
    MXQ_GROUP_COL_STATS_MAX_UTIME_SEC,
    MXQ_GROUP_COL_STATS_MAX_STIME_SEC,
    MXQ_GROUP_COL_STATS_MAX_REAL_SEC,
    MXQ_GROUP_COL__END
};

static inline int mxq_group_bind_results(MYSQL_BIND *bind, struct mxq_group *g)
{
    memset(bind, 0, sizeof(*bind)*MXQ_GROUP_COL__END);

    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_GROUP_ID,       &g->group_id);
    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_GROUP_COL_GROUP_NAME,     &g->_group_name_length);
    MXQ_MYSQL_BIND_UINT8(bind,  MXQ_GROUP_COL_GROUP_STATUS,   &g->group_status);
    MXQ_MYSQL_BIND_UINT16(bind, MXQ_GROUP_COL_GROUP_PRIORITY, &g->group_priority);

    MXQ_MYSQL_BIND_UINT32(bind, MXQ_GROUP_COL_USER_UID,    &g->user_uid);
    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_GROUP_COL_USER_NAME,   &g->_user_name_length);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_GROUP_COL_USER_GID,    &g->user_gid);
    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_GROUP_COL_USER_GROUP,  &g->_user_group_length);

    MXQ_MYSQL_BIND_VARSTR(bind, MXQ_GROUP_COL_JOB_COMMAND, &g->_job_command_length);

    MXQ_MYSQL_BIND_UINT16(bind, MXQ_GROUP_COL_JOB_THREADS, &g->job_threads);
    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_JOB_MEMORY,  &g->job_memory);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_GROUP_COL_JOB_TIME,    &g->job_time);

    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_GROUP_JOBS,           &g->group_jobs);
    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_GROUP_JOBS_RUNNING,   &g->group_jobs_running);
    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_GROUP_JOBS_FINISHED,  &g->group_jobs_finished);
    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_GROUP_JOBS_FAILED,    &g->group_jobs_failed);
    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_GROUP_JOBS_CANCELLED, &g->group_jobs_cancelled);

    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_GROUP_SLOTS_RUNNING, &g->group_slots_running);

    MXQ_MYSQL_BIND_INT32(bind,  MXQ_GROUP_COL_STATS_MAX_MAXRSS,    &g->stats_max_maxrss);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_GROUP_COL_STATS_MAX_UTIME_SEC, &g->stats_max_utime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_GROUP_COL_STATS_MAX_STIME_SEC, &g->stats_max_stime.tv_sec);
    MXQ_MYSQL_BIND_UINT32(bind, MXQ_GROUP_COL_STATS_MAX_REAL_SEC,  &g->stats_max_real.tv_sec);

    return 1;
}

static int mxq_group_fetch_results(MYSQL_STMT *stmt, MYSQL_BIND *bind, struct mxq_group *g)
{
    int res;

    memset(g, 0, sizeof(*g));

    res = mxq_mysql_stmt_fetch_row(stmt);
    if (!res)
        return 0;

    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_GROUP_COL_GROUP_NAME, &(g->group_name), g->_group_name_length);
    if (!res)
        return 0;
    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_GROUP_COL_USER_NAME, &(g->user_name), g->_user_name_length);
    if (!res)
        return 0;
    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_GROUP_COL_USER_GROUP, &(g->user_group), g->_user_group_length);
    if (!res)
        return 0;
    res = mxq_mysql_stmt_fetch_string(stmt, bind, MXQ_GROUP_COL_JOB_COMMAND, &(g->job_command), g->_job_command_length);
    if (!res)
        return 0;

    return 1;
}

void mxq_group_free_content(struct mxq_group *g)
{
        free_null(g->group_name);
        g->_group_name_length = 0;

        free_null(g->user_name);
        g->_user_name_length = 0;

        free_null(g->user_group);
        g->_user_group_length = 0;

        free_null(g->job_command);
        g->_job_command_length = 0;
}


inline uint64_t mxq_group_jobs_done(struct mxq_group *g)
{
    uint64_t done = 0;

    done += g->group_jobs_finished;
    done += g->group_jobs_failed;
    done += g->group_jobs_cancelled;

    return done;
}

inline uint64_t mxq_group_jobs_active(struct mxq_group *g)
{
    uint64_t inq;

    inq  = g->group_jobs;
    inq -= mxq_group_jobs_done(g);

    return inq;
}

inline uint64_t mxq_group_jobs_inq(struct mxq_group *g)
{
    uint64_t inq;

    inq  = mxq_group_jobs_active(g);
    inq -= g->group_jobs_running;

    return inq;
}



int mxq_group_load_active_groups(MYSQL *mysql, struct mxq_group **mxq_group)
{
    MYSQL_STMT *stmt;
    MYSQL_BIND result[MXQ_GROUP_COL__END];
    struct mxq_group g;
    struct mxq_group *groups;
    char *query;
    int cnt;

    *mxq_group = NULL;

    mxq_group_bind_results(result, &g);

    query = "SELECT " MXQ_GROUP_FIELDS
            " FROM mxq_group"
            " WHERE group_jobs-group_jobs_finished-group_jobs_failed-group_jobs_cancelled > 0"
            " ORDER BY user_uid, group_priority DESC";

    stmt = mxq_mysql_stmt_do_query(mysql, query, MXQ_GROUP_COL__END, NULL, result);
    if (!stmt) {
        MXQ_LOG_ERROR("mxq_mysql_stmt_do_query(mysql=%p, stmt_str=\"%s\", field_count=%d, param=%p, result=%p)\n", mysql, query, MXQ_GROUP_COL__END, NULL, result);
        return 0;
    }

    cnt = 0;
    groups = NULL;
    while (mxq_group_fetch_results(stmt, result, &g)) {
        groups = realloc_forever(groups, sizeof(*groups)*(cnt+1));
        memcpy(groups+cnt, &g, sizeof(*groups));
        cnt++;
    }

    *mxq_group = groups;

    mysql_stmt_close(stmt);
    return cnt;
}

int mxq_group_update_status_cancelled(MYSQL *mysql, struct mxq_group *group)
{
    char *query;
    MYSQL_BIND param[3];
    int   res;

    assert(group->group_id);
    assert(group->user_uid);
    assert(group->user_name);
    assert(*group->user_name);

    memset(param, 0, sizeof(param));
    query = "UPDATE mxq_group SET"
            " group_status = " status_str(MXQ_GROUP_STATUS_CANCELLED)
            " WHERE group_id = ?"
            " AND group_status = " status_str(MXQ_GROUP_STATUS_OK)
            " AND user_uid = ?"
            " AND user_name = ?"
            " AND group_jobs-group_jobs_finished-group_jobs_failed-group_jobs_cancelled > 0";

    MXQ_MYSQL_BIND_UINT64(param, 0, &group->group_id);
    MXQ_MYSQL_BIND_UINT32(param, 1, &group->user_uid);
    MXQ_MYSQL_BIND_STRING(param, 2, group->user_name);

    res = mxq_mysql_do_update(mysql, query, param);

    return res;
}
