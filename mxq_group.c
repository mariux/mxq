
#include <stdio.h>

#include <assert.h>

#include <mysql.h>

#include "mx_log.h"

#include "mxq_group.h"
#include "mxq_job.h"
#include "mxq_mysql.h"
#include "mx_mysql.h"

#define GROUP_FIELDS_CNT 25
#define GROUP_FIELDS \
            " group_id," \
            " group_name," \
            " group_status," \
            " group_flags," \
            " group_priority," \
            " user_uid," \
            " user_name," \
            " user_gid," \
            " user_group," \
            " job_command," \
            " job_threads," \
            " job_memory," \
            " job_time," \
            " group_jobs," \
            " group_jobs_inq," \
            " group_jobs_running," \
            " group_jobs_finished," \
            " group_jobs_failed," \
            " group_jobs_cancelled," \
            " group_jobs_unknown," \
            " group_slots_running," \
            " stats_max_maxrss," \
            " stats_max_utime_sec," \
            " stats_max_stime_sec," \
            " stats_max_real_sec"

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
                "group_jobs_unknown," \
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
    MXQ_GROUP_COL_GROUP_JOBS_UNKNOWN,
    MXQ_GROUP_COL_GROUP_SLOTS_RUNNING,
    MXQ_GROUP_COL_STATS_MAX_MAXRSS,
    MXQ_GROUP_COL_STATS_MAX_UTIME_SEC,
    MXQ_GROUP_COL_STATS_MAX_STIME_SEC,
    MXQ_GROUP_COL_STATS_MAX_REAL_SEC,
    MXQ_GROUP_COL__END
};

static int bind_result_group_fields(struct mx_mysql_bind *result, struct mxq_group *g)
{
    int res = 0;
    int idx = 0;

    res = mx_mysql_bind_init_result(result, GROUP_FIELDS_CNT);
    assert(res >= 0);

    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_id));
    res += mx_mysql_bind_var(result, idx++, string, &(g->group_name));
    res += mx_mysql_bind_var(result, idx++, uint8,  &(g->group_status));
    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_flags));
    res += mx_mysql_bind_var(result, idx++, uint16, &(g->group_priority));

    res += mx_mysql_bind_var(result, idx++, uint32, &(g->user_uid));
    res += mx_mysql_bind_var(result, idx++, string, &(g->user_name));
    res += mx_mysql_bind_var(result, idx++, uint32, &(g->user_gid));
    res += mx_mysql_bind_var(result, idx++, string, &(g->user_group));

    res += mx_mysql_bind_var(result, idx++, string, &(g->job_command));

    res += mx_mysql_bind_var(result, idx++, uint16, &(g->job_threads));
    res += mx_mysql_bind_var(result, idx++, uint64, &(g->job_memory));
    res += mx_mysql_bind_var(result, idx++, uint32, &(g->job_time));

    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_jobs));
    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_jobs_inq));
    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_jobs_running));
    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_jobs_finished));
    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_jobs_failed));
    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_jobs_cancelled));
    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_jobs_unknown));

    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_slots_running));

    res += mx_mysql_bind_var(result, idx++, uint32, &(g->stats_max_maxrss));
    res += mx_mysql_bind_var(result, idx++, int64, &(g->stats_max_utime.tv_sec));
    res += mx_mysql_bind_var(result, idx++, int64, &(g->stats_max_stime.tv_sec));
    res += mx_mysql_bind_var(result, idx++, int64, &(g->stats_max_real.tv_sec));

    return res;
}

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
    MXQ_MYSQL_BIND_UINT64(bind, MXQ_GROUP_COL_GROUP_JOBS_UNKNOWN,   &g->group_jobs_unknown);

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
    done += g->group_jobs_unknown;

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

int mxq_load_group(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t group_id)
{
    int res;
    struct mxq_group *groups = NULL;
    struct mxq_group g = {0};
    struct mx_mysql_bind param = {0};
    struct mx_mysql_bind result = {0};

    assert(mysql);
    assert(mxq_groups);
    assert(!(*mxq_groups));

    char *query =
            "SELECT"
                GROUP_FIELDS
            " FROM mxq_group"
            " WHERE group_id = ?"
            " LIMIT 1";

    res = mx_mysql_bind_init_param(&param, 1);
    assert(res == 0);
    res = mx_mysql_bind_var(&param, 0, uint64, &group_id);
    assert(res == 0);

    res = bind_result_group_fields(&result, &g);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, &param, &result, &g, (void **)&groups, sizeof(*groups));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *mxq_groups = groups;
    return res;
}

int mxq_load_all_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups)
{
    int res;
    struct mxq_group *groups = NULL;
    struct mxq_group g = {0};
    struct mx_mysql_bind result = {0};

    assert(mysql);
    assert(mxq_groups);
    assert(!(*mxq_groups));

    char *query =
            "SELECT"
                GROUP_FIELDS
            " FROM mxq_group"
            " ORDER BY user_name, group_mtime"
            " LIMIT 1000";

    res = bind_result_group_fields(&result, &g);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, NULL, &result, &g, (void **)&groups, sizeof(*groups));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *mxq_groups = groups;
    return res;
}

int mxq_load_all_groups_for_user(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t user_uid)
{
    int res;
    struct mxq_group *groups = NULL;
    struct mxq_group g = {0};
    struct mx_mysql_bind param = {0};
    struct mx_mysql_bind result = {0};

    assert(mysql);
    assert(mxq_groups);
    assert(!(*mxq_groups));

    char *query =
            "SELECT"
                GROUP_FIELDS
            " FROM mxq_group"
            " WHERE user_uid = ?"
            " ORDER BY user_name, group_mtime"
            " LIMIT 1000";

    res = mx_mysql_bind_init_param(&param, 1);
    assert(res == 0);
    res = mx_mysql_bind_var(&param, 0, uint64, &user_uid);
    assert(res == 0);

    res = bind_result_group_fields(&result, &g);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, &param, &result, &g, (void **)&groups, sizeof(*groups));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *mxq_groups = groups;
    return res;
}

int mxq_load_active_groups_for_user(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t user_uid)
{
    int res;
    struct mxq_group *groups = NULL;
    struct mxq_group g = {0};
    struct mx_mysql_bind result = {0};
    struct mx_mysql_bind param = {0};

    assert(mysql);
    assert(mxq_groups);
    assert(!(*mxq_groups));

    char *query =
            "SELECT"
                GROUP_FIELDS
            " FROM mxq_group"
            " WHERE ((group_jobs_inq > 0 OR group_jobs_running > 0)"
            "    OR (NOW()-group_mtime < 604800))"
            "   AND user_uid = ?"
            " ORDER BY user_name, group_mtime"
            " LIMIT 1000";

    res = mx_mysql_bind_init_param(&param, 1);
    assert(res == 0);
    res = mx_mysql_bind_var(&param, 0, uint64, &user_uid);
    assert(res == 0);

    res = bind_result_group_fields(&result, &g);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, &param, &result, &g, (void **)&groups, sizeof(*groups));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *mxq_groups = groups;
    return res;
}

int mxq_load_running_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups)
{
    int res;
    struct mxq_group *groups = NULL;
    struct mxq_group g = {0};
    struct mx_mysql_bind result = {0};

    assert(mysql);
    assert(mxq_groups);
    assert(!(*mxq_groups));

    char *query =
            "SELECT"
                GROUP_FIELDS
            " FROM mxq_group"
            " WHERE (group_jobs_inq > 0 OR group_jobs_running > 0)"
            " ORDER BY user_name, group_mtime"
            " LIMIT 1000";

    res = bind_result_group_fields(&result, &g);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, NULL, &result, &g, (void **)&groups, sizeof(*groups));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *mxq_groups = groups;
    return res;
}

int mxq_load_running_groups_for_user(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t user_uid)
{
    int res;
    struct mxq_group *groups = NULL;
    struct mxq_group g = {0};
    struct mx_mysql_bind param = {0};
    struct mx_mysql_bind result = {0};

    assert(mysql);
    assert(mxq_groups);
    assert(!(*mxq_groups));

    char *query =
            "SELECT"
                GROUP_FIELDS
            " FROM mxq_group"
            " WHERE (group_jobs_inq > 0 OR group_jobs_running > 0)"
            "   AND user_uid = ?"
            " ORDER BY user_name, group_mtime"
            " LIMIT 1000";

    res = mx_mysql_bind_init_param(&param, 1);
    assert(res == 0);
    res = mx_mysql_bind_var(&param, 0, uint64, &user_uid);
    assert(res == 0);

    res = bind_result_group_fields(&result, &g);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, &param, &result, &g, (void **)&groups, sizeof(*groups));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *mxq_groups = groups;
    return res;
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
            " WHERE group_jobs-group_jobs_finished-group_jobs_failed-group_jobs_cancelled-group_jobs_unknown > 0"
            " ORDER BY user_uid, group_priority DESC";

    stmt = mxq_mysql_stmt_do_query(mysql, query, MXQ_GROUP_COL__END, NULL, result);
    if (!stmt) {
        mx_log_err("mxq_mysql_stmt_do_query(mysql=%p, stmt_str=\"%s\", field_count=%d, param=%p, result=%p)", mysql, query, MXQ_GROUP_COL__END, NULL, result);
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
