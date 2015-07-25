
#include <stdio.h>

#include <assert.h>

#include <mysql.h>

#include "mx_log.h"

#include "mxq_group.h"
#include "mxq_job.h"
#include "mx_util.h"
#include "mx_mysql.h"

#define GROUP_FIELDS_CNT 26
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
            " group_jobs_restarted," \
            " group_slots_running," \
            " stats_max_maxrss," \
            " stats_max_utime_sec," \
            " stats_max_stime_sec," \
            " stats_max_real_sec"

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

    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_jobs_restarted));

    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_slots_running));

    res += mx_mysql_bind_var(result, idx++, uint32, &(g->stats_max_maxrss));
    res += mx_mysql_bind_var(result, idx++, int64, &(g->stats_max_utime.tv_sec));
    res += mx_mysql_bind_var(result, idx++, int64, &(g->stats_max_stime.tv_sec));
    res += mx_mysql_bind_var(result, idx++, int64, &(g->stats_max_real.tv_sec));

    return res;
}

void mxq_group_free_content(struct mxq_group *g)
{
        mx_free_null(g->group_name);
        g->_group_name_length = 0;

        mx_free_null(g->user_name);
        g->_user_name_length = 0;

        mx_free_null(g->user_group);
        g->_user_group_length = 0;

        mx_free_null(g->job_command);
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

int mxq_load_active_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups)
{
    int res;
    struct mxq_group *groups = NULL;
    struct mxq_group g = {0};
    struct mx_mysql_bind result = {0};

    assert(mysql);
    assert(mxq_groups);

    *mxq_groups = NULL;

    char *query =
            "SELECT"
                GROUP_FIELDS
            " FROM mxq_group"
            " WHERE (group_jobs_inq > 0 OR group_jobs_running > 0)"
            " ORDER BY user_name, group_mtime"
            " LIMIT 1000";

    res = bind_result_group_fields(&result, &g);
    assert(res == 0);

    res = mx_mysql_do_statement_retry_on_fail(mysql, query, NULL, &result, &g, (void **)&groups, sizeof(*groups));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement_retry_on_fail(): %m");
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

