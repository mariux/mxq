
#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>

#include <sysexits.h>

#include <mysql.h>

#include "mx_getopt.h"

#include "mxq.h"

#include "mxq_util.h"
#include "mx_mysql.h"

#include "mxq_group.h"

#define GROUP_FIELDS \
            " group_id," \
            " group_name," \
            " group_status," \
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

static void print_usage(void)
{
    mxq_print_generic_version();
    printf(
    "\n"
    "Usage:\n"
    "  %s [options]\n"
    "\n"
    "Synopsis:\n"
    "  Dump status information of MXQ cluster.\n"
    "\n"
    "options:\n\n"
    "  -g | --group-id <group-id>   dump group with <group-id>\n"
    "  -a | --all                   dump all groups\n"
    "\n"
    "  -V | --version\n"
    "  -h | --help\n"
    "\n"
    "Change how to connect to the mysql server:\n"
    "\n"
    "  -M | --mysql-default-file  [mysql-file]   (default: %s)\n"
    "  -S | --mysql-default-group [mysql-group]  (default: %s)\n"
    "\n"
    "Environment:\n"
    "  MXQ_MYSQL_DEFAULT_FILE   change default for [mysql-file]\n"
    "  MXQ_MYSQL_DEFAULT_GROUP  change default for [mysql-group]\n"
    "\n",
    program_invocation_short_name,
    MXQ_MYSQL_DEFAULT_FILE_STR,
    MXQ_MYSQL_DEFAULT_GROUP_STR
    );
}

static int bind_result_group_fields(struct mx_mysql_bind *result, struct mxq_group *g)
{
    int res = 0;
    int idx = 0;

    res = mx_mysql_bind_init_result(result, 23);
    assert(res >= 0);

    res += mx_mysql_bind_var(result, idx++, uint64, &(g->group_id));
    res += mx_mysql_bind_var(result, idx++, string, &(g->group_name));
    res += mx_mysql_bind_var(result, idx++, uint8,  &(g->group_status));
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

static int load_active_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups)
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
            " WHERE (group_jobs-group_jobs_finished-group_jobs_failed-group_jobs_cancelled-group_jobs_unknown > 0)"
            "    OR (NOW()-group_mtime < 604800)"
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

static int load_all_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups)
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

static int load_group_by_id(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t group_id)
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

static int print_group(struct mxq_group *g)
{
    return printf("user=%s uid=%u group_id=%lu pri=%d jobs_total=%lu run_jobs=%lu run_slots=%lu failed=%lu"
                    " finished=%lu cancelled=%lu unknown=%lu inq=%lu"
                    " job_threads=%u job_memory=%lu job_time=%u"
                    " memory_load=%lu%% time_load=%lu%%"
                    " max_utime=%lu max_real=%lu max_memory=%u job_command=%s group_name=%s\n",
        g->user_name, g->user_uid, g->group_id, g->group_priority, g->group_jobs,
        g->group_jobs_running, g->group_slots_running, g->group_jobs_failed,
        g->group_jobs_finished, g->group_jobs_cancelled, g->group_jobs_unknown,
        mxq_group_jobs_inq(g),
        g->job_threads, g->job_memory, g->job_time,
        (100*g->stats_max_maxrss/1024/g->job_memory),
        (100*g->stats_max_real.tv_sec/60/g->job_time),
        g->stats_max_utime.tv_sec/60, g->stats_max_real.tv_sec/60,
        g->stats_max_maxrss/1024, g->job_command, g->group_name);
}

static int dump_groups_by_id(struct mx_mysql *mysql, uint64_t group_id)
{
    struct mxq_group *g = NULL;
    struct mxq_group *groups = NULL;

    int group_cnt;
    int i;

    if (group_id == (uint64_t)(-1))
        group_cnt = load_active_groups(mysql, &groups);
    else if (group_id == 0)
        group_cnt = load_all_groups(mysql, &groups);
    else
        group_cnt = load_group_by_id(mysql, &groups, group_id);

    mx_log_debug("%d groups loaded.", group_cnt);

    for (i=0; i<group_cnt; i++) {
        g = &groups[i];
        print_group(g);
        mxq_group_free_content(g);
    }

    mx_free_null(groups);

    return group_cnt;
}

int main(int argc, char *argv[])
{
    struct mx_mysql *mysql = NULL;
    int i;
    int res;
    char *arg_mysql_default_group;
    char *arg_mysql_default_file;

    char     arg_debug;
    uint64_t arg_group_id;

    int opt;
    struct mx_getopt_ctl optctl;
    struct mx_option opts[] = {
                MX_OPTION_NO_ARG("help",                 'h'),
                MX_OPTION_NO_ARG("version",              'V'),

                MX_OPTION_NO_ARG("debug",                5),
                MX_OPTION_NO_ARG("verbose",              'v'),

                MX_OPTION_NO_ARG("all",              'a'),

                MX_OPTION_REQUIRED_ARG("group-id",       'g'),

                MX_OPTION_OPTIONAL_ARG("mysql-default-file",  'M'),
                MX_OPTION_OPTIONAL_ARG("mysql-default-group", 'S'),
                MX_OPTION_END
    };

    arg_debug    = 0;
    arg_group_id = (uint64_t)(-1);   // set all bits ..

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = MXQ_MYSQL_DEFAULT_GROUP;

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    mx_getopt_init(&optctl, argc-1, &argv[1], opts);
    optctl.flags = 0;

    while ((opt=mx_getopt(&optctl, &i)) != MX_GETOPT_END) {
        if (opt == MX_GETOPT_ERROR) {
            print_usage();
            exit(EX_USAGE);
        }

        switch (opt) {
            case 'V':
                mxq_print_generic_version();
                exit(EX_USAGE);

            case 'h':
                print_usage();
                exit(EX_USAGE);

            case 5:
                arg_debug = 1;
                mx_log_level_set(MX_LOG_DEBUG);
                break;

            case 'a':
                arg_group_id = 0;
                break;

            case 'g':
                if (mx_strtou64(optctl.optarg, &arg_group_id) < 0 || !arg_group_id || arg_group_id == (uint64_t)(-1)) {
                    if (!arg_group_id || arg_group_id == (uint64_t)(-1))
                        errno = ERANGE;
                    mx_log_err("Invalid argument for --group-id '%s': %m", optctl.optarg);
                    exit(1);
                }
                break;

            case 'v':
                if (!arg_debug)
                    mx_log_level_set(MX_LOG_INFO);
                break;

            case 'M':
                arg_mysql_default_file = optctl.optarg;
                break;

            case 'S':
                arg_mysql_default_group = optctl.optarg;
                break;
        }
    }

    MX_GETOPT_FINISH(optctl, argc, argv);

    res = mx_mysql_init(&mysql);
    assert(res == 0);

    mx_mysql_option_set_default_file(mysql, arg_mysql_default_file);
    mx_mysql_option_set_default_group(mysql, arg_mysql_default_group);

    res = mx_mysql_connect_forever(&mysql);
    assert(res == 0);

    mx_log_info("MySQL: Connection to database established.");

    dump_groups_by_id(mysql, arg_group_id);

    mx_mysql_finish(&mysql);
    mx_log_info("MySQL: Connection to database closed.");

    return 0;
};

