
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

static int load_active_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups)
{
    struct mx_mysql_stmt *stmt = NULL;
    unsigned long long num_rows = 0;
    int res;
    int cnt = 0;
    struct mxq_group *groups;


    assert(mysql);
    assert(mxq_groups);
    assert(!(*mxq_groups));

    stmt = mx_mysql_statement_prepare(mysql,
            "SELECT"
                " group_id,"
                " group_name,"
                " group_status,"
                " group_priority,"
                " user_uid,"
                " user_name,"
                " user_gid,"
                " user_group,"
                " job_command,"
                " job_threads,"
                " job_memory,"
                " job_time,"
                " group_jobs,"
                " group_jobs_running,"
                " group_jobs_finished,"
                " group_jobs_failed,"
                " group_jobs_cancelled,"
                " group_jobs_unknown,"
                " group_slots_running,"
                " stats_max_maxrss,"
                " stats_max_utime_sec,"
                " stats_max_stime_sec,"
                " stats_max_real_sec"
            " FROM mxq_group"
            " WHERE (group_jobs-group_jobs_finished-group_jobs_failed-group_jobs_cancelled-group_jobs_unknown > 0)"
            "    OR (NOW()-group_mtime < 604800)"
            " ORDER BY user_name, group_mtime"
            " LIMIT 1000");
    if (!stmt) {
        mx_log_err("mx_mysql_statement_prepare(): %m");
        return -errno;
    }

    res = mx_mysql_statement_execute(stmt, &num_rows);
    if (res < 0) {
        mx_log_err("mx_mysql_statement_execute(): %m");
        mx_mysql_statement_close(&stmt);
        return res;
    }

    if (num_rows) {
        int idx = 0;
        struct mxq_group g = {0};

        groups = mx_calloc_forever(num_rows, sizeof(*groups));

        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.group_id));
        res += mx_mysql_statement_result_bind(stmt, idx++, string, &(g.group_name));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint8,  &(g.group_status));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint16, &(g.group_priority));

        res += mx_mysql_statement_result_bind(stmt, idx++, uint32, &(g.user_uid));
        res += mx_mysql_statement_result_bind(stmt, idx++, string, &(g.user_name));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint32, &(g.user_gid));
        res += mx_mysql_statement_result_bind(stmt, idx++, string, &(g.user_group));

        res += mx_mysql_statement_result_bind(stmt, idx++, string, &(g.job_command));

        res += mx_mysql_statement_result_bind(stmt, idx++, uint16, &(g.job_threads));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.job_memory));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint32, &(g.job_time));

        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.group_jobs));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.group_jobs_running));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.group_jobs_finished));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.group_jobs_failed));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.group_jobs_cancelled));
        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.group_jobs_unknown));

        res += mx_mysql_statement_result_bind(stmt, idx++, uint64, &(g.group_slots_running));

        res += mx_mysql_statement_result_bind(stmt, idx++, uint32, &(g.stats_max_maxrss));
        res += mx_mysql_statement_result_bind(stmt, idx++, int64, &(g.stats_max_utime.tv_sec));
        res += mx_mysql_statement_result_bind(stmt, idx++, int64, &(g.stats_max_stime.tv_sec));
        res += mx_mysql_statement_result_bind(stmt, idx++, int64, &(g.stats_max_real.tv_sec));

        assert(res == 0);

        for (cnt = 0; cnt < num_rows; cnt++) {
            res = mx_mysql_statement_fetch(stmt);
            if (res < 0) {
                mx_log_err("mx_mysql_statement_fetch(): %m");
                mx_mysql_statement_close(&stmt);
                return res;
            }
            memcpy(groups+cnt, &g, sizeof(*groups));
        }
    }

    mx_mysql_statement_close(&stmt);

    *mxq_groups = groups;

    return cnt;
}


int main(int argc, char *argv[])
{
    struct mx_mysql *mysql = NULL;
    struct mxq_group *groups = NULL;
    int group_cnt;
    int i;
    int res;
    char *arg_mysql_default_group;
    char *arg_mysql_default_file;
    char     arg_debug;

    int opt;
    struct mx_getopt_ctl optctl;
    struct mx_option opts[] = {
                MX_OPTION_NO_ARG("help",                 'h'),
                MX_OPTION_NO_ARG("version",              'V'),

                MX_OPTION_NO_ARG("debug",                5),
                MX_OPTION_NO_ARG("verbose",              'v'),

                MX_OPTION_OPTIONAL_ARG("mysql-default-file",  'M'),
                MX_OPTION_OPTIONAL_ARG("mysql-default-group", 'S'),
                MX_OPTION_END
    };

    arg_debug = 0;

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = MXQ_MYSQL_DEFAULT_GROUP;

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    mx_getopt_init(&optctl, argc-1, &argv[1], opts);
    optctl.flags = MX_FLAG_STOPONUNKNOWN|MX_FLAG_STOPONNOOPT;

    while ((opt=mx_getopt(&optctl, &i)) != MX_GETOPT_END) {
        if (opt == MX_GETOPT_ERROR) {
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


    group_cnt = load_active_groups(mysql, &groups);

    for (i=0; i<group_cnt; i++) {
        struct mxq_group *g;

        g = &groups[i];

        printf("user=%s uid=%u group_id=%lu pri=%d jobs_total=%lu run_jobs=%lu run_slots=%lu failed=%lu finished=%lu cancelled=%lu unknown=%lu inq=%lu job_threads=%u job_memory=%lu job_time=%u stats_max_utime=%lu stats_max_real=%lu job_command=%s group_name=%s\n",
                g->user_name, g->user_uid, g->group_id, g->group_priority, g->group_jobs,
                g->group_jobs_running, g->group_slots_running, g->group_jobs_failed, g->group_jobs_finished, g->group_jobs_cancelled, g->group_jobs_unknown,
                mxq_group_jobs_inq(g),
                g->job_threads, g->job_memory, g->job_time, g->stats_max_utime.tv_sec, g->stats_max_real.tv_sec,
                g->job_command, g->group_name);

        mxq_group_free_content(&groups[i]);
    }

    free(groups);

    mx_mysql_finish(&mysql);
    mx_log_info("MySQL: Connection to database closed.");

    return 1;
};

