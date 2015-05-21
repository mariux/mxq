
#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>

#include <sysexits.h>

#include <mysql.h>

#include "mxq_util.h"
#include "mxq_mysql.h"
#include "mx_getopt.h"

#include "mxq_group.h"

#ifndef MXQ_VERSION
#define MXQ_VERSION "0.00"
#endif

#ifndef MXQ_VERSIONFULL
#define MXQ_VERSIONFULL "MXQ v0.00 super alpha 0"
#endif

#ifndef MXQ_VERSIONDATE
#define MXQ_VERSIONDATE "2015"
#endif

#define MYSQL_DEFAULT_FILE     MXQ_MYSQL_DEFAULT_FILE
#define MYSQL_DEFAULT_GROUP    "mxqdump"

static void print_version(void)
{
    printf(
    "mxqdump - " MXQ_VERSIONFULL "\n"
    "  by Marius Tolzmann <tolzmann@molgen.mpg.de> " MXQ_VERSIONDATE "\n"
    "  Max Planck Institute for Molecular Genetics - Berlin Dahlem\n"
    );
}

static void print_usage(void)
{
    print_version();
    printf(
    "\n"
    "Usage:\n"
    "  mxqdump [options]\n"
    "\n"
    "Synopsis:\n"
    "  Dump status infromation of MXQ cluster.\n"
    "\n"
    "options:\n\n"
    "  -V | --version\n"
    "  -h | --help\n"
    "\n"
    "Change how to connect to the mysql server:\n"
    "\n"
    "  -M | --mysql-default-file [mysql-file]    default: " MYSQL_DEFAULT_FILE "\n"
    "  -S | --mysql-default-group [mysql-group]  default: " MYSQL_DEFAULT_GROUP "\n"
    "\n"
    "Environment:\n"
    "  MXQ_MYSQL_DEFAULT_FILE   change default for [mysql-file]\n"
    "  MXQ_MYSQL_DEFAULT_GROUP  change default for [mysql-group]\n"
    "\n"
    );
}

int main(int argc, char *argv[])
{
    struct mxq_mysql mmysql;
    MYSQL *mysql;
    struct mxq_group *groups;
    int group_cnt;
    int i;
    char *arg_mysql_default_group;
    char *arg_mysql_default_file;

    int opt;
    struct mx_getopt_ctl optctl;
    struct mx_option opts[] = {
                MX_OPTION_NO_ARG("help",                 'h'),
                MX_OPTION_NO_ARG("version",              'V'),

                MX_OPTION_OPTIONAL_ARG("mysql-default-file",  'M'),
                MX_OPTION_OPTIONAL_ARG("mysql-default-group", 'S'),
                MX_OPTION_END
    };

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = MYSQL_DEFAULT_GROUP;

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MYSQL_DEFAULT_FILE;


    mx_getopt_init(&optctl, argc-1, &argv[1], opts);
    optctl.flags = MX_FLAG_STOPONUNKNOWN|MX_FLAG_STOPONNOOPT;

    while ((opt=mx_getopt(&optctl, &i)) != MX_GETOPT_END) {
        if (opt == MX_GETOPT_ERROR) {
            exit(EX_USAGE);
        }

        switch (opt) {
            case 'V':
                print_version();
                exit(EX_USAGE);

            case 'h':
                print_usage();
                exit(EX_USAGE);

            case 'M':
                arg_mysql_default_file = optctl.optarg;
                break;

            case 'S':
                arg_mysql_default_group = optctl.optarg;
                break;
        }
    }

    MX_GETOPT_FINISH(optctl, argc, argv);

    mmysql.default_file  = arg_mysql_default_file;
    mmysql.default_group = arg_mysql_default_group;

    mysql = mxq_mysql_connect(&mmysql);

    group_cnt = mxq_group_load_active_groups(mysql, &groups);

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

    mxq_mysql_close(mysql);

    return 1;
};

