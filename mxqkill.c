
#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>

#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>

#include <assert.h>

#include <sysexits.h>

#include <mysql.h>

#include "mx_log.h"
#include "mx_util.h"
#include "mxq_util.h"
#include "mxq_mysql.h"
#include "mx_getopt.h"

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
#define MYSQL_DEFAULT_GROUP    "mxqkill"

static void print_version(void)
{
    printf(
    "mxqkill - " MXQ_VERSIONFULL "\n"
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
    "  mxqkill [options]\n"
    "  mxqkill [options] --group-id <group-id>\n"
    "\n"
    "Synopsis:\n"
    "  kill/cancel jobs running in MXQ cluster\n"
    "\n"
    "options:\n"
    "\n"
    "  -g | --group-id <group-id>    cancel/kill group <group-id>\n"
    "\n"
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

    struct mxq_group group;

    uid_t ruid, euid, suid;
    struct passwd *passwd;
    uint64_t arg_group_id;

    int res;

    char *arg_mysql_default_group;
    char *arg_mysql_default_file;

    int i;
    int opt;
    struct bee_getopt_ctl optctl;
    struct bee_option opts[] = {
                BEE_OPTION_NO_ARG("help",                 'h'),
                BEE_OPTION_NO_ARG("version",              'V'),

                BEE_OPTION_REQUIRED_ARG("group-id", 'g'),

                BEE_OPTION_OPTIONAL_ARG("mysql-default-file",  'M'),
                BEE_OPTION_OPTIONAL_ARG("mysql-default-group", 'S'),
                BEE_OPTION_END
    };

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = MYSQL_DEFAULT_GROUP;

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MYSQL_DEFAULT_FILE;

    arg_group_id = 0;

    mx_log_level_set(MX_LOG_INFO);

    bee_getopt_init(&optctl, argc-1, &argv[1], opts);
    optctl.flags = BEE_FLAG_STOPONUNKNOWN|BEE_FLAG_STOPONNOOPT;

    while ((opt=bee_getopt(&optctl, &i)) != BEE_GETOPT_END) {
        if (opt == BEE_GETOPT_ERROR) {
            exit(EX_USAGE);
        }

        switch (opt) {
            case 'V':
                print_version();
                exit(EX_USAGE);

            case 'h':
                print_usage();
                exit(EX_USAGE);

            case 'g':
                if (mx_strtou64(optctl.optarg, &arg_group_id) < 0 || !arg_group_id) {
                    if (!arg_group_id)
                        errno = ERANGE;
                    mx_log_err("Invalid argument for --group-id '%s': %m", optctl.optarg);
                    exit(1);
                }
                break;

            case 'M':
                arg_mysql_default_file = optctl.optarg;
                break;

            case 'S':
                arg_mysql_default_group = optctl.optarg;
                break;
        }
    }

    BEE_GETOPT_FINISH(optctl, argc, argv);

    mmysql.default_file  = arg_mysql_default_file;
    mmysql.default_group = arg_mysql_default_group;


    if (!arg_group_id) {
        print_usage();
        exit(EX_USAGE);
    }

    mysql = mxq_mysql_connect(&mmysql);


    if (arg_group_id) {
        memset(&group, 0, sizeof(group));

        res = getresuid(&ruid, &euid, &suid);
        assert(res != -1);

        passwd = getpwuid(ruid);
        assert(passwd != NULL);

        group.group_id  = arg_group_id;
        group.user_uid  = ruid;
        group.user_name = passwd->pw_name;

        res = mxq_group_update_status_cancelled(mysql, &group);
        if (res < 0) {
            mxq_mysql_close(mysql);

            if (errno == ENOENT) {
                mx_log_err("no active group with group_id=%lu found for user=%s(%d)",
                        group.group_id, group.user_name, group.user_uid);
                return 1;
            }
            mx_log_err("cancelling group failed: %m");
        } else if (res) {
            assert(res == 1);

            res = mxq_job_update_status_cancelled_by_group(mysql, &group);
            mxq_mysql_close(mysql);

            if (res == -1 && errno == ENOENT)
                res=0;

            if (res >= 0) {
                mx_log_info("cancelled %d jobs in group with group_id=%lu",
                        res, group.group_id);
                mx_log_info("marked all running jobs in group with group_id=%lu to be killed by executing servers.",
                        group.group_id);
                mx_log_info("deactivated group with group_id=%lu",
                        group.group_id);
                return 0;
            } else {
                mx_log_err("cancelling jobs failed: %m");
            }
        }
    }

    return 1;
};

