
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

#include <ctype.h>

#include <mysql.h>
#include <string.h>

#include "mx_log.h"
#include "mx_util.h"
#include "mx_mysql.h"
#include "mx_getopt.h"

#include "mxq_group.h"
#include "mxq_job.h"

#include "mxq.h"

#define UINT64_UNSET       (uint64_t)(-1)
#define UINT64_ALL         (uint64_t)(-2)
#define UINT64_SPECIAL_MIN (uint64_t)(-2)
#define UINT64_HASVALUE(x) ((x) < UINT64_SPECIAL_MIN)

enum mode {
    MODE_UNSET=0,
    MODE_CLOSE,
    MODE_REOPEN
};

static void print_usage(void)
{
    mxq_print_generic_version();
    printf(
    "\n"
    "Usage:\n"
    "  %s [options]\n"
    "\n"
    "options:\n"
    "\n"
    "  -c, --close=GROUPID      close group <GROUPID>\n"
    "  -o, --reopen=GROUPID     reopen group <GROUPID>\n"
    "\n"
    "  -v, --verbose   be more verbose\n"
    "      --debug     set debug log level (default: warning log level)\n"
    "\n"
    "  -V, --version\n"
    "  -h, --help\n"
    "\n"
    "Change how to connect to the mysql server:\n"
    "\n"
    "  -M, --mysql-default-file[=MYSQLCNF]     (default: %s)\n"
    "  -S, --mysql-default-group[=MYSQLGROUP]  (default: %s)\n"
    "\n"
    "Environment:\n"
    "  MXQ_MYSQL_DEFAULT_FILE   change default for MYSQLCNF\n"
    "  MXQ_MYSQL_DEFAULT_GROUP  change default for MYSQLGROUP\n"
    "\n",
        program_invocation_short_name,
        MXQ_MYSQL_DEFAULT_FILE_STR,
        MXQ_MYSQL_DEFAULT_GROUP_STR
    );
}

static int update_group_flags_closed(struct mx_mysql *mysql, uint64_t group_id, uint32_t user_uid)
{
    struct mx_mysql_stmt *stmt = NULL;
    unsigned long long num_rows = 0;
    int res;
    uint64_t newflags = 0;

    newflags |= MXQ_GROUP_FLAG_CLOSED;

    stmt = mx_mysql_statement_prepare(mysql,
            "UPDATE mxq_group SET"
                " group_flags = group_flags | ?"
                " WHERE group_id = ?"
                " AND user_uid = ?"
            );
    if (!stmt) {
        mx_log_err("mx_mysql_statement_prepare(): %m");
        return -(errno=EIO);
    }

    res  = mx_mysql_statement_param_bind(stmt, 0, uint64, &(newflags));
    res += mx_mysql_statement_param_bind(stmt, 1, uint64, &(group_id));
    res += mx_mysql_statement_param_bind(stmt, 2, uint32, &(user_uid));
    assert(res == 0);

    res = mx_mysql_statement_execute(stmt, &num_rows);

    if (res < 0)
        mx_log_err("mx_mysql_statement_execute(): %m");

    mx_mysql_statement_close(&stmt);

    if (res < 0)
        return -(errno=-res);

    assert(num_rows <= 1);
    return (int)num_rows;
}

static int update_group_flags_reopen(struct mx_mysql *mysql, uint64_t group_id, uint32_t user_uid)
{
    struct mx_mysql_stmt *stmt = NULL;
    unsigned long long num_rows = 0;
    int res;
    uint64_t flagsnottobeset;
    uint64_t newflags = 0;

    flagsnottobeset = MXQ_GROUP_FLAG_IS_DEPENDENCY;
    newflags |= MXQ_GROUP_FLAG_CLOSED;

    stmt = mx_mysql_statement_prepare(mysql,
            "UPDATE mxq_group SET"
                " group_flags = group_flags & ~(?)"
                " WHERE group_id = ?"
                " AND user_uid = ?"
                " AND group_flags & ? = 0"
            );
    if (!stmt) {
        mx_log_err("mx_mysql_statement_prepare(): %m");
        return -(errno=EIO);
    }

    res  = mx_mysql_statement_param_bind(stmt, 0, uint64, &(newflags));
    res += mx_mysql_statement_param_bind(stmt, 1, uint64, &(group_id));
    res += mx_mysql_statement_param_bind(stmt, 2, uint32, &(user_uid));
    res += mx_mysql_statement_param_bind(stmt, 3, uint64, &(flagsnottobeset));
    assert(res == 0);

    res = mx_mysql_statement_execute(stmt, &num_rows);

    if (res < 0)
        mx_log_err("mx_mysql_statement_execute(): %m");

    mx_mysql_statement_close(&stmt);

    if (res < 0)
        return -(errno=-res);

    assert(num_rows <= 1);
    return (int)num_rows;
}

int _close_group_for_user(struct mx_mysql *mysql, uint64_t group_id, uint64_t user_uid)
{
    int res;

    res = update_group_flags_closed(mysql, group_id, user_uid);

    if (res == 0) {
        mx_log_warning("no group with group_id=%lu found for user with uid=%d",
                group_id, user_uid);
        return -(errno=ENOENT);
    }

    if (res < 0) {
        mx_log_err("closing group failed: %m");
        return res;
    }

    assert(res == 1);

    mx_log_notice("closing group %lu succeded.", group_id);
    return 0;
}

int _reopen_group_for_user(struct mx_mysql *mysql, uint64_t group_id, uint64_t user_uid)
{
    int res;

    res = update_group_flags_reopen(mysql, group_id, user_uid);

    if (res == 0) {
        mx_log_warning("no group with group_id=%lu found for user with uid=%d",
                group_id, user_uid);
        return -(errno=ENOENT);
    }

    if (res < 0) {
        mx_log_err("opening group failed: %m");
        return res;
    }

    assert(res == 1);

    mx_log_notice("opening group %lu succeded.", group_id);
    return 0;
}

int main(int argc, char *argv[])
{
    struct mx_mysql *mysql = NULL;

    uid_t ruid, euid, suid;
    struct passwd *passwd;

    int res;

    uint64_t  arg_group_id;
    char      arg_debug;
    uint64_t  arg_uid;
    enum mode arg_mode;

    char *arg_mysql_default_group;
    char *arg_mysql_default_file;

    int i;
    int opt;
    struct mx_getopt_ctl optctl;
    struct mx_option opts[] = {
                MX_OPTION_NO_ARG("help",                 'h'),
                MX_OPTION_NO_ARG("version",              'V'),

                MX_OPTION_NO_ARG("debug",                5),
                MX_OPTION_NO_ARG("verbose",              'v'),

                MX_OPTION_REQUIRED_ARG("user",     'u'),

                MX_OPTION_REQUIRED_ARG("close",    'c'),
                MX_OPTION_REQUIRED_ARG("reopen",   'o'),

                MX_OPTION_OPTIONAL_ARG("mysql-default-file",  'M'),
                MX_OPTION_OPTIONAL_ARG("mysql-default-group", 'S'),
                MX_OPTION_END
    };

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = MXQ_MYSQL_DEFAULT_GROUP;

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    arg_group_id = 0;
    arg_debug    = 0;
    arg_mode     = MODE_UNSET;
    arg_uid      = UINT64_UNSET;

    mx_log_level_set(MX_LOG_NOTICE);

    res = getresuid(&ruid, &euid, &suid);
    assert(res != -1);

    mx_getopt_init(&optctl, argc-1, &argv[1], opts);
    optctl.flags = MX_FLAG_STOPONNOOPT;

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

            case 'u':
                passwd = getpwnam(optctl.optarg);
                if (passwd) {
                    arg_uid = passwd->pw_uid;
                    break;
                }
                mx_log_debug("user %s not found. trying numeric uid.", optctl.optarg);

                if (!isdigit(*optctl.optarg)) {
                    mx_log_err("Invalid argument for --user '%s': User not found.", optctl.optarg);
                    exit(EX_USAGE);
                }

                if (mx_strtou64(optctl.optarg, &arg_uid) < 0 || arg_uid >= UINT64_SPECIAL_MIN) {
                    if (arg_uid >= UINT64_SPECIAL_MIN)
                        errno = ERANGE;
                    mx_log_err("Invalid argument for --user '%s': %m", optctl.optarg);
                    exit(EX_USAGE);
                }
                errno = 0;
                passwd = getpwuid(arg_uid);
                if (!passwd) {
                    if (errno)
                        mx_log_err("Can't load user '%s': %m");
                    else
                        mx_log_err("Invalid argument for --user '%s': User not found.", optctl.optarg);
                    exit(EX_USAGE);
                }
                break;

            case 'c':
                if (mx_strtou64(optctl.optarg, &arg_group_id) < 0 || !arg_group_id) {
                    if (!arg_group_id)
                        errno = ERANGE;
                    mx_log_err("Invalid argument for --close '%s': %m", optctl.optarg);
                    exit(EX_CONFIG);
                }
                arg_mode = MODE_CLOSE;
                break;

            case 'o':
                if (mx_strtou64(optctl.optarg, &arg_group_id) < 0 || !arg_group_id) {
                    if (!arg_group_id)
                        errno = ERANGE;
                    mx_log_err("Invalid argument for --reopen '%s': %m", optctl.optarg);
                    exit(EX_CONFIG);
                }
                arg_mode = MODE_REOPEN;
                break;
        }
    }

    MX_GETOPT_FINISH(optctl, argc, argv);

    if (!arg_group_id) {
        print_usage();
        exit(EX_USAGE);
    }

    if (arg_uid == UINT64_UNSET)
        arg_uid = ruid;

    if (arg_uid != ruid && ruid != 0) {
        mx_log_err("Nice try, but only root user may kill jobs of other users! Better luck next time.");
        exit(EX_USAGE);
    }

    if (!passwd) {
        errno = 0;
        passwd = getpwuid(arg_uid);
        if (!passwd && errno) {
            mx_log_err("Can't load user with uid '%lu': %m", arg_uid);
            exit(EX_IOERR);
        }
        if (!passwd) {
            assert(arg_uid == ruid);
            mx_log_err("Can't load current user with uid '%lu'.", arg_uid);
            exit(EX_NOUSER);
        }
    }

    res = mx_mysql_initialize(&mysql);
    assert(res == 0);

    mx_mysql_option_set_default_file(mysql, arg_mysql_default_file);
    mx_mysql_option_set_default_group(mysql, arg_mysql_default_group);

    res = mx_mysql_connect_forever(&mysql);
    assert(res == 0);

    mx_log_info("MySQL: Connection to database established.");

    if (arg_mode == MODE_CLOSE) {
        res = _close_group_for_user(mysql, arg_group_id, arg_uid);

        mx_mysql_finish(&mysql);
        mx_log_info("MySQL: Connection to database closed.");

        return (res < 0);
    } else if (arg_mode == MODE_REOPEN) {
        res = _reopen_group_for_user(mysql, arg_group_id, arg_uid);

        mx_mysql_finish(&mysql);
        mx_log_info("MySQL: Connection to database closed.");

        return (res < 0);
    }

    mx_mysql_finish(&mysql);
    mx_log_info("MySQL: Connection to database closed.");
    return 1;
}

