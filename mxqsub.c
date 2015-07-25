#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>


#include <pwd.h>
#include <grp.h>

#include <assert.h>

#include <sysexits.h>

#include <stdarg.h>

#include <my_global.h>
#include <mysql.h>

#include <inttypes.h>

#include <sys/time.h>
#include <sys/resource.h>

#include <string.h>

#include "mxq_util.h"

#include "mxq_group.h"
#include "mxq_job.h"

#include "mx_log.h"
#include "mx_util.h"
#include "mx_getopt.h"
#include "mx_mysql.h"

#include "mxq.h"

#define MXQ_TASK_JOB_FORCE_APPEND  (1<<0)
#define MXQ_TASK_JOB_FORCE_NEW     (1<<1)

#define MXQ_JOB_STATUS_ACTIVE      (1)


static void print_usage(void)
{
    mxq_print_generic_version();
    printf(
    "\n"
    "Usage:\n"
    "  %s [options] <command [arguments]>\n"
    "\n"
    "Synopsis:\n"
    "  queue a job to be executed on a cluster node.\n"
    "  <command [arguments]> will be executed on a node that offers\n"
    "  enough resources to run the job. the following [options] can\n"
    "  influence the job environment and the scheduling decisions made\n"
    "  by the cluster:\n"
    "\n"
    "Job environment:\n"
    "  -w, --workdir=DIRECTORY   set working directory      (default: current workdir)\n"
    "  -o, --stdout=FILE         set file to capture stdout (default: '/dev/null')\n"
    "  -e, --stderr=FILE         set file to capture stderr (default: <stdout>)\n"
    "  -u, --umask=MASK          set mode to use as umask   (default: current umask)\n"
    "  -p, --priority=PRIORITY   set priority               (default: 127)\n"
    "\n"
    "Job resource information:\n"
    "  Scheduling is done based on the resources a job needs and\n"
    "  on the priority given to the job.\n"
    "\n"
    "  -j, --threads=NUMBER     set number of threads       (default: 1)\n"
    "  -m, --memory=SIZE        set amount of memory in MiB (default: 2048)\n"
    "  -t, --runtime=MINUTES    set runtime in minutes      (default: 15)\n"
    "\n"
    "Job handling:\n"
    "  Define what to do if something bad happens:\n"
    "\n"
    "  -r | --restart[=MODE]  restart job on system failure (default: 'never')\n"
    "\n"
    "  available restart [MODE]s:\n"
    "      'never'     do not restart\n"
    "      'samehost'  only restart if running on the same host.\n"
    "      'always'    always restart or requeue. (default)\n"
    "\n"
    "Job grouping:\n"
    "  Grouping is done by default based on the jobs resource\n"
    "  and priority information, so that jobs using the same\n"
    "  amount of resources and having the same priority\n"
    "  are grouped and executed in parallel.\n"
    "\n"
    "  -a, --command-alias=NAME       set command alias  (default: <command>)\n"
    "  -N, --group-name=NAME          set group name     (default: 'default')\n"
    "  -P, --group-priority=PRIORITY  set group priority (default: 127)\n"
    "\n"
    "Other options:\n"
    "\n"
    "  -v, --verbose    be more verbose\n"
    "      --debug      set debug log level (default: warning log level)\n"
    "  -V, --version    print version and exit\n"
    "  -h, --help       print this help and exit ;)\n"
    "\n"
    "Change how to connect to the mysql server:\n"
    "\n"
    "  -M | --mysql-default-file[=MYSQLCNF]     (default: %s)\n"
    "  -S | --mysql-default-group[=MYSQLGROUP]  (default: %s)\n"
    "\n"
    "Environment:\n"
    "  MXQ_MYSQL_DEFAULT_FILE   change default for [MYSQLCNF]\n"
    "  MXQ_MYSQL_DEFAULT_GROUP  change default for [MYSQLGROUP]\n"
    "\n",
        program_invocation_short_name,
        MXQ_MYSQL_DEFAULT_FILE_STR,
        MXQ_MYSQL_DEFAULT_GROUP_STR
    );
}

static int load_group_id(struct mx_mysql *mysql, struct mxq_group *g)
{
    struct mx_mysql_stmt *stmt = NULL;
    unsigned long long num_rows = 0;
    int res;

    assert(mysql);
    assert(g);
    assert(g->group_id == 0);

    assert(g->group_name); assert(*g->group_name);
    assert(g->group_priority);
    assert(g->user_uid); assert(g->user_name); assert(*g->user_name);
    assert(g->user_gid); assert(g->user_group); assert(*g->user_group);
    assert(g->job_command); assert(*g->job_command);
    assert(g->job_threads); assert(g->job_memory); assert(g->job_time);

    stmt = mx_mysql_statement_prepare(mysql,
            "SELECT"
                " group_id"
            " FROM mxq_group "
            " WHERE group_name = ?"
                " AND user_uid = ?"
                " AND user_name = ?"
                " AND user_gid = ?"
                " AND user_group = ?"
                " AND job_command = ?"
                " AND job_threads = ?"
                " AND job_memory = ?"
                " AND job_time = ?"
                " AND group_priority = ?"
                " AND group_status = 0"
            " ORDER BY group_id "
            " LIMIT 1");
    if (!stmt) {
        mx_log_err("mx_mysql_statement_prepare(): %m");
        return -errno;
    }

    res  = mx_mysql_statement_param_bind(stmt, 0, string, &(g->group_name));
    res += mx_mysql_statement_param_bind(stmt, 1, uint32, &(g->user_uid));
    res += mx_mysql_statement_param_bind(stmt, 2, string, &(g->user_name));
    res += mx_mysql_statement_param_bind(stmt, 3, uint32, &(g->user_gid));
    res += mx_mysql_statement_param_bind(stmt, 4, string, &(g->user_group));
    res += mx_mysql_statement_param_bind(stmt, 5, string, &(g->job_command));
    res += mx_mysql_statement_param_bind(stmt, 6, uint16, &(g->job_threads));
    res += mx_mysql_statement_param_bind(stmt, 7, uint64, &(g->job_memory));
    res += mx_mysql_statement_param_bind(stmt, 8, uint32, &(g->job_time));
    res += mx_mysql_statement_param_bind(stmt, 9, uint16, &(g->group_priority));
    assert(res == 0);

    res = mx_mysql_statement_execute(stmt, &num_rows);
    if (res < 0) {
        mx_log_err("mx_mysql_statement_execute(): %m");
        mx_mysql_statement_close(&stmt);
        return res;
    }
    assert(num_rows <= 1);

    if (num_rows) {
        mx_mysql_statement_result_bind(stmt, 0, uint64, &(g->group_id));

        res = mx_mysql_statement_fetch(stmt);
        if (res < 0) {
            mx_log_err("mx_mysql_statement_fetch(): %m");
            mx_mysql_statement_close(&stmt);
            return res;
        }
    }

    mx_mysql_statement_close(&stmt);

    return (int)num_rows;
}

static int add_group(struct mx_mysql *mysql, struct mxq_group *g)
{
    struct mx_mysql_stmt *stmt = NULL;
    unsigned long long num_rows = 0;
    unsigned long long insert_id = 0;
    int res;

    assert(g->group_name); assert(*g->group_name);
    assert(g->group_priority);
    assert(g->user_uid); assert(g->user_name); assert(*g->user_name);
    assert(g->user_gid); assert(g->user_group); assert(*g->user_group);
    assert(g->job_command); assert(*g->job_command);
    assert(g->job_threads); assert(g->job_memory); assert(g->job_time);

    stmt = mx_mysql_statement_prepare(mysql,
            "INSERT INTO mxq_group SET"
                " group_name = ?,"

                " user_uid = ?,"
                " user_name = ?,"
                " user_gid = ?,"
                " user_group = ?,"

                " job_command = ?,"

                " job_threads = ?,"
                " job_memory = ?,"
                " job_time = ?,"
                " group_priority = ?");
    if (!stmt) {
        mx_log_err("mx_mysql_statement_prepare(): %m");
        return -errno;
    }

    res  = mx_mysql_statement_param_bind(stmt, 0, string, &(g->group_name));
    res += mx_mysql_statement_param_bind(stmt, 1, uint32, &(g->user_uid));
    res += mx_mysql_statement_param_bind(stmt, 2, string, &(g->user_name));
    res += mx_mysql_statement_param_bind(stmt, 3, uint32, &(g->user_gid));
    res += mx_mysql_statement_param_bind(stmt, 4, string, &(g->user_group));
    res += mx_mysql_statement_param_bind(stmt, 5, string, &(g->job_command));
    res += mx_mysql_statement_param_bind(stmt, 6, uint16, &(g->job_threads));
    res += mx_mysql_statement_param_bind(stmt, 7, uint64, &(g->job_memory));
    res += mx_mysql_statement_param_bind(stmt, 8, uint32, &(g->job_time));
    res += mx_mysql_statement_param_bind(stmt, 9, uint16, &(g->group_priority));
    assert(res == 0);

    res = mx_mysql_statement_execute(stmt, &num_rows);
    if (res < 0) {
        mx_log_err("mx_mysql_statement_execute(): %m");
        mx_mysql_statement_close(&stmt);
        return res;
    }

    assert(num_rows == 1);
    mx_mysql_statement_insert_id(stmt, &insert_id);

    g->group_id = insert_id;

    mx_mysql_statement_close(&stmt);

    return (int)num_rows;
}

static int add_job(struct mx_mysql *mysql, struct mxq_job *j)
{
    struct mx_mysql_stmt *stmt = NULL;
    unsigned long long num_rows = 0;
    unsigned long long insert_id = 0;
    int res;

    assert(j);
    assert(j->job_priority); assert(j->group_id);
    assert(j->job_workdir); assert(*j->job_workdir);
    assert(j->job_argc); assert(j->job_argv); assert(*j->job_argv);
    assert(j->job_argv_str); assert(*j->job_argv_str);
    assert(j->job_stdout); assert(*j->job_stdout);
    assert(j->job_stderr); assert(*j->job_stderr);
    assert(j->job_umask);
    assert(j->host_submit); assert(*j->host_submit);

    stmt = mx_mysql_statement_prepare(mysql,
            "INSERT INTO mxq_job SET"
                " job_priority = ?,"

                " group_id = ?,"

                " job_workdir = ?,"
                " job_argc = ? ,"
                " job_argv = ?,"

                " job_stdout = ?,"
                " job_stderr = ?,"

                " job_umask = ?,"

                " host_submit = ?,"

                " job_flags = ?"
                );
    if (!stmt) {
        mx_log_err("mx_mysql_statement_prepare(): %m");
        mx_mysql_statement_close(&stmt);
        return -errno;
    }

    res  = mx_mysql_statement_param_bind(stmt, 0, uint16, &(j->job_priority));
    res += mx_mysql_statement_param_bind(stmt, 1, uint64, &(j->group_id));
    res += mx_mysql_statement_param_bind(stmt, 2, string, &(j->job_workdir));
    res += mx_mysql_statement_param_bind(stmt, 3, uint16, &(j->job_argc));
    res += mx_mysql_statement_param_bind(stmt, 4, string, &(j->job_argv_str));
    res += mx_mysql_statement_param_bind(stmt, 5, string, &(j->job_stdout));
    res += mx_mysql_statement_param_bind(stmt, 6, string, &(j->job_stderr));
    res += mx_mysql_statement_param_bind(stmt, 7, uint32, &(j->job_umask));
    res += mx_mysql_statement_param_bind(stmt, 8, string, &(j->host_submit));
    res += mx_mysql_statement_param_bind(stmt, 9, uint64, &(j->job_flags));
    assert(res ==0);

    res = mx_mysql_statement_execute(stmt, &num_rows);
    if (res < 0) {
        mx_log_err("mx_mysql_statement_execute(): %m");
        mx_mysql_statement_close(&stmt);
        return res;
    }

    assert(num_rows == 1);
    mx_mysql_statement_insert_id(stmt, &insert_id);
    assert(insert_id > 0);

    j->job_id = insert_id;

    res = mx_mysql_statement_close(&stmt);

    return (int)num_rows;
}

static int mxq_submit_task(struct mx_mysql *mysql, struct mxq_job *j, int flags)
{
    int res;
    struct mxq_group *g;

    g = j->group_ptr;

    res = load_group_id(mysql, g);
    if (res < 0)
        return res;

    if (res == 0) {
        res = add_group(mysql, g);
        if (res < 0)
            return res;

        if (res == 0) {
            mx_log_err("Failed to add new group.");
            return -(errno=EIO);
        }

        mx_log_info("The new job will be added to new group with group_id=%lu", g->group_id);

    } else {
        mx_log_info("The new job will be attached to existing group with group_id=%lu", g->group_id);
    }

    assert(g->group_id);

    j->group_id = g->group_id;

    res = add_job(mysql, j);
    if (res < 0)
        return res;

    if (res == 0) {
        mx_log_err("Failed to add job group.");
        return -(errno=EIO);
    }

    mx_log_info("The new job has been queued successfully with job_id=%lu in group with group_id=%lu", j->job_id, g->group_id);

    assert(j->job_id);

    return res;
}

int main(int argc, char *argv[])
{
    int i;
    uid_t ruid, euid, suid;
    gid_t rgid, egid, sgid;
    int res;

    u_int16_t  arg_priority;
    char      *arg_group_name;
    u_int16_t  arg_group_priority;
    char      *arg_program_name;
    u_int16_t  arg_threads;
    u_int64_t  arg_memory;
    u_int32_t  arg_time;
    char      *arg_workdir;
    char      *arg_stdout;
    char      *arg_stderr;
    mode_t     arg_umask;
    char      *arg_mysql_default_file;
    char      *arg_mysql_default_group;
    char       arg_debug;
    char       arg_jobflags;

    _mx_cleanup_free_ char *current_workdir = NULL;
    _mx_cleanup_free_ char *arg_stdout_absolute = NULL;
    _mx_cleanup_free_ char *arg_stderr_absolute = NULL;
    _mx_cleanup_free_ char *arg_args = NULL;

    int flags = 0;

    struct mxq_job job;
    struct mxq_group group;

    struct mx_mysql *mysql = NULL;

    struct passwd *passwd;
    struct group  *grp;
    char *p;

    int opt;
    struct mx_getopt_ctl optctl;
    struct mx_option opts[] = {
                MX_OPTION_NO_ARG("help",                 'h'),
                MX_OPTION_NO_ARG("version",              'V'),

                MX_OPTION_REQUIRED_ARG("group_id",       1),
                MX_OPTION_REQUIRED_ARG("group_priority", 2),
                MX_OPTION_REQUIRED_ARG("group-id",       3),
                MX_OPTION_REQUIRED_ARG("time",           4),

                MX_OPTION_NO_ARG("debug",                5),
                MX_OPTION_NO_ARG("verbose",              'v'),

                MX_OPTION_OPTIONAL_ARG("restartable",    'r'),

                MX_OPTION_REQUIRED_ARG("group-name",     'N'),
                MX_OPTION_REQUIRED_ARG("group-priority", 'P'),

                MX_OPTION_REQUIRED_ARG("command-alias", 'a'),

                MX_OPTION_REQUIRED_ARG("workdir",      'w'),
                MX_OPTION_REQUIRED_ARG("stdout",       'o'),
                MX_OPTION_REQUIRED_ARG("stderr",       'e'),
                MX_OPTION_REQUIRED_ARG("umask",        'u'),
                MX_OPTION_REQUIRED_ARG("priority",     'p'),

                MX_OPTION_REQUIRED_ARG("threads",      'j'),
                MX_OPTION_REQUIRED_ARG("memory",       'm'),
                MX_OPTION_REQUIRED_ARG("runtime",      't'),

                MX_OPTION_REQUIRED_ARG("define",       'D'),

                MX_OPTION_OPTIONAL_ARG("mysql-default-file",  'M'),
                MX_OPTION_OPTIONAL_ARG("mysql-default-group", 'S'),
                MX_OPTION_END
    };


    /******************************************************************/

    current_workdir  = get_current_dir_name();

    /******************************************************************/

    arg_priority       = 127;
    arg_group_name     = "default";
    arg_group_priority = 127;
    arg_program_name   = NULL;
    arg_threads        = 1;
    arg_memory         = 2048;
    arg_time           = 0;
    arg_workdir        = current_workdir;
    arg_stdout         = "/dev/null";
    arg_stderr         = "stdout";
    arg_umask          = getumask();
    arg_debug          = 0;
    arg_jobflags       = 0;

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = MXQ_MYSQL_DEFAULT_GROUP;

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    /******************************************************************/

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

            case 'r':
                if (!optctl.optarg || mx_streq(optctl.optarg, "always")) {
                    arg_jobflags |= MXQ_JOB_FLAGS_RESTART_ON_HOSTFAIL;
                    arg_jobflags |= MXQ_JOB_FLAGS_REQUEUE_ON_HOSTFAIL;
                } else if (mx_streq(optctl.optarg, "samehost")) {
                    arg_jobflags |= MXQ_JOB_FLAGS_RESTART_ON_HOSTFAIL;
                } else if (mx_streq(optctl.optarg, "never")) {
                    arg_jobflags &= ~(MXQ_JOB_FLAGS_RESTART_ON_HOSTFAIL|MXQ_JOB_FLAGS_REQUEUE_ON_HOSTFAIL);
                } else {
                    mx_log_crit("--restart '%s': restartmode unknown.", optctl.optarg);
                    exit(EX_CONFIG);
                }
                break;

            case 'p':
                if (mx_strtou16(optctl.optarg, &arg_priority) < 0) {
                    mx_log_crit("--priority '%s': %m", optctl.optarg);
                    exit(EX_CONFIG);
                }
                break;

            case 1:
            case 3:
                if (opt == 3)
                    mx_log_warning("option --group-id is deprecated (usage will change in next version). Using --group-name instead.");
                else
                    mx_log_warning("option --group_id is deprecated. Using --group-name instead.");
            case 'N':
                if (!(*optctl.optarg)) {
                    mx_log_crit("--group-name '%s': String is empty.", optctl.optarg);
                    exit(EX_CONFIG);
                }
                arg_group_name = optctl.optarg;
                break;

            case 'a':
                p = strchr(optctl.optarg, ' ');
                if (p)
                    *p = 0;
                if (!(*optctl.optarg)) {
                    mx_log_crit("--command-alias '%s': String is empty.", optctl.optarg);
                    exit(EX_CONFIG);
                }
                arg_program_name = optctl.optarg;
                break;

            case 2:
                mx_log_warning("option --group_priority is deprecated. please use --group-priority instead.");
            case 'P':
                if (mx_strtou16(optctl.optarg, &arg_group_priority) < 0) {
                    mx_log_crit("--group-priority '%s': %m", optctl.optarg);
                    exit(EX_CONFIG);
                }
                break;

            case 'j':
                if (mx_strtou16(optctl.optarg, &arg_threads) < 0) {
                    mx_log_crit("--threads '%s': %m", optctl.optarg);
                    exit(EX_CONFIG);
                }
                break;

            case 'm':
                if (mx_strtou64(optctl.optarg, &arg_memory) < 0) {
                    mx_log_crit("--memory '%s': %m", optctl.optarg);
                    exit(EX_CONFIG);
                }
                break;

            case 4:
                mx_log_warning("option '--time' is deprecated. please use '--runtime' or '-t' in future calls.");
            case 't':
                if (mx_strtou32(optctl.optarg, &arg_time) < 0) {
                    mx_log_crit("--runtime '%s': %m", optctl.optarg);
                    exit(EX_CONFIG);
                }
                break;

            case 'w':
                if (!(*optctl.optarg)) {
                    mx_log_crit("--workdir '%s': String is empty.", optctl.optarg);
                    exit(EX_CONFIG);
                }
                if (optctl.optarg[0] != '/') {
                    mx_log_crit("--workdir '%s': workdir is a relativ path. please use absolute path.",
                                optctl.optarg);
                    exit(EX_CONFIG);
                }
                arg_workdir = optctl.optarg;
                break;

            case 'o':
                if (!(*optctl.optarg)) {
                    mx_log_crit("--stdout '%s': String is empty.", optctl.optarg);
                    exit(EX_CONFIG);
                }
                arg_stdout = optctl.optarg;
                break;

            case 'e':
                if (!(*optctl.optarg)) {
                    mx_log_crit("--stderr '%s': String is empty.", optctl.optarg);
                    exit(EX_CONFIG);
                }
                arg_stderr = optctl.optarg;
                break;

            case 'u':
                if (mx_strtou32(optctl.optarg, &arg_umask) < 0) {
                    mx_log_crit("--umask '%s': %m", optctl.optarg);
                    exit(EX_CONFIG);
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

    MX_GETOPT_FINISH(optctl, argc, argv);
    if (argc < 1) {
        print_usage();
        exit(EX_USAGE);
    }

    /* from this point values in argc,argv are the ones of the cluster job  */

    if (!arg_time) {
        arg_time = 15;
        mx_log_warning("option '--runtime' or '-t' not used. Your job will get killed if it runs longer than the default of %d minutes.", arg_time);
    }

    if (arg_time > 60*24) {
        mx_log_warning("option '--runtime' specifies a runtime longer than 24h. Your job may get killed. Be sure to implement some check pointing.");
    }

    if (!arg_program_name)
        arg_program_name = argv[0];

    if (!(*arg_program_name)) {
        mx_log_crit("<command> is empty. Please check usage with '%s --help'.", program_invocation_short_name);
        exit(EX_CONFIG);
    }

    /******************************************************************/

    if (*arg_stdout != '/') {
        res = asprintf(&arg_stdout_absolute, "%s/%s", arg_workdir, arg_stdout);
        assert(res != -1);
        arg_stdout = arg_stdout_absolute;
    }

    if (mx_streq(arg_stderr, "stdout")) {
        arg_stderr = arg_stdout;
    }

    if (*arg_stderr != '/') {
        res = asprintf(&arg_stderr_absolute, "%s/%s", arg_workdir, arg_stderr);
        assert(res != -1);
        arg_stderr = arg_stderr_absolute;
    }

    arg_args = strvec_to_str(argv);
    assert(arg_args);

    /******************************************************************/

    memset(&job,   0, sizeof(job));
    memset(&group, 0, sizeof(group));

    /* connect job and group */
    job.group_ptr = &group;

    /******************************************************************/

    group.group_name     = arg_group_name;
    group.group_priority = arg_group_priority;
    group.job_threads    = arg_threads;
    group.job_memory     = arg_memory;
    group.job_time       = arg_time;

    job.job_flags      = arg_jobflags;
    job.job_priority   = arg_priority;
    job.job_workdir    = arg_workdir;
    job.job_stdout     = arg_stdout;
    job.job_stderr     = arg_stderr;
    job.job_umask      = arg_umask;

    job.job_argc     = argc;
    job.job_argv     = argv;
    job.job_argv_str = arg_args;

    /******************************************************************/

    res = getresuid(&ruid, &euid, &suid);
    assert(res != -1);

    passwd = getpwuid(ruid);
    assert(passwd != NULL);

    res = getresgid(&rgid, &egid, &sgid);
    assert(res != -1);

    grp = getgrgid(rgid);
    assert(grp != NULL);

    group.user_uid   = ruid;
    group.user_name  = passwd->pw_name;
    group.user_gid   = rgid;
    group.user_group = grp->gr_name;

    /******************************************************************/

    group.job_command = arg_program_name;
    job.host_submit = mxq_hostname();

    /******************************************************************/

    res = mx_mysql_initialize(&mysql);
    assert(res == 0);

    mx_mysql_option_set_default_file(mysql, arg_mysql_default_file);
    mx_mysql_option_set_default_group(mysql, arg_mysql_default_group);

    res = mx_mysql_connect_forever(&mysql);
    assert(res == 0);

    mx_log_info("MySQL: Connection to database established.");

    res = mxq_submit_task(mysql, &job, flags);

    mx_mysql_finish(&mysql);

    mx_log_info("MySQL: Connection to database closed.");

    if (res < 0) {
        mx_log_err("Job submission failed: %m");
        return 1;
    }

    printf("mxq_group_id=%" PRIu64 " \n",   group.group_id);
    printf("mxq_group_name=%s\n", group.group_name);
    printf("mxq_job_id=%" PRIu64 "\n",    job.job_id);

    return 0;
}
