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

#include "mxq_mysql.h"
#include "mxq_util.h"
#include "bee_getopt.h"


#define MXQ_TASK_JOB_FORCE_APPEND  (1<<0)
#define MXQ_TASK_JOB_FORCE_NEW     (1<<1)

#define MXQ_JOB_STATUS_ACTIVE      (1)

#ifndef VERSION
#define VERSION "0.00"
#endif

#ifndef VERSIONFULL
#define VERSIONFULL "MXQ v0.00 super alpha 0"
#endif

#ifndef VERSIONDATE
#define VERSIONDATE "2014"
#endif


static void print_usage(void)
{
    printf(
    VERSIONFULL "\n"
    "  by Marius Tolzmann <tolzmann@molgen.mpg.de> " VERSIONDATE "\n"
    "  Max Planck Institute for Molecular Genetics - Berlin Dahlem\n"
    "\n"
    "Usage:\n"
    "  mxqsub [options] <command> [arguments]\n"
    "\n"
    "Synopsis:\n"
    "  queue a job to be executed on a cluster node.\n"
    "  <command> [arguments] will be executed on a node that offers\n"
    "  enough ressources to run the job. the following [options] can\n"
    "  influence the job environment and the scheduling decisions made\n"
    "  by the cluster:\n"
    "\n"
    "Job environment:\n"
    "  -w | --workdir  <directory> set working directory (default: current workdir)\n"
    "  -o | --stdout   <file>      set file to capture stdout (default: '/dev/null')\n"
    "  -e | --stderr   <file>      set file to capture stderr (default: <stdout>)\n"
    "  -u | --umask    <mode>      set mode to use as umask (default: current umask)\n"
    "  -p | --priority <priority>  set priority (default: 127)\n"
    "\n"
    "Job ressource information:\n"
    "  Scheduling is done based on the ressources a job needs and\n"
    "  on the priority given to the job.\n"
    "\n"
    "  -j | --threads  <number>  set number of threads (default: 1)\n"
    "  -m | --memory   <size>    set amount of memory (default: 2048MiB)\n"
    "  -t | --time     <minutes> set runtime (default: 15 minutes)\n"
    "\n"
    "Job grouping:\n"
    "  Grouping is done by default based on the jobs ressource\n"
    "  and priority information, so that jobs using the same\n"
    "  amount of ressources and having the same priority\n"
    "  are grouped and executed in parallel.\n"
    "\n"
    "  -N | --group-name <name>          set group name (default: 'default')\n"
    "  -P | --group-priority <priority>  set group priority (default: 127)\n"
    "\n"
    );
}

void mxq_mysql_row_to_group(struct mxq_job *j, MYSQL_ROW row)
{
    int r;
    struct mxq_group *g;

    g = j->group_ptr;

    r = 0;

    safe_convert_string_to_ui64(row[r++], &g->group_id);
    safe_convert_string_to_ui8(row[r++],  &g->group_status);
    safe_convert_string_to_ui16(row[r++], &g->group_priority);

    safe_convert_string_to_ui64(row[r++], &g->group_jobs);
    safe_convert_string_to_ui64(row[r++], &g->group_jobs_running);
    safe_convert_string_to_ui64(row[r++], &g->group_jobs_finished);
    safe_convert_string_to_ui64(row[r++], &g->group_jobs_failed);

    safe_convert_string_to_ui64(row[r++], &g->group_slots_running);
    r++; /* mtime */
    safe_convert_string_to_ui32(row[r++], &g->stats_max_maxrss);
    r++; /* utime */
    r++; /* stime */
    r++; /* real */

    j->group_id = g->group_id;
}


static int mxq_mysql_load_group(MYSQL *mysql, struct mxq_job *j)
{
    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_rows;
    unsigned int num_fields;

    _cleanup_free_ char *q_group_name  = NULL;
    _cleanup_free_ char *q_user_name   = NULL;
    _cleanup_free_ char *q_user_group  = NULL;
    _cleanup_free_ char *q_job_command = NULL;

    struct mxq_group *g;

    assert(j);
    assert(j->group_ptr);

    g = j->group_ptr;

    assert(g->group_id == 0);

    assert(g->group_name); assert(*g->group_name);
    assert(g->group_priority);
    assert(g->user_uid); assert(g->user_name); assert(*g->user_name);
    assert(g->user_gid); assert(g->user_group); assert(*g->user_group);
    assert(g->job_command); assert(*g->job_command);
    assert(g->job_threads); assert(g->job_memory); assert(g->job_time);

    q_group_name  = mxq_mysql_escape_string(mysql, g->group_name);
    q_user_name   = mxq_mysql_escape_string(mysql, g->user_name);
    q_user_group  = mxq_mysql_escape_string(mysql, g->user_group);
    q_job_command = mxq_mysql_escape_string(mysql, g->job_command);

    if (!q_group_name || !q_user_name || !q_user_group || !q_job_command)
        return 0;
    mres = mxq_mysql_query_with_result(mysql, "SELECT "
        "group_id,"
        "group_status,"
        "group_priority,"

        "group_jobs,"
        "group_jobs_running,"
        "group_jobs_finished,"
        "group_jobs_failed,"
        "group_slots_running,"

        "group_mtime,"

        "stats_max_maxrss,"
        "stats_max_utime_sec,"
        "stats_max_stime_sec,"
        "stats_max_real_sec "

        "FROM mxq_group "
        "WHERE group_name = '%s' "
        "AND user_uid = %" PRIu32 " "
        "AND user_name = '%s' "
        "AND user_gid = %" PRIu32 " "
        "AND user_group = '%s' "
        "AND job_command = '%s' "
        "AND job_threads = %" PRIu16 " "
        "AND job_memory = %" PRIu64 " "
        "AND job_time = %" PRIu32 " "
        "AND group_status = 0 "
        "ORDER BY group_id "
        "LIMIT 1",
        q_group_name, g->user_uid, q_user_name, g->user_gid, q_user_group,
        q_job_command, g->job_threads, g->job_memory, g->job_time);

    if (!mres) {
        log_msg(0, "mxq_mysql_select_next_job: Failed to query database: Error: %s\n", mysql_error(mysql));
        sleep(10);
        return 0;
    }

    num_rows = mysql_num_rows(mres);
    assert(num_rows <= 1);

    if (num_rows == 1) {
        num_fields = mysql_num_fields(mres);
        assert(num_fields == 13);

        row = mysql_fetch_row(mres);
        if (!row) {
            fprintf(stderr, "mxq_mysql_select_next_job: Failed to fetch row: Error: %s\n", mysql_error(mysql));
            mysql_free_result(mres);
            return 0;
        }
        mxq_mysql_row_to_group(j, row);
    }

    mysql_free_result(mres);

    return num_rows;
}


static int mxq_mysql_add_group(MYSQL *mysql, struct mxq_job *j)
{
    _cleanup_free_ char *q_group_name  = NULL;
    _cleanup_free_ char *q_user        = NULL;
    _cleanup_free_ char *q_group       = NULL;
    _cleanup_free_ char *q_command     = NULL;

    int   len;
    int   res;
    int   i;

    struct mxq_group *g;

    assert(j);
    assert(j->group_ptr);

    g = j->group_ptr;

    assert(g->group_name); assert(*g->group_name);
    assert(g->group_priority);
    assert(g->user_uid); assert(g->user_name); assert(*g->user_name);
    assert(g->user_gid); assert(g->user_group); assert(*g->user_group);
    assert(g->job_command); assert(*g->job_command);
    assert(g->job_threads); assert(g->job_memory); assert(g->job_time);

    q_group_name  = mxq_mysql_escape_string(mysql, g->group_name);
    q_user        = mxq_mysql_escape_string(mysql, g->user_name);
    q_group       = mxq_mysql_escape_string(mysql, g->user_group);
    q_command     = mxq_mysql_escape_string(mysql, g->job_command);

    if (!q_group_name || !q_user || !q_group || !q_command)
        return 0;

    res = mxq_mysql_query(mysql,
            "INSERT INTO mxq_group SET "
                "group_name = '%s',"
                "group_priority = %" PRIu16 ","

                "user_uid = %" PRIu32 ","
                "user_name = '%s',"
                "user_gid = %" PRIu32 ","
                "user_group = '%s',"

                "job_command = '%s',"

                "job_threads = %" PRIu16 ","
                "job_memory = %" PRIu64 " ,"
                "job_time = %" PRIu32 " ",
                q_group_name, g->group_priority,
                g->user_uid, q_user, g->user_gid, q_group,
                q_command,
                g->job_threads, g->job_memory, g->job_time);
    if (res) {
        fprintf(stderr, "Failed to query database: Error: %s\n", mysql_error(mysql));
        return 0;
    }

    g->group_id = mysql_insert_id(mysql);
    j->group_id = g->group_id;

    return 1;
}

static int mxq_mysql_add_job(MYSQL *mysql, struct mxq_job *j)
{
    _cleanup_free_ char *q_workdir     = NULL;
    _cleanup_free_ char *q_argv        = NULL;
    _cleanup_free_ char *q_stdout      = NULL;
    _cleanup_free_ char *q_stderr      = NULL;
    _cleanup_free_ char *q_submit_host = NULL;

    int   len;
    int   res;
    int   i;

    assert(j);
    assert(j->job_priority); assert(j->group_id);
    assert(j->job_workdir); assert(*j->job_workdir);
    assert(j->job_argc); assert(j->job_argv); assert(*j->job_argv);
    assert(j->job_stdout); assert(*j->job_stdout);
    assert(j->job_stderr); assert(*j->job_stderr);
    assert(j->job_umask);
    assert(j->host_submit); assert(*j->host_submit);

    q_workdir     = mxq_mysql_escape_str(mysql, j->job_workdir);
    q_argv        = mxq_mysql_escape_strvec(mysql, j->job_argv);
    q_stdout      = mxq_mysql_escape_str(mysql, j->job_stdout);
    q_stderr      = mxq_mysql_escape_str(mysql, j->job_stderr);
    q_submit_host = mxq_mysql_escape_str(mysql, j->host_submit);

    if (!q_workdir || !q_argv
        || !q_stdout || !q_stderr || !q_submit_host)
        return 0;

    res = mxq_mysql_query(mysql,
            "INSERT INTO mxq_job SET "
                "job_priority = %" PRIu16 ","

                "group_id = '%" PRIu64 "',"

                "job_workdir = '%s',"
                "job_argc = %" PRIu16 " ,"
                "job_argv = '%s',"

                "job_stdout = '%s',"
                "job_stderr = '%s',"

                "job_umask = %" PRIu32 ","

                "host_submit = '%s'",

                j->job_priority,
                j->group_id,
                q_workdir, j->job_argc, q_argv,
                q_stdout, q_stderr,
                j->job_umask, q_submit_host);
    if (res) {
        fprintf(stderr, "Failed to query database: Error: %s\n", mysql_error(mysql));
        return 0;
    }

    j->job_id = mysql_insert_id(mysql);
    return 1;
}

static int mxq_submit_task(struct mxq_mysql *mmysql, struct mxq_job *j, int flags)
{
    MYSQL *mysql;

    mysql = mxq_mysql_connect(mmysql);


    while(!mxq_mysql_load_group(mysql, j)) {
        mxq_mysql_add_group(mysql, j);
        j->group_ptr->group_id = 0;
    }
    mxq_mysql_add_job(mysql, j);

    mxq_mysql_close(mysql);

    return 1;
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
    u_int16_t  arg_threads;
    u_int64_t  arg_memory;
    u_int32_t  arg_time;
    char      *arg_workdir;
    char      *arg_stdout;
    char      *arg_stderr;
    mode_t     arg_umask;
    char      **arg_env;
    char      *arg_mysql_default_file;
    char      *arg_mysql_default_group;

    _cleanup_free_ char *current_workdir = NULL;
    _cleanup_free_ char *arg_stdout_absolute = NULL;
    _cleanup_free_ char *arg_stderr_absolute = NULL;
    _cleanup_free_ char *arg_args = NULL;

    int flags = 0;

    struct mxq_job job;
    struct mxq_group group;
    struct mxq_mysql mmysql;

    struct passwd *passwd;
    struct group  *grp;

    int opt;
    struct bee_getopt_ctl optctl;
    struct bee_option opts[] = {
                BEE_OPTION_NO_ARG("help",                 'h'),
                BEE_OPTION_NO_ARG("version",              'V'),

                BEE_OPTION_REQUIRED_ARG("group_id",       1),
                BEE_OPTION_REQUIRED_ARG("group_priority", 2),
                BEE_OPTION_REQUIRED_ARG("group-id",       3),

                BEE_OPTION_REQUIRED_ARG("group-name",     'N'),
                BEE_OPTION_REQUIRED_ARG("group-priority", 'P'),

                BEE_OPTION_REQUIRED_ARG("workdir",      'w'),
                BEE_OPTION_REQUIRED_ARG("stdout",       'o'),
                BEE_OPTION_REQUIRED_ARG("stderr",       'e'),
                BEE_OPTION_REQUIRED_ARG("umask",        'u'),
                BEE_OPTION_REQUIRED_ARG("priority",     'p'),

                BEE_OPTION_REQUIRED_ARG("threads",      'j'),
                BEE_OPTION_REQUIRED_ARG("memory",       'm'),
                BEE_OPTION_REQUIRED_ARG("time",         't'),

                BEE_OPTION_REQUIRED_ARG("define",       'D'),

                BEE_OPTION_REQUIRED_ARG("mysql-default-file",  'M'),
                BEE_OPTION_REQUIRED_ARG("mysql-default-group", 'S'),
                BEE_OPTION_END
    };


    /******************************************************************/

    current_workdir  = get_current_dir_name();

    /******************************************************************/

    arg_priority       = 127;
    arg_group_name     = "default";
    arg_group_priority = 127;
    arg_threads        = 1;
    arg_memory         = 2048;
    arg_time           = 15;
    arg_workdir        = current_workdir;
    arg_stdout         = "/dev/null";
    arg_stderr         = "stdout";
    arg_umask          = getumask();
    arg_env            = strvec_new();

    assert(arg_env);

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = "mxq_submit";

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    /******************************************************************/

    bee_getopt_init(&optctl, argc-1, &argv[1], opts);
    optctl.flags = BEE_FLAG_STOPONUNKNOWN|BEE_FLAG_STOPONNOOPT;

    while ((opt=bee_getopt(&optctl, &i)) != BEE_GETOPT_END) {
        if (opt == BEE_GETOPT_ERROR) {
            exit(EX_USAGE);
        }

        switch (opt) {
            case 'h':
            case 'V':
                printf("help/version\n");
                printf("mxq_submit [mxq-options] <command> [command-arguments]\n");
                print_usage();
                exit(EX_USAGE);

            case 'p':
                if (!safe_convert_string_to_ui16(optctl.optarg, &arg_priority)) {
                    fprintf(stderr, "ignoring --priority '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 1:
                fprintf(stderr, "INFO: option --group_id is deprecated. please use --group-name instead.\n");
            case 'N':
                arg_group_name = optctl.optarg;
                break;

            case 2:
                fprintf(stderr, "INFO: option --group_priority is deprecated. please use --group-priority instead.\n");
            case 'P':
                if (!safe_convert_string_to_ui16(optctl.optarg, &arg_group_priority)) {
                    fprintf(stderr, "ignoring --group-priority '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 3:
                fprintf(stderr, "WARNING: option --group-id is deprecated (usage will change in next version). please use --group-name instead.\n");
                arg_group_name = optctl.optarg;
                break;

            case 'j':
                if (!safe_convert_string_to_ui16(optctl.optarg, &arg_threads)) {
                    fprintf(stderr, "ignoring --threads '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 'm':
                if (!safe_convert_string_to_ui64(optctl.optarg, &arg_memory)) {
                    fprintf(stderr, "ignoring --memory '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 't':
                if (!safe_convert_string_to_ui32(optctl.optarg, &arg_time)) {
                    fprintf(stderr, "ignoring --time '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 'w':
                if (optctl.optarg[0] == '/')
                    arg_workdir = optctl.optarg;
                else
                    fprintf(stderr, "ignoring relative workdir\n");
                break;

            case 'o':
                arg_stdout = optctl.optarg;
                break;

            case 'e':
                arg_stderr = optctl.optarg;
                break;

            case 'D': {
                char *arg_env_str;
                assert(strvec_push_str(&arg_env, optctl.optarg));
                break;
            }
            case 'u':
                if (!safe_convert_string_to_ui32(optctl.optarg, &arg_umask)) {
                    fprintf(stderr, "ignoring --umask '%s': %s\n", optctl.optarg, strerror(errno));
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
    if (argc < 1) {
        print_usage();
        exit(EX_USAGE);
    }

    /* from this point values in argc,argv are the ones of the cluster job  */

    /******************************************************************/

    if (*arg_stdout != '/') {
        res = asprintf(&arg_stdout_absolute, "%s/%s", arg_workdir, arg_stdout);
        assert(res != -1);
        arg_stdout = arg_stdout_absolute;
    }

    if (streq(arg_stderr, "stdout")) {
        arg_stderr = arg_stdout;
    }

    if (*arg_stderr != '/') {
        res = asprintf(&arg_stderr_absolute, "%s/%s", arg_workdir, arg_stderr);
        assert(res != -1);
        arg_stderr = arg_stderr_absolute;
    }

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

    job.job_priority   = arg_priority;
    job.job_workdir    = arg_workdir;
    job.job_stdout     = arg_stdout;
    job.job_stderr     = arg_stderr;
    job.job_umask      = arg_umask;

    job.job_argc = argc;
    job.job_argv = argv;

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

    group.job_command = argv[0];
    job.host_submit = mxq_hostname();

    /******************************************************************/

    mmysql.default_file  = arg_mysql_default_file;
    mmysql.default_group = arg_mysql_default_group;

    /******************************************************************/

    mxq_submit_task(&mmysql, &job, flags);

    /******************************************************************/

    printf("mxq_group_id=%" PRIu64 " \n",   group.group_id);
    printf("mxq_group_name=%s\n", group.group_name);
    printf("mxq_job_id=%" PRIu64 "\n",    job.job_id);

    return 0;
}

