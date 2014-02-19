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
    "  mxq_submit [options] <command> [arguments]\n"
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
    "  -N | --group_id <name>            set group id (default: 'default')\n"
    "  -P | --group_priority <priority>  set group priority (default: 127)\n"
    "\n"
    );
}

static int mxq_mysql_add_job(MYSQL *mysql, struct mxq_job_full *j)
{
    _cleanup_free_ char *q_group_id    = NULL;
    _cleanup_free_ char *q_user        = NULL;
    _cleanup_free_ char *q_group       = NULL;
    _cleanup_free_ char *q_workdir     = NULL;
    _cleanup_free_ char *q_command     = NULL;
    _cleanup_free_ char *q_argv        = NULL;
    _cleanup_free_ char *q_stdout      = NULL;
    _cleanup_free_ char *q_stderr      = NULL;
    _cleanup_free_ char *q_submit_host = NULL;

    int   len;
    int   res;
    int   i;

    assert(j);
    assert(j->job_priority);
    assert(j->group_id);
    assert(*j->group_id);
    assert(j->group_priority);
    assert(j->user_uid);
    assert(j->user_name);
    assert(*j->user_name);
    assert(j->user_gid);
    assert(j->user_group);
    assert(*j->user_group);
    assert(j->job_threads);
    assert(j->job_memory);
    assert(j->job_time);
    assert(j->job_workdir);
    assert(*j->job_workdir);
    assert(j->job_command);
    assert(*j->job_command);
    assert(j->job_argc);
    assert(j->job_argv);
    assert(*j->job_argv);
    assert(j->job_stdout);
    assert(*j->job_stdout);
    assert(j->job_stderr);
    assert(*j->job_stderr);
    assert(j->job_umask);
    assert(j->host_submit);
    assert(*j->host_submit);

    if (!(q_group_id    = mxq_mysql_escape_string(mysql, j->group_id)   )) return 0;
    if (!(q_user        = mxq_mysql_escape_string(mysql, j->user_name)  )) return 0;
    if (!(q_group       = mxq_mysql_escape_string(mysql, j->user_group) )) return 0;
    if (!(q_workdir     = mxq_mysql_escape_string(mysql, j->job_workdir))) return 0;
    if (!(q_command     = mxq_mysql_escape_string(mysql, j->job_command))) return 0;
    if (!(q_argv        = mxq_mysql_escape_string(mysql, j->job_argv)   )) return 0;
    if (!(q_stdout      = mxq_mysql_escape_string(mysql, j->job_stdout) )) return 0;
    if (!(q_stderr      = mxq_mysql_escape_string(mysql, j->job_stderr) )) return 0;
    if (!(q_submit_host = mxq_mysql_escape_string(mysql, j->host_submit))) return 0;

    res = mxq_mysql_query(mysql, "INSERT INTO job SET "
                "job_priority = %d, "
                "group_id = '%s', "
                "group_priority = %d, "
                "user_uid = %d, "
                "user_name = '%s', "
                "user_gid = %d, "
                "user_group = '%s', "
                "job_threads = %d, "
                "job_memory = %d, "
                "job_time = %d, "
                "job_workdir = '%s', "
                "job_command = '%s', "
                "job_argc = %d, "
                "job_argv = '%s', "
                "job_stdout = '%s', "
                "job_stderr = '%s', "
                "job_umask = %d, "
                "host_submit = '%s'",
                j->job_priority, q_group_id, j->group_priority,
                j->user_uid, q_user, j->user_gid, q_group,
                j->job_threads, j->job_memory, j->job_time,
                q_workdir, q_command, j->job_argc, q_argv,
                q_stdout, q_stderr, j->job_umask, q_submit_host);
    if (res) {
        fprintf(stderr, "Failed to query database: Error: %s\n", mysql_error(mysql));
        return 0;
    }

    j->job_id = mysql_insert_id(mysql);
    return 1;
}



static int mxq_submit_task(struct mxq_mysql *mmysql, struct mxq_job_full *job, int flags)
{
    MYSQL *mysql;

    mysql = mxq_mysql_connect(mmysql);

    mxq_mysql_add_job(mysql, job);

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
    char      *arg_group_id;
    u_int16_t  arg_group_priority;
    u_int16_t  arg_threads;
    u_int64_t  arg_memory;
    u_int32_t  arg_time;
    char      *arg_workdir;
    char      *arg_stdout;
    char      *arg_stderr;
    mode_t     arg_umask;
    char      *arg_mysql_default_file;
    char      *arg_mysql_default_group;

    _cleanup_free_ char *current_workdir;
    _cleanup_free_ char *arg_stdout_absolute = NULL;
    _cleanup_free_ char *arg_stderr_absolute = NULL;
    _cleanup_free_ char *arg_args = NULL;

    int flags = 0;

    struct mxq_job_full job;
    struct mxq_mysql mmysql;

    int opt;
    struct bee_getopt_ctl optctl;
    struct bee_option opts[] = {
                BEE_OPTION_NO_ARG("help",                 'h'),
                BEE_OPTION_NO_ARG("version",              'V'),

                BEE_OPTION_REQUIRED_ARG("group_id",       'N'),
                BEE_OPTION_REQUIRED_ARG("group_priority", 'P'),

                BEE_OPTION_REQUIRED_ARG("workdir",      'w'),
                BEE_OPTION_REQUIRED_ARG("stdout",       'o'),
                BEE_OPTION_REQUIRED_ARG("stderr",       'e'),
                BEE_OPTION_REQUIRED_ARG("umask",        'u'),
                BEE_OPTION_REQUIRED_ARG("priority",     'p'),

                BEE_OPTION_REQUIRED_ARG("threads",      'j'),
                BEE_OPTION_REQUIRED_ARG("memory",       'm'),
                BEE_OPTION_REQUIRED_ARG("time",         't'),

                BEE_OPTION_REQUIRED_ARG("mysql-default-file",  'M'),
                BEE_OPTION_REQUIRED_ARG("mysql-default-group", 'S'),
                BEE_OPTION_END
    };

    struct passwd *passwd;
    struct group  *group;

    /******************************************************************/

    current_workdir  = get_current_dir_name();

    /******************************************************************/

    arg_priority       = 127;
    arg_group_id       = "default";
    arg_group_priority = 127;
    arg_threads        = 1;
    arg_memory         = 2048;
    arg_time           = 15;
    arg_workdir        = current_workdir;
    arg_stdout         = "/dev/null";
    arg_stderr         = "stdout";
    arg_umask          = getumask();

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
                    fprintf(stderr, "ignoring priority '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 'N':
                arg_group_id = optctl.optarg;
                break;

            case 'P':
                if (!safe_convert_string_to_ui16(optctl.optarg, &arg_group_priority)) {
                    fprintf(stderr, "ignoring group_priority '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 'j':
                if (!safe_convert_string_to_ui16(optctl.optarg, &arg_threads)) {
                    fprintf(stderr, "ignoring threads '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 'm':
                if (!safe_convert_string_to_ui64(optctl.optarg, &arg_memory)) {
                    fprintf(stderr, "ignoring time '%s': %s\n", optctl.optarg, strerror(errno));
                }
                break;

            case 't':
                if (!safe_convert_string_to_ui32(optctl.optarg, &arg_time)) {
                    fprintf(stderr, "ignoring time '%s': %s\n", optctl.optarg, strerror(errno));
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

            case 'u':
                if (!safe_convert_string_to_ui32(optctl.optarg, &arg_umask)) {
                    fprintf(stderr, "ignoring umask '%s': %s\n", optctl.optarg, strerror(errno));
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

    memset(&job, 0, sizeof(job));

    /******************************************************************/

    job.job_priority = arg_priority;
    strncpy(job.group_id, arg_group_id, sizeof(job.group_id)-1);
    job.group_priority = arg_group_priority;
    job.job_threads = arg_threads;
    job.job_memory  = arg_memory;
    job.job_time    = arg_time;
    strncpy(job.job_workdir, arg_workdir, sizeof(job.job_workdir)-1);
    strncpy(job.job_stdout,  arg_stdout,  sizeof(job.job_stdout)-1);
    strncpy(job.job_stderr,  arg_stderr,  sizeof(job.job_stderr)-1);
    job.job_umask = arg_umask;

    /******************************************************************/

    res = getresuid(&ruid, &euid, &suid);
    assert(res != -1);

    passwd = getpwuid(ruid);
    assert(passwd != NULL);

    res = getresgid(&rgid, &egid, &sgid);
    assert(res != -1);

    group = getgrgid(rgid);
    assert(group != NULL);

    job.user_uid = ruid;
    strncpy(job.user_name, passwd->pw_name, sizeof(job.user_name)-1);
    job.user_gid = rgid;
    strncpy(job.user_group, group->gr_name, sizeof(job.user_group)-1);

    /******************************************************************/

    strncpy(job.job_command, argv[0], sizeof(job.job_command)-1);
    job.job_argc = argc;

    arg_args = stringvectostring(argc, argv);
    assert(arg_args);
    strncpy(job.job_argv, arg_args, sizeof(job.job_argv)-1);

    strncpy(job.host_submit, mxq_hostname(), sizeof(job.host_submit)-1);

    /******************************************************************/

    mmysql.default_file  = arg_mysql_default_file;
    mmysql.default_group = arg_mysql_default_group;

    /******************************************************************/

    mxq_submit_task(&mmysql, &job, flags);

    /******************************************************************/

    printf("mxq_group_id=%s\n", job.group_id);
    printf("mxq_job_id=%d\n", job.job_id);

    return 0;
}

