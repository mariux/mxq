
#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>

#include <sysexits.h>

#include <time.h>
#include <unistd.h>

#include <pwd.h>

#include <mysql.h>

#include "mx_getopt.h"

#include "mxq.h"

#include "mxq_util.h"
#include "mx_mysql.h"

#include "mxq_group.h"
#include "mxq_job.h"

#define UINT64_UNSET       (uint64_t)(-1)
#define UINT64_ALL         (uint64_t)(-2)
#define UINT64_SPECIAL_MIN (uint64_t)(-2)
#define UINT64_HASVALUE(x) ((x) < UINT64_SPECIAL_MIN)


static void print_usage(void)
{
    mxq_print_generic_version();
    printf(
    "\n"
    "Usage:\n"
    "  %s [options] [modes]\n"
    "\n"
    "  %s [options] [--groups] [groups-options]\n"
    "  %s [options] --group-id <group-id>\n"
    "  %s [options] --jobs [jobs-options]\n"
    "  %s [options] --job-id <job-id>\n"
    "\n\n"
    "Synopsis:\n"
    "  Dump status information of MXQ cluster.\n"
    "\n\n"
    "available [modes]:\n"
    "       --groups [groups-options]   dump (active) groups (default mode)\n"
    "  -g | --group-id <group-id>       dump group with <group-id>\n"
    "  -j | --jobs [job-options]        dump jobs\n"
    "  -J | --job-id <job-id>           dump job with <job-id>\n"
    "\n\n"
    "[groups-options]:\n"
    "  -r | --running                filter groups with running jobs (default: active groups)\n"
    "  -u | --user [username|uid]    filter user/everybodys groups   (default: own groups)\n"
    "  -a | --all                    no filter - dump all groups     (default: active groups)\n"
    "\n\n"
    "[jobs-options]:\n"
    "  -u | --user [usrname|uid]       filter user/everybodys jobs (default: own uid)\n"
    "  -g | --group-id <group-id>      filter jobs in group with <group-id>\n"
    "     -s | --status <job-status>   filter jobs with <job-status> (default: running)\n"
    "                                  (only available when --group-id is set)\n"
    "\n"
    "     -q | --inq        alias for '--status=inq'\n"
    "     -r | --running    alias for '--status=running'\n"
    "     -f | --finished   alias for '--status=finished'\n"
    "     -F | --failed     alias for '--status=failed'\n"
    "     -K | --killed     alias for '--status=killed'\n"
    "     -C | --cancelled  alias for '--status=cancelled'\n"
    "     -U | --unknown    alias for '--status=unknown'\n"
    "\n\n"
    "[options]:\n"
    "  -V | --version\n"
    "  -h | --help\n"
    "\n"
    "  Change how to connect to the mysql server:\n"
    "\n"
    "  -M | --mysql-default-file  [mysql-file]   (default: %s)\n"
    "  -S | --mysql-default-group [mysql-group]  (default: %s)\n"
    "\n"
    "\n"
    "Option parameters:\n"
    "\n"
    "    Parameters in [] are optional and values in <> are mandatory\n"
    "\n"
    "    [uid]        numeric user id. If not set assume 'all users'.\n"
    "    <group-id>   numeric group id\n"
    "    <job-id>     numeric job id\n"
    "    <job-status> one of 'running', 'finished', 'failed' or 'unknown'\n"
    "\n"
    "Environment:\n"
    "  MXQ_MYSQL_DEFAULT_FILE   change default for [mysql-file]\n"
    "  MXQ_MYSQL_DEFAULT_GROUP  change default for [mysql-group]\n"
    "\n",
    program_invocation_short_name,
    program_invocation_short_name,
    program_invocation_short_name,
    program_invocation_short_name,
    program_invocation_short_name,
    MXQ_MYSQL_DEFAULT_FILE_STR,
    MXQ_MYSQL_DEFAULT_GROUP_STR
    );
}

static int print_group(struct mxq_group *g)
{
    return printf("user=%s uid=%u group_id=%lu pri=%d jobs_total=%lu run_jobs=%lu run_slots=%lu failed=%lu"
                    " finished=%lu cancelled=%lu unknown=%lu inq=%lu"
                    " job_threads=%u job_memory=%luMiB job_time=%umin"
                    " memory_load=%lu%% time_load=%lu%%"
                    " max_utime=%lumin max_real=%lumin max_memory=%uMiB job_command=%s group_name=%s\n",
        g->user_name, g->user_uid, g->group_id, g->group_priority, g->group_jobs,
        g->group_jobs_running, g->group_slots_running, g->group_jobs_failed,
        g->group_jobs_finished, g->group_jobs_cancelled, g->group_jobs_unknown,
        g->group_jobs_inq,
        g->job_threads, g->job_memory, g->job_time,
        (100UL*(uint64_t)g->stats_max_maxrss/1024UL/g->job_memory),
        (100UL*(uint64_t)g->stats_max_real.tv_sec/60UL/g->job_time),
        g->stats_max_utime.tv_sec/60, g->stats_max_real.tv_sec/60,
        g->stats_max_maxrss/1024, g->job_command, g->group_name);
}

static char *restart_to_string(uint64_t flags)
{
    if (flags & MXQ_JOB_FLAGS_REQUEUE_ON_HOSTFAIL)
        return "always";

    if (flags & MXQ_JOB_FLAGS_RESTART_ON_HOSTFAIL)
        return "samehost";

    return "never";
}

static int print_job(struct mxq_group *g, struct mxq_job *j)
{
    time_t now;
    uint64_t run_sec = 0;
    uint64_t wait_sec = 0;


    if (!j->date_start || !j->date_end) {
        time(&now);
        if (now == ((time_t)-1)) {
            mx_log_err("time() failed: %m");
            now = 0;
        }
    }

    if (j->date_start) {
        if (j->date_end)
            run_sec = (j->date_end - j->date_start);
        else if (now)
            run_sec = (now - j->date_start);

        wait_sec = (j->date_start - j->date_submit);
    } else {
        if (now)
            wait_sec = (now - j->date_submit);
    }

    if (!j->date_end) {
        if (j->date_start)
            run_sec = (now - j->date_start);
    } else {
        assert(j->date_start);
        run_sec = (j->date_end - j->date_start);
    }

    return printf("job=%s(%u):%lu:%lu host_pid=%u server=%s::%s waittime=%lus runtime=%lus utime=%lus stime=%lus runtime_requested=%us time_load=%lu%% "
                    "memory_requested=%lukiB max_rss=%lukiB memory_load=%lu%% threads=%d slots=%u status=%s(%d) stats_status=%u restart=%s workdir=%s command=%s"
                    "\n",
        g->user_name, g->user_uid, g->group_id, j->job_id,
        j->host_pid,
        j->host_hostname, j->server_id,
        wait_sec, run_sec,
        j->stats_rusage.ru_utime.tv_sec,j->stats_rusage.ru_stime.tv_sec,g->job_time*60,
        (100UL*(run_sec)/60UL/g->job_time),
        g->job_memory*1024, j->stats_rusage.ru_maxrss,
        (100UL*j->stats_rusage.ru_maxrss/1024UL/g->job_memory),
        g->job_threads, j->host_slots,
        mxq_job_status_to_name(j->job_status), j->job_status, j->stats_status,
        restart_to_string(j->job_flags),
        j->job_workdir,
        j->job_argv_str);
}

static int dump_group(struct mx_mysql *mysql, uint64_t group_id)
{
    struct mxq_group *grp, *groups = NULL;

    int grp_cnt = 0;
    int total = 0;

    int g;

    assert(mysql);
    assert(UINT64_HASVALUE(group_id));

    grp_cnt = mxq_load_group(mysql, &groups, group_id);
    if (!grp_cnt)
        return 0;

    grp = &groups[0];

    print_group(grp);

    mxq_group_free_content(grp);
    mx_free_null(groups);

    return 1;
}

static int dump_groups(struct mx_mysql *mysql, uint64_t status, uint64_t user_uid)
{
    struct mxq_group *grp, *groups = NULL;

    int grp_cnt = 0;
    int total = 0;

    int g;

    assert(mysql);

    if (status == MXQ_JOB_STATUS_RUNNING) {
        if (UINT64_HASVALUE(user_uid)) {
            grp_cnt = mxq_load_running_groups_for_user(mysql, &groups, user_uid);
        } else {
            assert(user_uid == UINT64_ALL);
            grp_cnt = mxq_load_running_groups(mysql, &groups);
        }
    } else if (status == UINT64_ALL && user_uid == UINT64_ALL) {
        grp_cnt = mxq_load_all_groups(mysql, &groups);
    } else if (status == UINT64_ALL && UINT64_HASVALUE(user_uid)) {
        grp_cnt = mxq_load_all_groups_for_user(mysql, &groups, user_uid);
    } else {
        grp_cnt = mxq_load_active_groups_for_user(mysql, &groups, user_uid);
    }

    for (g = 0; g < grp_cnt; g++) {
        grp = &groups[g];

        print_group(grp);

        mxq_group_free_content(grp);
    }

    mx_free_null(groups);

    return grp_cnt;
}

static int dump_job(struct mx_mysql *mysql, uint64_t job_id)
{
    struct mxq_group *grp, *groups = NULL;
    struct mxq_job   *job, *jobs   = NULL;

    int grp_cnt = 0;
    int job_cnt = 0;
    int total = 0;

    int g, j;

    assert(mysql);
    assert(UINT64_HASVALUE(job_id));

    job_cnt = mxq_load_job(mysql, &jobs, job_id);
    if (!job_cnt) {
        return 0;
    }

    job = &jobs[0];

    grp_cnt = mxq_load_group(mysql, &groups, job->group_id);
    if (!grp_cnt) {
        mx_log_err("can'load group with group_id='%lu' for job with job_id='%lu'", job->group_id, job_id);
        mxq_job_free_content(job);
        mx_free_null(jobs);
        return 0;
    }

    grp = &groups[0];

    print_job(grp, job);

    mxq_job_free_content(job);
    mxq_group_free_content(grp);
    mx_free_null(groups);
    mx_free_null(jobs);

    return 1;
}

static int dump_jobs(struct mx_mysql *mysql, uint64_t group_id, uint64_t job_status, uint64_t user_uid)
{
    struct mxq_group *grp, *groups = NULL;
    struct mxq_job   *job, *jobs   = NULL;

    int grp_cnt = 0;
    int job_cnt = 0;
    int total = 0;

    int g, j;

    assert(mysql);
    assert(UINT64_HASVALUE(group_id)   || group_id   == UINT64_ALL);
    assert(UINT64_HASVALUE(job_status) || job_status == UINT64_ALL);
    assert(UINT64_HASVALUE(user_uid)   || user_uid   == UINT64_ALL);

    if (UINT64_HASVALUE(group_id)) {
        assert(user_uid == UINT64_ALL);
        grp_cnt = mxq_load_group(mysql, &groups, group_id);
    } else {
        assert(group_id   == UINT64_ALL);
        assert(job_status == MXQ_JOB_STATUS_RUNNING);

        if (UINT64_HASVALUE(user_uid))
            grp_cnt = mxq_load_running_groups_for_user(mysql, &groups, user_uid);
        else
            grp_cnt = mxq_load_running_groups(mysql, &groups);
    }

    mx_debug_value("%lu", grp_cnt);

    for (g=0; g < grp_cnt; g++) {
        grp = &groups[g];

        if (UINT64_HASVALUE(job_status))
            job_cnt = mxq_load_jobs_in_group_with_status(mysql, &jobs, grp, job_status);
        else
            job_cnt = mxq_load_jobs_in_group(mysql, &jobs, grp);

        mx_debug_value("%lu", job_cnt);

        for (j=0; j < job_cnt; j++) {
            job = &jobs[j];
            print_job(grp, job);
            mxq_job_free_content(job);
        }
        mxq_group_free_content(grp);
        mx_free_null(jobs);

        total += job_cnt;
    }
    mx_free_null(groups);
    return total;
}

int main(int argc, char *argv[])
{
    struct mx_mysql *mysql = NULL;
    int i;
    int res;
    char *arg_mysql_default_group;
    char *arg_mysql_default_file;
    uid_t ruid, euid, suid;
    struct passwd *passwd;

    char     arg_debug;
    char     arg_all;
    char     arg_running;
    char     arg_jobs;
    char     arg_groups;
    uint64_t arg_group_id;
    uint64_t arg_job_id;
    uint64_t arg_uid;
    uint64_t arg_status;

    uint64_t cnt;

    int opt;
    struct mx_getopt_ctl optctl;
    struct mx_option opts[] = {
                MX_OPTION_NO_ARG("help",    'h'),
                MX_OPTION_NO_ARG("version", 'V'),

                MX_OPTION_NO_ARG("debug",   5),
                MX_OPTION_NO_ARG("verbose", 'v'),

                MX_OPTION_NO_ARG("all",     'a'),
                MX_OPTION_NO_ARG("running", 'r'),

                MX_OPTION_NO_ARG("inq",      'q'),
                MX_OPTION_NO_ARG("finished", 'f'),
                MX_OPTION_NO_ARG("failed",   'F'),
                MX_OPTION_NO_ARG("killed",   'K'),
                MX_OPTION_NO_ARG("cancelled",'C'),
                MX_OPTION_NO_ARG("unknown",  'U'),

                MX_OPTION_NO_ARG("jobs",    'j'),

                MX_OPTION_OPTIONAL_ARG("groups",   'g'),
                MX_OPTION_REQUIRED_ARG("group-id", 'g'),
                MX_OPTION_REQUIRED_ARG("job-id",   'J'),

                MX_OPTION_OPTIONAL_ARG("users",    'u'),

                MX_OPTION_REQUIRED_ARG("status",   's'),

                MX_OPTION_OPTIONAL_ARG("mysql-default-file",  'M'),
                MX_OPTION_OPTIONAL_ARG("mysql-default-group", 'S'),
                MX_OPTION_END
    };

    arg_debug    = 0;
    arg_all      = 0;
    arg_running  = 0;
    arg_jobs     = 0;
    arg_groups   = 0;
    arg_status   = UINT64_UNSET;
    arg_group_id = UINT64_UNSET;
    arg_job_id   = UINT64_UNSET;
    arg_uid      = UINT64_UNSET;

    mx_log_level_set(MX_LOG_NOTICE);

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

            case 'v':
                if (!arg_debug)
                    mx_log_level_set(MX_LOG_INFO);
                break;

            case 'a':
                arg_all = 1;
                break;

            case 'j':
                arg_jobs = 1;
                break;

            case 'u':
                if (!optctl.optarg) {
                    arg_uid = UINT64_ALL;
                    break;
                }
                passwd = getpwnam(optctl.optarg);
                if (passwd) {
                    arg_uid = passwd->pw_uid;
                    break;
                }
                mx_log_debug("user %s not found. trying numeric uid.", optctl.optarg);
                if (mx_strtou64(optctl.optarg, &arg_uid) < 0 || arg_uid >= UINT64_SPECIAL_MIN) {
                    if (arg_uid >= UINT64_SPECIAL_MIN)
                        errno = ERANGE;
                    mx_log_err("Invalid argument for --group-id '%s': User not found and %m", optctl.optarg);
                    exit(EX_USAGE);
                }
                break;

            case 's':
                if (mx_streq_nocase(optctl.optarg, "inq")) {
                    arg_status = MXQ_JOB_STATUS_INQ;
                } else if (mx_streq_nocase(optctl.optarg, "running")) {
                    arg_status = MXQ_JOB_STATUS_RUNNING;
                } else if (mx_streq_nocase(optctl.optarg, "failed")) {
                    arg_status = MXQ_JOB_STATUS_FAILED;
                } else if (mx_streq_nocase(optctl.optarg, "killed")) {
                    arg_status = MXQ_JOB_STATUS_KILLED;
                } else if (mx_streq_nocase(optctl.optarg, "cancelled")) {
                    arg_status = MXQ_JOB_STATUS_CANCELLED;
                } else if (mx_streq_nocase(optctl.optarg, "unknown")) {
                    arg_status = MXQ_JOB_STATUS_UNKNOWN;
                } else if (mx_streq_nocase(optctl.optarg, "finished")) {
                    arg_status = MXQ_JOB_STATUS_FINISHED;
                } else if (mx_streq_nocase(optctl.optarg, "all")) {
                    arg_status = UINT64_ALL;
                } else if (mx_streq_nocase(optctl.optarg, "any")) {
                    arg_status = UINT64_ALL;
                } else {
                    mx_log_err("Invalid argument for --group-id '%s'", optctl.optarg);
                    exit(EX_USAGE);
                }
                break;

            case 'r':
                arg_running = 1;
                arg_status  = MXQ_JOB_STATUS_RUNNING;
                break;

            case 'q':
                arg_status  = MXQ_JOB_STATUS_INQ;
                break;

            case 'f':
                arg_status  = MXQ_JOB_STATUS_FINISHED;
                break;

            case 'F':
                arg_status  = MXQ_JOB_STATUS_FAILED;
                break;

            case 'K':
                arg_status  = MXQ_JOB_STATUS_KILLED;
                break;

            case 'C':
                arg_status  = MXQ_JOB_STATUS_CANCELLED;
                break;

            case 'U':
                arg_status  = MXQ_JOB_STATUS_UNKNOWN;
                break;

            case 'g':
                if (!optctl.optarg) {
                    arg_groups = 1;
                    break;
                }
                if (mx_strtou64(optctl.optarg, &arg_group_id) < 0 || !arg_group_id || arg_group_id >= UINT64_SPECIAL_MIN) {
                    if (!arg_group_id || arg_group_id >= UINT64_SPECIAL_MIN)
                        errno = ERANGE;
                    mx_log_err("Invalid argument for --group-id '%s': %m", optctl.optarg);
                    exit(EX_USAGE);
                }
                break;

            case 'J':
                if (mx_strtou64(optctl.optarg, &arg_job_id) < 0 || !arg_job_id || arg_job_id >= UINT64_SPECIAL_MIN) {
                    if (!arg_job_id || arg_job_id >= UINT64_SPECIAL_MIN)
                        errno = ERANGE;
                    mx_log_err("Invalid argument for --job-id '%s': %m", optctl.optarg);
                    exit(EX_USAGE);
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

    res = getresuid(&ruid, &euid, &suid);
    assert(res != -1);

    if (UINT64_HASVALUE(arg_job_id)) {
        if (UINT64_HASVALUE(arg_group_id))
            mx_log_err("options '--job-id' and '--group-id' are mutually exclusive.");
        if (arg_jobs)
            mx_log_err("options '--job-id' and '--jobs' are mutually exclusive.");
        if (arg_groups)
            mx_log_err("options '--job-id' and '--groups' are mutually exclusive.");
        if (UINT64_HASVALUE(arg_group_id) || arg_jobs || arg_groups) {
            mx_log_notice("usage: %s [options] --job-id <job-id>", program_invocation_short_name);
               mx_log_notice("use %s --help to see full usage information.", program_invocation_short_name);
            exit(EX_USAGE);
        }
    }

    if (UINT64_HASVALUE(arg_group_id) && !arg_jobs) {
        assert(!UINT64_HASVALUE(arg_job_id));
        if (arg_groups) {
            mx_log_err("options '--group-id' and '--groups' are mutually exclusive.");
            mx_log_notice("usage: %s [options] --group-id <group-id>", program_invocation_short_name);
               mx_log_notice("use %s --help to see full usage information.", program_invocation_short_name);
            exit(EX_USAGE);
        }
    }

    if (arg_jobs && arg_groups) {
        mx_log_err("options '--jobs' and '--groups' are mutually exclusive.");
        mx_log_notice("usage: %s [options] --jobs   [jobs-options]", program_invocation_short_name);
        mx_log_notice("usage: %s [options] --groups [groups-options]", program_invocation_short_name);
        mx_log_notice("use %s --help to see full usage information.", program_invocation_short_name);
        exit(EX_USAGE);
    }

    if (!UINT64_HASVALUE(arg_job_id) && !UINT64_HASVALUE(arg_group_id) && !arg_jobs) {
        arg_groups = 1; // set default mode
    }

    if (UINT64_HASVALUE(arg_job_id) || (UINT64_HASVALUE(arg_group_id) && !arg_jobs)) {
       if (arg_running) {
           mx_log_warning("ignoring option '--running'.");
           if (arg_status == MXQ_JOB_STATUS_RUNNING)
               arg_status = UINT64_UNSET;
           arg_running = 0;
       }
       if (arg_all) {
           mx_log_warning("ignoring option '--all'.");
           arg_all = 0;
       }
       if (UINT64_HASVALUE(arg_status)) {
           mx_log_warning("ignoring option '--status'.");
           arg_status = UINT64_UNSET;
       }
       if (UINT64_HASVALUE(arg_uid)) {
           mx_log_warning("ignoring option '--user=%lu'.", arg_uid);
           arg_uid = UINT64_UNSET;
       }
       if (arg_uid == UINT64_ALL) {
           mx_log_warning("ignoring option '--users'.");
           arg_uid = UINT64_UNSET;
       }
    } else {
       assert(arg_jobs || arg_groups);
       if (arg_uid == UINT64_UNSET)
           arg_uid = ruid;
    }


    if (arg_jobs && arg_running && arg_all && UINT64_HASVALUE(arg_group_id) ) {
        mx_log_info("ignoring option '--all' when '--status', '--jobs' and '--group-id' are activated.");
        arg_all = 0;
    }

    if (!arg_jobs && arg_running && arg_status == MXQ_JOB_STATUS_RUNNING) {
        arg_status = UINT64_UNSET;
    }

    if (UINT64_HASVALUE(arg_status) && !arg_jobs) {
        mx_log_warning("ignoring option '--status=%s' when '--jobs' is not set.", mxq_job_status_to_name(arg_status));
        arg_status = UINT64_UNSET;
    }

    if (UINT64_HASVALUE(arg_status) && arg_status != MXQ_JOB_STATUS_RUNNING && !UINT64_HASVALUE(arg_group_id)) {
        mx_log_warning("ignoring option '--status=%s' when '--group-id' is not set.", mxq_job_status_to_name(arg_status));
        arg_status = UINT64_UNSET;
    }

    if (0 && arg_jobs && arg_running) {
        mx_log_debug("ignoring option '--status=running' when '--jobs' is activated.");
        arg_running = 0;
    }

    // set defaults
    if (arg_jobs && arg_status == UINT64_UNSET)
        arg_status = MXQ_JOB_STATUS_RUNNING;

    res = mx_mysql_init(&mysql);
    assert(res == 0);

    mx_mysql_option_set_default_file(mysql, arg_mysql_default_file);
    mx_mysql_option_set_default_group(mysql, arg_mysql_default_group);

    res = mx_mysql_connect_forever(&mysql);
    assert(res == 0);

    mx_log_info("MySQL: Connection to database established.");

    if (UINT64_HASVALUE(arg_job_id)) {
        mx_log_debug("DO: print job with job_id=%lu", arg_job_id);
        cnt = dump_job(mysql, arg_job_id);
        if (!cnt)
           mx_log_notice("No job found with job_id=%lu.", arg_job_id);
    } else if (arg_jobs) {
        if (UINT64_HASVALUE(arg_group_id)) {
            if (arg_all) {
                mx_log_debug("DO: print all jobs in group_id=%lu", arg_group_id);
                cnt = dump_jobs(mysql, arg_group_id, UINT64_ALL, UINT64_ALL);
                if (!cnt)
                    mx_log_notice("There are no jobs in group '%lu'.", arg_group_id);

            } else {
                mx_log_debug("DO: print jobs in group_id=%lu with status='%s(%lu)'",
                    arg_group_id, mxq_job_status_to_name(arg_status), arg_status);
                cnt = dump_jobs(mysql, arg_group_id, arg_status, UINT64_ALL);
                if (!cnt)
                    mx_log_notice("There are no jobs with status '%s' in group '%lu'.", mxq_job_status_to_name(arg_status), arg_group_id);
            }
        } else {
            if (arg_all) {
                mx_log_debug("DO: print all running jobs");
                dump_jobs(mysql, UINT64_ALL, MXQ_JOB_STATUS_RUNNING, UINT64_ALL);
            } else {
                mx_log_debug("DO: print MY running jobs");
                cnt = dump_jobs(mysql, UINT64_ALL, MXQ_JOB_STATUS_RUNNING, arg_uid);
                if (!cnt) {
                    if (arg_uid == ruid)
                        mx_log_notice("You do not have any jobs running on the cluster.");
                    else
                        mx_log_notice("No running jobs for user with uid '%lu'.", arg_uid);
                }
            }
        }
    } else if (UINT64_HASVALUE(arg_group_id)) {
        mx_log_debug("DO: print group with group_id=%lu", arg_group_id);
        cnt = dump_group(mysql, arg_group_id);
        if (!cnt)
           mx_log_notice("No group found with group_id=%lu.", arg_group_id);
    } else {
        if (UINT64_HASVALUE(arg_uid) && !arg_all && !(arg_uid == ruid && !arg_running)) {
            mx_log_debug("DO: print running groups for user with uid=%d", arg_uid);
            cnt = dump_groups(mysql, MXQ_JOB_STATUS_RUNNING, arg_uid);
            if (!cnt)
                mx_log_notice("No running groups found for user with uid=%d", arg_uid);
        } else if (arg_uid == UINT64_ALL && arg_all) {
            mx_log_debug("DO: print all groups");
            cnt = dump_groups(mysql, UINT64_ALL, UINT64_ALL);
            if (!cnt)
                mx_log_notice("No groups found.");
        } else if (arg_all) {
            mx_log_debug("DO: print all groups for user with uid=%lu", arg_uid);
            cnt = dump_groups(mysql, UINT64_ALL, arg_uid);
            if (!cnt)
                mx_log_notice("No groups found for user with uid=%d.", arg_uid);
        } else {
            if (arg_uid == UINT64_ALL) {
                mx_log_debug("DO: print all running groups");
                cnt = dump_groups(mysql, MXQ_JOB_STATUS_RUNNING, UINT64_ALL);
                if (!cnt)
                    mx_log_notice("No running groups found.");
            } else {
                mx_log_debug("DO: print MY active groups (default)");
                cnt = dump_groups(mysql, UINT64_UNSET, ruid);
                if (!cnt)
                    mx_log_notice("You do not have any active groups.");
            }
        }
    }

    mx_mysql_finish(&mysql);
    mx_log_info("MySQL: Connection to database closed.");
    return 0;
};
