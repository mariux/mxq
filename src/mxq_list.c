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

struct mxq_status {
    char       user_name[256];
    char       group_id[512];
    char       job_command[4096];

    u_int64_t  exit_status;
    u_int64_t  max_stats_maxrss_mib;
    u_int64_t  total_runtime_seconds;
    u_int64_t  total_real_seconds;
    u_int64_t  total_jobs;
    u_int64_t  total_jobs_in_q;
    u_int64_t  total_jobs_runnning;
    u_int64_t  total_jobs_done;
};

void mxq_mysql_row_to_status(struct mxq_status *status, MYSQL_ROW row)
{
    int r;

    assert(sizeof(uid_t)  <= 4);
    assert(sizeof(gid_t)  <= 4);
    assert(sizeof(pid_t)  <= 4);
    assert(sizeof(mode_t) <= 4);

    r = 0;

    strncpy(status->user_name, row[r++], sizeof(status->user_name)-1);
    strncpy(status->group_id, row[r++], sizeof(status->group_id)-1);
    strncpy(status->job_command, row[r++], sizeof(status->job_command)-1);

    safe_convert_string_to_ui64(row[r++], &status->exit_status);
    safe_convert_string_to_ui64(row[r++], &status->max_stats_maxrss_mib);
    safe_convert_string_to_ui64(row[r++], &status->total_runtime_seconds);
    safe_convert_string_to_ui64(row[r++], &status->total_real_seconds);
    safe_convert_string_to_ui64(row[r++], &status->total_jobs);
    safe_convert_string_to_ui64(row[r++], &status->total_jobs_in_q);
    safe_convert_string_to_ui64(row[r++], &status->total_jobs_runnning);
    safe_convert_string_to_ui64(row[r++], &status->total_jobs_done);
}


static void print_usage(void)
{
    printf(
    VERSIONFULL "\n"
    "  by Marius Tolzmann <tolzmann@molgen.mpg.de> " VERSIONDATE "\n"
    "  Max Planck Institute for Molecular Genetics - Berlin Dahlem\n"
    "\n"
    "Usage:\n"
    "  mxq_list\n"
    "\n"
    );
}



int mxq_mysql_load_status(MYSQL  *mysql, struct mxq_status **status)
{
    struct mxq_status *s = NULL;
    int res;
    int i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_rows;
    unsigned int num_fields;

    *status = NULL;

    mres = mxq_mysql_query_with_result(mysql, "SELECT "
        "user_name, "
        "group_id, "
        "job_command, "
        "exit_status, "
        "max_stats_maxrss_mib, "
        "total_runtime_seconds, "
        "total_real_seconds, "
        "total_jobs, "
        "total_jobs_in_q, "
        "total_jobs_runnning, "
        "total_jobs_done "
        "FROM v_job_overview "
        );

    if (!mres) {
        log_msg(0, "mxq_mysql_select_next_job: Failed to query database: Error: %s\n", mysql_error(mysql));
        sleep(10);
        return 0;
    }

    num_rows = mysql_num_rows(mres);

    if (!num_rows)
        return 0;

    num_fields = mysql_num_fields(mres);
    assert(num_fields == 11);

    *status = calloc(num_rows, sizeof(**status));
    if (!*status) {
        fprintf(stderr, "mxq_mysql_select_next_job: failed to allocate memory for status: %s\n", strerror(errno));
        return 0;
    }

    for (i = 0, s = *status; i < num_rows; i++, s++) {
        fflush(stdout);
        row = mysql_fetch_row(mres);
        if (!row) {
            fprintf(stderr, "mxq_mysql_select_next_job: Failed to fetch row: Error: %s\n", mysql_error(mysql));
            mysql_free_result(mres);
            return 0;
        }

        mxq_mysql_row_to_status(s, row);
    }
    mysql_free_result(mres);

    return num_rows;
}

void print_running(struct mxq_status *s)
{
    int years;
    int days;
    int hours;
    int minutes;
    int seconds;
    int status;
    
    printf("\n%s :: %s :: %s\n",
        s->user_name, s->group_id, s->job_command);
    printf("\tjobs: %lu running: %lu inq: %lu finished: %lu (%.2f%%)\n",
           s->total_jobs, s->total_jobs_runnning, s->total_jobs_in_q, 
           s->total_jobs_done, (double)s->total_jobs_done*100/s->total_jobs);
    printf("\tmaximum memory used by a single finished job (so far): %lu MiB\n",
           s->max_stats_maxrss_mib);


    printf("\ttime in cluster: ");
    
    seconds = s->total_real_seconds;
    
    years    = seconds/60/60/24/365;
    seconds -= years*365*24*60*60;
    
    days     = seconds/60/60/24;
    seconds -= days*60*60*24;
    
    hours    = seconds/60/60;
    seconds -= hours*60*60;

    minutes  = seconds/60;
    seconds -= minutes*60;
    
    if (years)
       printf("%d year%s ", years, (years != 1)?"s":"");

    if (years || days)
       printf("%d day%s ", days, (days != 1)?"s":"");

    if (years || days || hours)
       printf("%d hour%s ", hours, (hours != 1)?"s":"");

    if (years || days || hours || minutes)
       printf("%d minute%s ", minutes, (minutes != 1)?"s":"");
    
    printf("%d second%s\n", seconds, (seconds != 1)?"s":"");
    printf("\tcalculation time: ");
    
    seconds = s->total_runtime_seconds;
    
    years    = seconds/60/60/24/365;
    seconds -= years*365*24*60*60;
    
    days     = seconds/60/60/24;
    seconds -= days*60*60*24;
    
    hours    = seconds/60/60;
    seconds -= hours*60*60;

    minutes  = seconds/60;
    seconds -= minutes*60;
    
    if (years)
       printf("%d year%s ", years, (years != 1)?"s":"");

    if (years || days)
       printf("%d day%s ", days, (days != 1)?"s":"");

    if (years || days || hours)
       printf("%d hour%s ", hours, (hours != 1)?"s":"");

    if (years || days || hours || minutes)
       printf("%d minute%s ", minutes, (minutes != 1)?"s":"");
    
    printf("%d second%s ", seconds, (seconds != 1)?"s":"");

    printf("(speedup: %.2f)\n", (double)s->total_runtime_seconds/s->total_real_seconds);
}


void print_finished(struct mxq_status *s)
{
    int years;
    int days;
    int hours;
    int minutes;
    int seconds;
    int status;

    
    status = s->exit_status;
    
    
    printf("\n%s :: %s :: %s\n",
        s->user_name, s->group_id, s->job_command);
    printf("\t%lu jobs\t%lu MiB\n",
           s->total_jobs, s->max_stats_maxrss_mib);

    if (WIFEXITED(status)) {
        printf("\tprocesses exited with exit status %d\n", WEXITSTATUS(status));
    } else if(WIFSIGNALED(status)) {
        printf("\tprocesses were killed by signal %d\n", WTERMSIG(status));
    } else {
        assert(WIFCONTINUED(status) || WIFSTOPPED(status));
    }

    printf("\ttime in cluster: ");
    
    seconds = s->total_real_seconds;
    
    years    = seconds/60/60/24/365;
    seconds -= years*365*24*60*60;
    
    days     = seconds/60/60/24;
    seconds -= days*60*60*24;
    
    hours    = seconds/60/60;
    seconds -= hours*60*60;

    minutes  = seconds/60;
    seconds -= minutes*60;
    
    if (years)
       printf("%d year%s ", years, (years != 1)?"s":"");

    if (years || days)
       printf("%d day%s ", days, (days != 1)?"s":"");

    if (years || days || hours)
       printf("%d hour%s ", hours, (hours != 1)?"s":"");

    if (years || days || hours || minutes)
       printf("%d minute%s ", minutes, (minutes != 1)?"s":"");
    
    printf("%d second%s\n", seconds, (seconds != 1)?"s":"");
    printf("\tcalculation time: ");
    
    seconds = s->total_runtime_seconds;
    
    years    = seconds/60/60/24/365;
    seconds -= years*365*24*60*60;
    
    days     = seconds/60/60/24;
    seconds -= days*60*60*24;
    
    hours    = seconds/60/60;
    seconds -= hours*60*60;

    minutes  = seconds/60;
    seconds -= minutes*60;
    
    if (years)
       printf("%d year%s ", years, (years != 1)?"s":"");

    if (years || days)
       printf("%d day%s ", days, (days != 1)?"s":"");

    if (years || days || hours)
       printf("%d hour%s ", hours, (hours != 1)?"s":"");

    if (years || days || hours || minutes)
       printf("%d minute%s ", minutes, (minutes != 1)?"s":"");
    
    printf("%d second%s ", seconds, (seconds != 1)?"s":"");

    printf("(speedup: %.2f)\n", (s->total_real_seconds?((double)s->total_runtime_seconds/s->total_real_seconds):0.0));
}



void mxq_list(struct mxq_mysql *mmysql, char *user_name, int flags)
{
    _cleanup_close_mysql_ MYSQL *mysql = NULL;
    _cleanup_free_ struct mxq_status *status;
    struct mxq_status *s;
    int cnt;
    int i;
    int first;
    int last;

    mysql = mxq_mysql_connect(mmysql);

    cnt = mxq_mysql_load_status(mysql, &status);
    
    for (i = 0, s = status, first = 0; i < cnt; i++, s++) {
        if (s->total_jobs_runnning || s->total_jobs_in_q) {
            if (!first++) {
                printf("\n === running jobs ===\n");
            }
            print_running(s);
        }
    }
    last = first;

    for (i = 0, s = status, first = 0; i < cnt; i++, s++) {
        if (!(s->total_jobs_runnning || s->total_jobs_in_q) && 
            (streq(user_name, "root") || (streq(user_name, s->user_name)))) {
            
            if (!first++) {
                printf("\n === job history ===\n");
            }
            print_finished(s);
        }
    }
    
    if (first || last)
       printf("\n");
    
}

int main(int argc, char *argv[])
{
    int i;
    uid_t ruid, euid, suid;
    gid_t rgid, egid, sgid;
    int res;

    char      *arg_mysql_default_file;
    char      *arg_mysql_default_group;

    int flags = 0;

    struct mxq_job_full job;
    struct mxq_mysql mmysql;

    int opt;
    struct bee_getopt_ctl optctl;
    struct bee_option opts[] = {
                BEE_OPTION_NO_ARG("help",                 'h'),
                BEE_OPTION_NO_ARG("version",              'V'),

                BEE_OPTION_REQUIRED_ARG("mysql-default-file",  'M'),
                BEE_OPTION_REQUIRED_ARG("mysql-default-group", 'S'),
                BEE_OPTION_END
    };

    struct passwd *passwd;
    struct group  *group;

    /******************************************************************/

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

            case 'M':
                arg_mysql_default_file = optctl.optarg;
                break;

            case 'S':
                arg_mysql_default_group = optctl.optarg;
                break;
        }
    }

    BEE_GETOPT_FINISH(optctl, argc, argv);

    /******************************************************************/

    res = getresuid(&ruid, &euid, &suid);
    assert(res != -1);

    passwd = getpwuid(ruid);
    assert(passwd != NULL);

    /******************************************************************/

    mmysql.default_file  = arg_mysql_default_file;
    mmysql.default_group = arg_mysql_default_group;

    /******************************************************************/

    mxq_list(&mmysql, passwd->pw_name, flags);

    /******************************************************************/

    return 0;
}

