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


int mxq_mysql_add_task(MYSQL *mysql, struct mxq_task *task)
{
    assert(task);

    _cleanup_free_ char *q_groupname   = NULL;
    _cleanup_free_ char *q_command     = NULL;
    _cleanup_free_ char *q_argv        = NULL;
    _cleanup_free_ char *q_workdir     = NULL;
    _cleanup_free_ char *q_stdout      = NULL;
    _cleanup_free_ char *q_stderr      = NULL;
    _cleanup_free_ char *q_submit_host = NULL;

    int   len;
    int   res;
    int   i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_fields;
    unsigned int num_rows;

    if (!(q_groupname   = mxq_mysql_escape_string(mysql, task->groupname)   )) return 0;
    if (!(q_command     = mxq_mysql_escape_string(mysql, task->command)     )) return 0;
    if (!(q_argv        = mxq_mysql_escape_string(mysql, task->argv)        )) return 0;
    if (!(q_workdir     = mxq_mysql_escape_string(mysql, task->workdir)     )) return 0;
    if (!(q_stdout      = mxq_mysql_escape_string(mysql, task->stdout)      )) return 0;
    if (!(q_stderr      = mxq_mysql_escape_string(mysql, task->stderr)      )) return 0;
    if (!(q_submit_host = mxq_mysql_escape_string(mysql, task->submit_host) )) return 0;

    res = mxq_mysql_query(mysql, "INSERT INTO tasks"
             " SET job_id = '%d'"
             ", status = %d"
             ", gid = %d"
             ", groupname = '%s'"
             ", priority = %d"
             ", command = '%s'"             
             ", argc = %d"
             ", argv = '%s'"             
             ", workdir = '%s'"             
             ", stdout = '%s'"             
             ", stderr = '%s'"
             ", umask = %d"
             ", submit_host = '%s'",
             task->job->id, task->status, task->gid, q_groupname, task->priority,
             q_command, task->argc, q_argv, q_workdir, q_stdout, q_stderr, 
             task->umask, q_submit_host); 
    if (res) {
        fprintf(stderr, "Failed to query database: Error: %s\n", mysql_error(mysql));
        return 0;
    }

    task->id       = mysql_insert_id(mysql);
    return 1;
}


int mxq_mysql_load_job_by_name(MYSQL *mysql, struct mxq_job *job)
{
    assert(job);
    assert(job->jobname);
    assert(job->jobname[0]);
    assert(job->username);
    assert(job->username[0]);
    assert(job->uid >= 0);

    _cleanup_free_ char *q_username = NULL;
    _cleanup_free_ char *q_jobname  = NULL;
    _cleanup_free_ char *query      = NULL;

    int   len;
    int   res;
    int   i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_fields;
    unsigned int num_rows;

    if (!(q_jobname  = mxq_mysql_escape_string(mysql, job->jobname)  )) return 0;
    if (!(q_username = mxq_mysql_escape_string(mysql, job->username) )) return 0;

    mres = mxq_mysql_query_with_result(mysql, "SELECT id from jobs"
             " WHERE jobname = '%s'"
             " AND username = '%s'"
             " AND uid = %d"
             " AND priority = %d"
             " ORDER BY id DESC LIMIT 1", 
             q_jobname, q_username, job->uid, job->priority);

    if (!mres) {
        fprintf(stderr, "Error: %s\n", mysql_error(mysql));
        return 0;
    }

    num_rows = mysql_num_rows(mres);
    assert(num_rows <= 1);
    if (num_rows == 1) {
        num_fields = mysql_num_fields(mres);
        assert(num_fields == 1);

        row = mysql_fetch_row(mres);
        if (!row) {
            fprintf(stderr, "Failed to fetch row: Error: %s\n", mysql_error(mysql));
            return 0;
        }
        job->id       = atoi(row[0]);

        mysql_free_result(mres);
        return 1;
    } else {
        mysql_free_result(mres);
    }

    res = mxq_mysql_query(mysql, "INSERT INTO jobs"
             " SET jobname = '%s'"
             ", username = '%s'"
             ", uid = %d"
             ", priority = %d"
             ", status = %d", 
             q_jobname, q_username, job->uid, job->priority, job->status);    
    if (res) {
        fprintf(stderr, "Failed to query database: Error: %s\n", mysql_error(mysql));
        return 0;
    }

    job->id       = mysql_insert_id(mysql);
    return 1;
}

int mxq_submit_task(struct mxq_mysql *mmysql, struct mxq_task *task, int flags)
{
    MYSQL *mysql;

    mysql = mxq_mysql_connect(mmysql);

    mxq_mysql_load_job_by_name(mysql, task->job);
    mxq_mysql_add_task(mysql, task);

    mysql_close(mysql);

    return 1;
}


int main(int argc, char *argv[])
{
    int i;
    uid_t ruid, euid, suid;
    gid_t rgid, egid, sgid;
    int res;
    
    char *arg_stdout;
    char *arg_stderr;
    char *arg_jobname;
    char *arg_taskpriority;
    char *arg_jobpriority;
    char *arg_workdir;
    char *arg_umask;
    char *current_workdir;

    short arg_append;
    short arg_newjob;

    char *arg_mysql_default_file;
    char *arg_mysql_default_group;
    
    char hostname[1024];    
    
    int opt;
    int flags = 0;
    
    struct mxq_job   job;
    struct mxq_task  task;
    struct mxq_mysql mmysql;

    struct bee_getopt_ctl optctl;
    
    struct bee_option opts[] = {
        BEE_OPTION_NO_ARG("help",               'h'),
        BEE_OPTION_NO_ARG("version",            'V'),
        BEE_OPTION_REQUIRED_ARG("stdout",       'o'),
        BEE_OPTION_REQUIRED_ARG("stderr",       'e'),
        BEE_OPTION_REQUIRED_ARG("workdir",      'w'),
        BEE_OPTION_REQUIRED_ARG("umask",        'm'),
        BEE_OPTION_REQUIRED_ARG("jobname",      'n'),
        BEE_OPTION_REQUIRED_ARG("priority",     'p'),
        BEE_OPTION_REQUIRED_ARG("taskpriority", 'p'),
        BEE_OPTION_REQUIRED_ARG("jobpriority",  'P'),
        BEE_OPTION_NO_ARG("append",  'A'),
        BEE_OPTION_NO_ARG("new-job", 'N'),
        BEE_OPTION_REQUIRED_ARG("mysql-default-file", 'M'),
        BEE_OPTION_REQUIRED_ARG("mysql-default-group", 'G'),
        BEE_OPTION_END
    };

    struct passwd *passwd;
    struct group  *group;

    arg_stdout       = "/dev/null";
    arg_stderr       = "stdout";
    arg_taskpriority = "default(127)";
    arg_jobpriority  = "default(127)";
    arg_jobname      = NULL;
    current_workdir  = get_current_dir_name();
    arg_workdir      = current_workdir;
    arg_umask        = NULL;
    
    arg_append = 0;
    arg_newjob = 0;

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = "mxq_submit";

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    bee_getopt_init(&optctl, argc-1, &argv[1], opts);

    optctl.flags = BEE_FLAG_STOPONUNKNOWN|BEE_FLAG_STOPONNOOPT;
    optctl.flags = BEE_FLAG_STOPONUNKNOWN;

    while ((opt=bee_getopt(&optctl, &i)) != BEE_GETOPT_END) {
        if (opt == BEE_GETOPT_ERROR) {
            exit(EX_USAGE);
        }
        
        switch (opt) {
            case 'h':
            case 'V':
                printf("help/version\n");
                printf("mxq_submit [mxq-options] <command> [command-arguments]\n");
                exit(EX_USAGE);
                
            case 'o':
                arg_stdout = optctl.optarg;
                break;
                
            case 'e':
                arg_stderr = optctl.optarg;
                break;
                
            case 'w':
                if (optctl.optarg[0] == '/')
                    arg_workdir = optctl.optarg;
                else
                    fprintf(stderr, "ignoring relative workdir\n");
                break;

            case 'n':
                arg_jobname = optctl.optarg;
                break;
                
            case 'p':
                arg_taskpriority = optctl.optarg;
                break;
                
            case 'P':
                arg_jobpriority = optctl.optarg;
                break;
                
            case 'm':
                arg_umask = optctl.optarg;
                break;

            case 'M':
                arg_mysql_default_file = optctl.optarg;
                break;
                
            case 'G':
                arg_mysql_default_group = optctl.optarg;
                break;
                
            case 'A':
                arg_append = 1;
                arg_newjob = 0;
                break;
                
            case 'N':
                arg_newjob = 1;
                arg_append = 0;
                break;
                
        }
    }

    BEE_GETOPT_FINISH(optctl, argc, argv);
    assert(argc >= 1);

    /* from this point values in argc,argv are the ones of the cluster job  */

    if (!arg_jobname)
        arg_jobname = arg_workdir;

    res = getresuid(&ruid, &euid, &suid);
    assert(res != -1);

    passwd = getpwuid(ruid);
    assert(passwd != NULL);

    res = getresgid(&rgid, &egid, &sgid);
    assert(res != -1);
    
    group = getgrgid(rgid);
    assert(group != NULL);
    
    res = gethostname(hostname, 1024);
    if (res == -1) {
        assert(errno == ENAMETOOLONG);
        hostname[1024-1] = 0;
    }

    task.job          = &job;

    task.job->jobname  = arg_jobname;
    task.job->uid      = ruid;
    task.job->username = passwd->pw_name;
    task.job->priority = atoi(arg_jobpriority);
    task.job->status   = 0;

    task.gid          = rgid;
    task.groupname    = group->gr_name;
    task.command      = argv[0];
    task.priority     = atoi(arg_taskpriority);
    task.status       = 0;
    task.workdir      = arg_workdir;
    task.stdout       = arg_stdout;
    if (streq(arg_stderr, "stdout")) {
        task.stderr       = arg_stdout;
    } else {
        task.stderr       = arg_stderr;
    }
    
    if (arg_umask) { 
        task.umask = strtol(arg_umask, NULL, 0);
    } else {
        task.umask = getumask();
    }

    task.submit_host  = hostname;
    task.argc         = argc;
    task.argv         = stringvectostring(argc, argv);
    assert(task.argv);

    if (arg_append)
        flags |= MXQ_TASK_JOB_FORCE_APPEND;
    else if (arg_newjob)
        flags |= MXQ_TASK_JOB_FORCE_NEW;

    mmysql.default_file  = arg_mysql_default_file;
    mmysql.default_group = arg_mysql_default_group;

    mxq_submit_task(&mmysql, &task, flags);

    printf("mxq_job_id=%d\n", task.job->id);
    printf("mxq_task_id=%d\n", task.id);
    
    
    free(task.argv);
    free(current_workdir);
    
    return 0;
}

