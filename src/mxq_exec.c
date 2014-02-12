#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>

#include <sys/types.h>

#include <pwd.h>
#include <grp.h>

#include <assert.h>

#include <sysexits.h>

#include <string.h>
#include <stdarg.h>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include <time.h>

#include <my_global.h>
#include <mysql.h>

#include "bee_getopt.h"

#include "mxq_util.h"
#include "mxq_mysql.h"


#define MXQ_TASK_JOB_FORCE_APPEND  (1<<0)
#define MXQ_TASK_JOB_FORCE_NEW     (1<<1)

#define MXQ_JOB_STATUS_ACTIVE      (1)

#define MXQ_SELECT_TASK_FULL \
        " job_id," \
        " job_name," \
        " job_status," \
        " job_priority," \
        " user_uid," \
        " user_name," \
        " group_gid," \
        " group_name," \
        " task_id," \
        " task_status," \
        " task_priority," \
        " task_threads, " \
        " task_command," \
        " task_argc," \
        " task_argv," \
        " task_workdir," \
        " task_stdout," \
        " task_stderr," \
        " host_server_id," \
        " host_hostname," \
        " host_pid"


void mxq_mysql_row_to_job(struct mxq_job_full *job, MYSQL_ROW row)
{
    MYSQL_ROW r;

    assert(sizeof(uid_t)  <= 4);
    assert(sizeof(gid_t)  <= 4);
    assert(sizeof(pid_t)  <= 4);
    assert(sizeof(mode_t) <= 4);

    memset(job, 0, sizeof(*job));

    r = row;

    safe_convert_string_to_ui64(*r++, &job->job_id);
    safe_convert_string_to_ui8(*r++,  &job->job_status);
    safe_convert_string_to_ui16(*r++, &job->job_priority);

    strncpy(job->group_id, *r++, sizeof(job->group_id)-1);

    safe_convert_string_to_ui8(*r++,   &job->group_status);
    safe_convert_string_to_ui16(*r++,  &job->group_priority);

    safe_convert_string_to_ui32(*r++,  &job->user_uid);
    strncpy(job->user_name,  *r++, sizeof(job->user_name)-1);
    safe_convert_string_to_ui32(*r++,  &job->user_gid);
    strncpy(job->user_group, *r++, sizeof(job->user_group)-1);

    safe_convert_string_to_ui16(*r++,  &job->job_threads);
    safe_convert_string_to_ui64(*r++,  &job->job_memory);
    safe_convert_string_to_ui32(*r++,  &job->job_time);

    strncpy(job->job_workdir, *r++, sizeof(job->job_workdir)-1);
    strncpy(job->job_command, *r++, sizeof(job->job_command)-1);
    safe_convert_string_to_ui16(*r++,  &job->job_argc);
    strncpy(job->job_argv,    *r++, sizeof(job->job_argv)-1);

    strncpy(job->job_stdout,  *r++, sizeof(job->job_stdout)-1);
    strncpy(job->job_stderr,  *r++, sizeof(job->job_stderr)-1);

    if (streq(job->job_stdout, "/dev/null")) {
        strncpy(job->tmp_stdout, job->job_stdout, sizeof(job->tmp_stdout)-1);
    } else {
        snprintf(job->tmp_stdout, sizeof(job->tmp_stdout)-1, "%s.%d.mxqtmp", job->job_stdout, job->job_id);
    }

    if (streq(job->job_stderr, "/dev/null")) {
        strncpy(job->tmp_stderr, job->job_stderr, sizeof(job->job_stderr)-1);
    } else {
        snprintf(job->tmp_stderr, sizeof(job->tmp_stderr)-1, "%s.%d.mxqtmp", job->job_stderr, job->job_id);
    }

    safe_convert_string_to_ui32(*r++,  &job->job_umask);

    strncpy(job->host_submit,    *r++, sizeof(job->host_submit)-1);

    strncpy(job->server_id,      *r++, sizeof(job->server_id)-1);
    strncpy(job->host_hostname,  *r++, sizeof(job->host_hostname)-1);

    safe_convert_string_to_ui32(*r++,  &job->host_pid);
}

 struct mxq_job_full *mxq_mysql_select_next_job(MYSQL *mysql, char *hostname, char *serverid)
{
    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_rows;
    unsigned int num_fields;

    _cleanup_free_ char *q_hostname = NULL;
    _cleanup_free_ char *q_serverid = NULL;

    struct mxq_job_full *job = NULL;

    if (!(q_hostname = mxq_mysql_escape_string(mysql, hostname) )) return 0;
    if (!(q_serverid = mxq_mysql_escape_string(mysql, serverid) )) return 0;

    mres = mxq_mysql_query_with_result(mysql, "SELECT "
        "job_id, "
        "job_status, "
        "job_priority, "
        "group_id, "
        "group_status, "
        "group_priority, "
        "user_uid, "
        "user_name, "
        "user_gid, "
        "user_group, "
        "job_threads, "
        "job_memory, "
        "job_time, "
        "job_workdir, "
        "job_command, "
        "job_argc, "
        "job_argv, "
        "job_stdout, "
        "job_stderr, "
        "job_umask, "
        "host_submit, "
        "server_id, "
        "host_hostname, "
        "host_pid "
        "FROM job "
        "WHERE host_hostname = '%s' "
        "AND server_id = '%s' "
        "AND host_pid IS NULL "
        "AND job_status = 1 "
        "LIMIT 1",
        q_hostname, q_serverid);

    if (!mres) {
        log_msg(0, "mxq_mysql_select_next_job: Failed to query database: Error: %s\n", mysql_error(mysql));
        sleep(10);
        return NULL;
    }

    num_rows = mysql_num_rows(mres);
    assert(num_rows <= 1);

    if (num_rows == 1) {
        num_fields = mysql_num_fields(mres);
        assert(num_fields == 24);

        row = mysql_fetch_row(mres);
        if (!row) {
            fprintf(stderr, "mxq_mysql_select_next_job: Failed to fetch row: Error: %s\n", mysql_error(mysql));
            mysql_free_result(mres);
            return NULL;
        }
        job = calloc(1, sizeof(*job));
        if (!job) {
            fprintf(stderr, "mxq_mysql_select_next_job: failed to allocate memory for job: %s\n", strerror(errno));
            mysql_free_result(mres);
            return NULL;
        }
        mxq_mysql_row_to_job(job, row);
    }

    mysql_free_result(mres);

    return job;
}

int mxq_mysql_job_started(MYSQL *mysql, int job_id, int host_pid)
{
    int res;
    int tries = 1;

    assert(mysql);
    assert(job_id);
    assert(host_pid);

    do {
        res = mxq_mysql_query(mysql, "UPDATE job SET "
                "host_pid = %d, "
                "job_status = 2, "
                "date_start = NULL "
                "WHERE job_id = %d "
                "AND   host_pid IS NULL",
                host_pid, job_id);
        if (res) {
            log_msg(0, "mxq_mysql_job_started: Failed to query database (tries=%d): Error(%d): %s\n", tries++, res, mysql_error(mysql));
            sleep(1);
        }
    } while (res);

    return mysql_affected_rows(mysql);
}

int mxq_mysql_reserve_job(MYSQL  *mysql, char *hostname, char *server_id)
{
    assert(mysql);

    int   len;
    int   res;
    int   i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_fields;
    unsigned int num_rows;

    _cleanup_free_ char *q_hostname  = NULL;
    _cleanup_free_ char *q_server_id = NULL;

    if (!(q_hostname  = mxq_mysql_escape_string(mysql, hostname)  )) return 0;
    if (!(q_server_id = mxq_mysql_escape_string(mysql, server_id) )) return 0;

    // update v_tasks set task_status=1,host_hostname='localhost',host_server_id='localhost-1'
    // where task_status = 0 AND host_hostname='localhost' AND host_pid IS NULL order by job_id limit 1;

    res = mxq_mysql_query(mysql, "UPDATE job SET "
                "job_status = 1, "
                "host_hostname = '%s', "
                "server_id = '%s' "
                "WHERE job_status = 0 "
                "AND host_hostname IS NULL "
                "AND server_id IS NULL "
                "AND host_pid IS NULL "
                "ORDER BY job_id "
                "LIMIT 1",
                q_hostname, q_server_id);
    if (res) {
        log_msg(0, "mxq_mysql_reserve_job: Failed to query database: Error: %s\n", mysql_error(mysql));
        sleep(10);
        return -1;
    }

    return mysql_affected_rows(mysql);

    return 0;
}

#define MXQ_TOTIME(x) ((double)(( (double)(x).tv_sec*1000000L + (double)(x).tv_usec)/1000000L))


int mxq_mysql_finish_job(MYSQL *mysql, struct mxq_job_full *job)
{
    int   res;
    int   tries = 1;

    assert(mysql);

    do {
        res = mxq_mysql_query(mysql, "UPDATE job SET "
                    "date_end = NULL, "
                    "stats_status = %d, "
                    "stats_utime = %f, "
                    "stats_stime = %f, "
                    "stats_real = %f, "
                    "stats_maxrss = %d, "
                    "stats_minflt = %d, "
                    "stats_majflt = %d, "
                    "stats_nswap = %d, "
                    "stats_inblock = %d, "
                    "stats_oublock = %d, "
                    "stats_nvcsw = %d, "
                    "stats_nivcsw = %d, "
                    "job_status = %d "
                    "WHERE job_status = 2 "
                    "AND job_id = %d ",
                    job->stats_status,
                    MXQ_TOTIME(job->stats_rusage.ru_utime),
                    MXQ_TOTIME(job->stats_rusage.ru_stime),
                    MXQ_TOTIME(job->stats_realtime),
                    job->stats_rusage.ru_maxrss,
                    job->stats_rusage.ru_minflt,
                    job->stats_rusage.ru_majflt,
                    job->stats_rusage.ru_nswap,
                    job->stats_rusage.ru_inblock,
                    job->stats_rusage.ru_oublock,
                    job->stats_rusage.ru_nvcsw,
                    job->stats_rusage.ru_nivcsw,
                    job->job_status, job->job_id);
        if (res) {
            log_msg(0, "mxq_mysql_finish_job: Failed to query database (tries=%d): Error(%d): %s\n", tries++, res, mysql_error(mysql));
            sleep(5);
        }
    } while (res);

    return mysql_affected_rows(mysql);
}

struct mxq_job_full *mxq_mysql_load_next_job(MYSQL  *mysql, char *hostname, char *server_id)
{
    struct mxq_job_full *job = NULL;
    int res;

    while (1) {
        /* add res check here.. to be sure if it failed because there are no jobs or during query */
        job = mxq_mysql_select_next_job(mysql, hostname, server_id);
        if (job) {
            return job;
        }

        res = mxq_mysql_reserve_job(mysql, hostname, server_id);
        if (res < 1) {
            return NULL;
        }
    };
}

struct mxq_reaped_child {
    pid_t pid;
    int status;
    struct rusage rusage;
    struct timeval time;
    int signal;
    int reaped;
};

static struct mxq_reaped_child *mxq_childs = NULL;
static int mxq_child_index_reaped   = 0;
static int mxq_child_index_finished = 0;
static int mxq_max_childs           = 0;

static void child_handler(int sig);

static struct mxq_reaped_child *mxq_setup_reaper(int max_childs)
{
    struct sigaction sa;

    assert(mxq_childs == NULL);
    assert(max_childs > 0);

    mxq_child_index_reaped   = 0;
    mxq_child_index_finished = 0;
    mxq_max_childs           = max_childs;

    mxq_childs = calloc(mxq_max_childs, sizeof(*mxq_childs));

    if (!mxq_childs)
        return NULL;

    sigemptyset(&sa.sa_mask);

    sa.sa_flags = 0;
    sa.sa_handler = child_handler;

    sigaction(SIGCHLD, &sa, NULL);

    return mxq_childs;
}

void mxq_cleanup_reaper(void)
{
    free(mxq_childs);
    mxq_childs = NULL;
}

static void child_handler(int sig)
{
    pid_t pid;
    struct rusage rusage;
    int status;

    struct mxq_reaped_child *child;

    while ((mxq_childs[mxq_child_index_reaped].reaped == 0) && (pid = wait3(&status, WNOHANG, &rusage)) > 0) {
        child = &mxq_childs[mxq_child_index_reaped];

        mxq_child_index_reaped = (mxq_child_index_reaped + 1) % mxq_max_childs;

        gettimeofday(&child->time, NULL);
        child->rusage = rusage;
        child->status = status;
        child->signal = sig;
        child->pid    = pid;
        child->reaped = 1;
    };
}

struct mxq_job_full_list_item *joblist_add_job(struct mxq_job_full_list *list, struct mxq_job_full *job)
{
    struct mxq_job_full_list_item *li;

    assert(list);
    assert(job);

    li = calloc(1, sizeof(*li));
    if (!li) {
        return NULL;
    }

    li->job = job;

    if (!list->first) {
        assert(!list->last);
        assert(!list->count);

        list->first = li;
        list->last  = li;
    } else {
        assert(list->last);
        assert(list->count);

        li->prev = list->last;
        list->last->next = li;
        list->last = li;
    }
    list->count++;
    return li;
}

struct mxq_job_full *joblist_remove_item(struct mxq_job_full_list *list, struct mxq_job_full_list_item *li)
{
    struct mxq_job_full *job;
    struct mxq_job_full_list_item *l;

    assert(list);
    assert(list->count);
    assert(li);

    if (li == list->first) {
        list->first = li->next;
    }

    if (li == list->last) {
        list->last = li->prev;
    }

    if (li->next) {
        li->next->prev = li->prev;
    }

    if (li->prev) {
        li->prev->next = li->next;
    }

    job = li->job;

    free(li);

    list->count--;

    return job;
}

struct mxq_job_full_list_item *joblist_find_item_by_host_pid(struct mxq_job_full_list *list, pid_t host_pid)
{
    struct mxq_job_full_list_item *li;

    assert(list);
    assert(host_pid);

    for (li = list->first; li; li = li->next) {
        assert(li->job);

        if (li->job->host_pid == host_pid) {
            return li;
        }
    }
    return NULL;
}

struct mxq_job_full *joblist_remove_job_by_host_pid(struct mxq_job_full_list *job_list, pid_t host_pid)
{
    struct mxq_job_full_list_item *li;

    assert(job_list);
    assert(host_pid);

    li = joblist_find_item_by_host_pid(job_list, host_pid);
    if (!li) {
        return NULL;
    }

    return joblist_remove_item(job_list, li);
}

void job_add_child_stats(struct mxq_job_full *job, struct mxq_reaped_child *child)
{
    struct timeval end;
    struct timeval start;

    assert(job);
    assert(child);
    assert(child->pid == job->host_pid);

    job->job_status = 3;

    job->stats_status   = child->status;
    job->stats_rusage   = child->rusage;

    start = job->stats_starttime;
    end   = child->time;

    if (end.tv_usec < start.tv_usec) {
        assert(end.tv_sec);
        end.tv_usec += 1000000L;
        end.tv_sec  -= 1;
    }

    job->stats_realtime.tv_sec  = end.tv_sec  - start.tv_sec;
    job->stats_realtime.tv_usec = end.tv_usec - start.tv_usec;
}


int job_finish_cleanup_files(struct mxq_job_full *job)
{
    int res;
    
    assert(job);

    if (!streq(job->job_stdout, "/dev/null")) {
        log_msg(0, "job_id=%d action=rename-stdout tmp_stdout=%s job_stdout=%s\n", job->job_id, job->tmp_stdout, job->job_stdout);
        res = rename(job->tmp_stdout, job->job_stdout);
        if (res == -1) {
            log_msg(0, "job_id=%d rename(%s, %s) failed: %s\n", job->job_id, job->tmp_stdout, job->job_stdout, strerror(errno));
        }
    }

    if (!streq(job->job_stderr, "/dev/null") && !streq(job->job_stderr, job->job_stdout)) {
        log_msg(0, "job_id=%d action=rename-stderr tmp_stderr=%s job_stderr=%s\n", job->job_id, job->tmp_stderr, job->job_stderr);
        res = rename(job->tmp_stderr, job->job_stderr);
        if (res == -1) {
            log_msg(0, "job_id=%d rename(%s, %s) failed: %s\n", job->job_id, job->tmp_stderr, job->job_stderr, strerror(errno));
        }
    }
}


void job_finish_log(struct mxq_job_full *job)
{
    int status;
    char *exit_status;
    int exit_code = -1;

    assert(job);

    status = job->stats_status;


    if (WIFEXITED(status)) {
            exit_status = "exited";
            exit_code = WEXITSTATUS(status);
    } else if(WIFSIGNALED(status)) {
            exit_status = "killed";
            exit_code = WTERMSIG(status);
    } else if(WIFSTOPPED(status)) {
            exit_status = "stopped";
            exit_code = WSTOPSIG(status);
    } else {
            assert(WIFCONTINUED(status));
            exit_status = "continued";
    }

    log_msg(0, "job_id=%d exit_status=%s exit_code=%d threads=%d status=%d maxrss=%d usr=%lf sys=%lf real=%lf\n",
                job->job_id,
                exit_status,
                exit_code,
                job->job_threads,
                job->stats_status,
                job->stats_rusage.ru_maxrss,
                MXQ_TOTIME(job->stats_rusage.ru_utime),
                MXQ_TOTIME(job->stats_rusage.ru_stime),
                MXQ_TOTIME(job->stats_realtime)
    );
}

int mxq_mysql_finish_reaped_jobs(MYSQL *mysql, struct mxq_job_full_list *job_list)
{
    struct mxq_job_full *job;
    struct mxq_job_full_list_item *li;
    int cnt = 0;
    int res;

    char *exit_status;
    int exit_code = -1;
    int index;

    struct mxq_reaped_child *child;

    while (mxq_childs[mxq_child_index_finished].reaped == 1) {
        child = &mxq_childs[mxq_child_index_finished];

        log_msg(0, "pid=%d action=finish-child mxq_child_index_finished=%d mxq_child_index_reaped=%d\n", child->pid, mxq_child_index_finished, mxq_child_index_reaped);

        job = joblist_remove_job_by_host_pid(job_list, child->pid);
        if (!job) {
            log_msg(0, "pid=%d error=no-matching-job job_cnt=%d postponing\n", child->pid, job_list->count);
            return -1;
        }

        job_add_child_stats(job, child);
        job_finish_cleanup_files(job);
        mxq_mysql_finish_job(mysql, job);
        job_finish_log(job);

        cnt += job->job_threads;

        mxq_free_job(job);

        child->reaped = 0;

        mxq_child_index_finished = (mxq_child_index_finished + 1) % mxq_max_childs;
    }

    return cnt;
}

int job_setup_environment(struct mxq_job_full *job)
{
    int res;
    struct passwd *passwd;
    char *job_id_str;
    _cleanup_free_ char *job_id_str_buf = NULL;
    int fh;

    res = clearenv();
    if (res != 0) {
        log_msg(0, "jobd_id=%d clearenv() failed. (%s)\n", job->job_id, strerror(errno));
        return 0;
    }

    passwd = getpwuid(job->user_uid);
    assert(passwd != NULL);
    assert(streq(passwd->pw_name, job->user_name));

    res = asprintf(&job_id_str_buf, "%d", job->job_id);
    if (res != -1) {
        job_id_str = job_id_str_buf;
    } else {
        job_id_str = "0";
    }

    setenv("JOB_ID",   job_id_str, 1);
    setenv("USER",     job->user_name, 1);
    setenv("USERNAME", job->user_name, 1);
    setenv("LOGNAME",  job->user_name, 1);
    setenv("PATH",     "/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin:/usr/local/package/bin", 1);
    setenv("PWD",      job->job_workdir , 1);
    setenv("HOME",     passwd->pw_dir, 1);
    setenv("HOSTNAME", mxq_hostname(), 1);

    res = initgroups(passwd->pw_name, job->user_gid);
    if (res == -1) {
        log_msg(0, "jobd_id=%d initgroups() failed. (%s)\n", job->job_id, strerror(errno));
        return 0;
    }

    fh = open("/proc/self/loginuid", O_WRONLY|O_TRUNC);
    if (fh == -1) {
        log_msg(0, "job_id=%d open(%s) failed (%s)\n", job->job_id, "/proc/self/loginuid", strerror(errno));
        return 0;
    }
    dprintf(fh, "%d", job->user_uid);
    close(fh);

    res = setregid(job->user_gid, job->user_gid);
    if (res == -1) {
        log_msg(0, "job_id=%d: setregid(%d, %d) failed (%s)\n", job->job_id, job->user_gid, job->user_gid, strerror(errno));
        return 0;
    }

    res = setreuid(job->user_uid, job->user_uid);
    if (res == -1) {
        log_msg(0, "job_id=%d: setreuid(%d, %d) failed (%s)\n", job->job_id, job->user_uid, job->user_uid, strerror(errno));
        return 0;
    }

    res = chdir(job->job_workdir);
    if (res == -1) {
        log_msg(0, "job_id=%d: chdir(%s) failed (%s)\n", job->job_id, job->job_workdir, strerror(errno));
        return 0;
    }

    umask(job->job_umask);

    return 1;
 }

int main(int argc, char *argv[])
{
    struct mxq_mysql mmysql;
    _cleanup_close_mysql_ MYSQL *mysql = NULL;

    struct mxq_job_full *job = NULL;
    struct mxq_job_full_list job_list;

    int status;

    int res;
    pid_t pid;

    int i;

    u_int16_t threads_max     = 1;
    u_int16_t threads_current = 0;

    int fh;
    int opt;

    char *arg_mysql_default_file;
    char *arg_mysql_default_group;
    char *arg_server_id;

    struct bee_getopt_ctl optctl;
    struct bee_option opts[] = {
                BEE_OPTION_NO_ARG("help",               'h'),
                BEE_OPTION_NO_ARG("version",            'V'),
                BEE_OPTION_REQUIRED_ARG("threads",      'j'),
                BEE_OPTION_REQUIRED_ARG("server_id",    'N'),
                BEE_OPTION_REQUIRED_ARG("mysql-default-file", 'M'),
                BEE_OPTION_REQUIRED_ARG("mysql-default-group", 'S'),
                BEE_OPTION_END
    };

    arg_server_id = "main";
    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = "mxq_submit";

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    bee_getopt_init(&optctl, argc-1, &argv[1], opts);

    optctl.flags = BEE_FLAG_STOPONUNKNOWN|BEE_FLAG_STOPONNOOPT;
//    optctl.flags = BEE_FLAG_STOPONUNKNOWN;

    while ((opt=bee_getopt(&optctl, &i)) != BEE_GETOPT_END) {
        if (opt == BEE_GETOPT_ERROR) {
            exit(EX_USAGE);
        }

        switch (opt) {
            case 'h':
            case 'V':
                printf("help/version\n");
                printf("mxq_exec [mxq-options]\n");
                exit(EX_USAGE);

            case 'j':
                if (!safe_convert_string_to_ui16(optctl.optarg, &threads_max)) {
                    fprintf(stderr, "ignoring threads '%s': %s\n", optctl.optarg, strerror(errno));
                }
                if (!threads_max)
                    threads_max = 1;
                break;

            case 'N':
                arg_server_id = optctl.optarg;
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

    mxq_setup_reaper(threads_max);

    mysql = mxq_mysql_connect(&mmysql);

    memset(&job_list, 0, sizeof(job_list));

    while (1) {
        threads_current -= mxq_mysql_finish_reaped_jobs(mysql, &job_list);
        assert(threads_current <= threads_max);
        assert(threads_current >= 0);

        if (threads_current == threads_max) {
            log_msg(0, "MAIN: waiting for tasks to finish (%d of %d running)\n", threads_current, threads_max);
            sleep(1);
            continue;
        }

        if (!job) {
            if (!(job = mxq_mysql_load_next_job(mysql, mxq_hostname(), arg_server_id))) {
                log_msg(0, "MAIN: action=wait_for_task slots_running=%d slots_available=%d  \n", threads_current, threads_max);
                sleep(1);
                continue;
            }
            log_msg(0, "job %d: job loaded..\n", job->job_id);
        }

        if (threads_current + job->job_threads > threads_max) {
            log_msg(0, "job_id=%d action=wait_for_slots slots_running=%d slots_available=%d slots_needed=%d slots_needed_by_task=%d\n",
                        job->job_id,
                        threads_current,
                        threads_max,
                        (threads_current + job->job_threads - threads_max),
                        job->job_threads);
            sleep(1);
            continue;
        }

        mxq_mysql_close(mysql);

        pid = fork();
        if (pid < 0) {
            perror("fork");
            return 1;
        } else if (pid == 0) {
            char **argv;
            FILE *fp;

            res = job_setup_environment(job);
            if (!res) {
                log_msg(0, "job_setup_environment failed\n");
                _exit(EX__MAX + 1);
            }

            argv = stringtostringvec(job->job_argc, job->job_argv);
            if (!argv) {
                perror("argv = stringtostringvec()");
                _exit(EX__MAX + 1);
            }
            log_msg(0, "job_id=%d action=delayed-execute command=%s threads=%d uid=%d gid=%d umask=%04o workdir=%s\n",
                        job->job_id, argv[0], job->job_threads, job->user_uid, job->user_gid, job->job_umask, job->job_workdir);

            log_msg(0, "job_id=%d action=redirect-stderr tmpstderr=%s\n", job->job_id, job->tmp_stderr);
            if (!streq(job->tmp_stderr, "/dev/null")) {
                res = unlink(job->tmp_stderr);
                if (res == -1 && errno != ENOENT) {
                    log_msg(0, "task=%d unlink(%s) failed (%s)\n", job->job_id, job->tmp_stderr, strerror(errno));
                    _exit(EX__MAX + 1);
                }
            }
            fh = open(job->tmp_stderr, O_WRONLY|O_CREAT|O_NOFOLLOW|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
            if (fh == -1) {
                log_msg(0, "task=%d open(%s) failed (%s)\n", job->job_id, job->tmp_stderr, strerror(errno));
                _exit(EX__MAX + 1);
            }
            if (fh != STDERR_FILENO) {
                res = dup2(fh, STDERR_FILENO);
                if (res == -1) {
                    log_msg(0, "task=%d dup2(fh=%d, %d) failed (%s)\n", job->job_id, fh, STDERR_FILENO, strerror(errno));
                    _exit(EX__MAX + 1);
                }
                res = close(fh);
                if (res == -1) {
                    log_msg(0, "task=%d close(fh=%d) failed (%s)\n", job->job_id, fh, strerror(errno));
                    _exit(EX__MAX + 1);
                }
            }

            if (!streq(job->tmp_stdout, job->tmp_stderr)) {
                log_msg(0, "task=%d action=redirect-stdout stdout=%s\n", job->job_id, job->tmp_stdout);

                if (!streq(job->tmp_stdout, "/dev/null")) {
                    res = unlink(job->tmp_stdout);
                    if (res == -1 && errno != ENOENT) {
                        log_msg(0, "task=%d unlink(%s) failed (%s)\n", job->job_id, job->tmp_stdout, strerror(errno));
                        _exit(EX__MAX + 1);
                    }
                }

                fh = open(job->tmp_stdout, O_WRONLY|O_CREAT|O_NOFOLLOW|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
                if (fh == -1) {
                    log_msg(0, "task=%d open(%s) failed (%s)\n", job->job_id, job->tmp_stdout, strerror(errno));
                    _exit(EX__MAX + 1);
                }
                if (fh != STDOUT_FILENO) {
                    res = dup2(fh, STDOUT_FILENO);
                    if (res == -1) {
                        log_msg(0, "task=%d dup2(fh=%d, %d) failed (%s)\n", job->job_id, fh, STDOUT_FILENO, strerror(errno));
                        _exit(EX__MAX + 1);
                    }
                    res = close(fh);
                    if (res == -1) {
    //                    log_msg(0, "task=%d close(fh=%d) failed (%s)\n", task->id, fh, strerror(errno));
                        _exit(EX__MAX + 1);
                    }
                }
            } else {
                log_msg(0, "task=%d action=redirect-stdout stdout=stderr(%s)\n", job->job_id, job->tmp_stdout);
                res = dup2(STDERR_FILENO, STDOUT_FILENO);
                if (res == -1) {
                    log_msg(0, "task=%d dup2(STDERR_FILENO=%d, STDOUT_FILENO=%d) failed (%s)\n", job->job_id, STDERR_FILENO, STDOUT_FILENO, strerror(errno));
                    _exit(EX__MAX + 1);
                }
            }

            execvp(argv[0], argv);
            log_msg(0, "task %d: execvp failed (%s)\n", job->job_id, strerror(errno));
            _exit(EX__MAX + 1);
        }

        job->job_status = 2;
        job->host_pid   = pid;

        gettimeofday(&job->stats_starttime, NULL);

        joblist_add_job(&job_list, job);

        threads_current += job->job_threads;

        mysql = mxq_mysql_connect(&mmysql);

        res = mxq_mysql_job_started(mysql, job->job_id, pid);
        if (res < 0) {
            return 1;
        }

        job = NULL;
    };

    log_msg(0, NULL);

    return 0;
}



