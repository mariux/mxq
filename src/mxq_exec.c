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
       " task_command," \
       " task_argc," \
       " task_argv," \
       " task_workdir," \
       " task_stdout," \
       " task_stderr," \
       " host_server_id," \
       " host_hostname," \
       " host_pid"


struct mxq_task_list_item *tasks = NULL;


struct mxq_task *mxq_mysql_row_to_task(MYSQL_ROW row)
{
    struct mxq_task *task;
    struct mxq_job  *job;
    
    job = calloc(1, sizeof(*job));
    if (!job) {
        return NULL;
    }

    task = calloc(1, sizeof(*task));
    if (!task) {
        free(job);
        return NULL;
    }

    task->job = job;
        
    job->id       = atoi(row[0]);
    job->jobname  = strdup(row[1]);
    job->status   = atoi(row[2]);
    job->priority = atoi(row[3]);
    job->uid      = atoi(row[4]);
    job->username = strdup(row[5]);

    if (!job->jobname || !job->username) {
        mxq_free_task(task);
        free(task);
        return NULL;
    }

    task->gid         = atoi(row[6]);
    task->groupname   = strdup(row[7]);
    task->id          = atoi(row[8]);
    task->status      = atoi(row[9]);
    task->priority    = atoi(row[10]);
    task->command     = strdup(row[11]);
    task->argc        = atoi(row[12]);
    task->argv        = strdup(row[13]);
    task->workdir     = strdup(row[14]);

    task->stdout = strdup(row[15]);
    if (streq(row[15], "/dev/null")) {
        task->stdouttmp = strdup(row[15]);
    } else {
        asprintf(&task->stdouttmp, "%s.%d.mxqtmp", row[15], task->id);
    }
    
    task->stderr = strdup(row[16]);
    if (streq(row[16], "/dev/null")) {
        task->stderrtmp = strdup(row[16]);
    } else {
        asprintf(&task->stderrtmp, "%s.%d.mxqtmp", row[16], task->id);
    }

    task->umask = atoi(row[17]);
    task->submit_host = strdup(row[18]);

    if (!task->groupname || !task->command || !task->argv ||
        !task->workdir   || !task->stdout  || !task->stderr ||
        !task->stdouttmp || !task->stderrtmp ||
        !task->submit_host) {
            mxq_free_task(task);
            free(task);
            return NULL;
    }

    //host->server_id = strdup(row[19]);
    //host->hostname  = strdup(row[20]);
    //host->pid       = atoi(row[21]);
    
    return task;
}

int mxq_mysql_select_next_task(MYSQL *mysql, struct mxq_task **task, char *hostname, char *serverid)
{
    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_rows;
    unsigned int num_fields;

    _cleanup_free_ char *q_hostname = NULL;
    _cleanup_free_ char *q_serverid = NULL;
    
    if (!(q_hostname = mxq_mysql_escape_string(mysql, hostname) )) return 0;
    if (!(q_serverid = mxq_mysql_escape_string(mysql, serverid) )) return 0;
    
    mres = mxq_mysql_query_with_result(mysql, "SELECT"
       " job_id,"
       " job_name,"
       " job_status,"
       " job_priority,"
       " user_uid,"
       " user_name,"
       " group_gid,"
       " group_name,"
       " task_id,"
       " task_status,"
       " task_priority,"
       " task_command,"
       " task_argc,"
       " task_argv,"
       " task_workdir,"
       " task_stdout,"
       " task_stderr,"
       " task_umask,"
       " host_submit_host,"
       " host_server_id,"
       " host_hostname,"
       " host_pid"
       " FROM v_tasks" 
       " WHERE host_hostname = '%s'"
       " AND host_server_id = '%s'"
       " AND host_pid IS NULL"
       " AND task_status = 1"
       " LIMIT 1",
       q_hostname, q_serverid);

    if (!mres) {
        log_msg(0, "mxq_mysql_select_next_task: Failed to query database: Error: %s\n", mysql_error(mysql));
        sleep(10);
        return -1;
    }

    num_rows = mysql_num_rows(mres);
    assert(num_rows <= 1);

    if (num_rows == 1) {
        num_fields = mysql_num_fields(mres);
        assert(num_fields == 22);

        row = mysql_fetch_row(mres);
        if (!row) {
            fprintf(stderr, "Failed to fetch row: Error: %s\n", mysql_error(mysql));
            return -1;
        }
        
        *task = mxq_mysql_row_to_task(row);
        
        if (!*task) {
            return -1;
        }

    }

    mysql_free_result(mres);

    return num_rows;
}


int mxq_mysql_task_started(MYSQL  *mysql, int task_id, int host_pid)
{
    assert(mysql);

    int   len;
    int   res;
    int   i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_fields;
    unsigned int num_rows;

    res = mxq_mysql_query(mysql, "UPDATE v_tasks SET"
             " host_pid = %d,"
             " task_status = 2"
             " WHERE task_id = %d"
             " AND   host_pid IS NULL",
             host_pid, task_id);
    if (res) {
        log_msg(0, "mxq_mysql_task_started: Failed to query database: Error(%d): %s\n", res, mysql_error(mysql));
        sleep(10);
        return -1;
    }

    return mysql_affected_rows(mysql);
}

int mxq_mysql_reserve_task(MYSQL  *mysql, char *hostname, char *server_id)
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

    res = mxq_mysql_query(mysql, "UPDATE v_tasks SET"
             " task_status = 1,"
             " host_hostname = '%s',"
             " host_server_id = '%s'"
             " WHERE task_status = 0"
             " AND host_hostname IS NULL"
             " AND host_server_id IS NULL"
             " AND host_pid IS NULL"
             " ORDER BY task_id"
             " LIMIT 1",
             q_hostname, q_server_id);
    if (res) {
        log_msg(0, "mxq_mysql_reserve_task: Failed to query database: Error: %s\n", mysql_error(mysql));
        sleep(10);
        return -1;
    }

    return mysql_affected_rows(mysql);
    
    return 0;
}

int mxq_mysql_finish_task(MYSQL  *mysql, struct mxq_task *task)
{
    assert(mysql);

    int   len;
    int   res;
    int   i;

    MYSQL_RES *mres;
    MYSQL_ROW  row;

    unsigned int num_fields;
    unsigned int num_rows;

    res = mxq_mysql_query(mysql, "UPDATE v_tasks SET"
             " task_status = %d"
             " WHERE task_status = 2"
             " AND task_id = %d",
             task->status, task->id);
    if (res) {
        log_msg(0, "mxq_mysql_finish_task: Failed to query database: Error: %s\n", mysql_error(mysql));
        sleep(10);
        return -1;
    }

    return mysql_affected_rows(mysql);
    
    return 0;
}


char *mxq_task_set_hostname(struct mxq_task *task)
{
    return NULL;
//    res = gethostname(hostname, 1024);
    //if (res == -1) {
      //  assert(errno == ENAMETOOLONG);
        //hostname[1024-1] = 0;
   // }
}
struct mxq_task *mxq_mysql_load_next_task(MYSQL  *mysql, char *hostname, char *server_id)
{
    struct mxq_task *task = NULL;
    int res;

    while (1) {
        res = mxq_mysql_select_next_task(mysql, &task, hostname, server_id);
        if (res < 0) {
            return NULL;
        } else if (res) {
            return task;
        }

        res = mxq_mysql_reserve_task(mysql, hostname, server_id);
        if (res < 1) {
            return NULL;
        }
    };
}

struct mxq_task *mxq_task_find_by_pid(pid_t pid)
{
    struct mxq_task_list_item *l;

    for (l = tasks; l ; l = l->next) {
        if (l->task->stats.pid == pid)
            return l->task;
    }

    return NULL;
}

static void child_handler(int sig) 
{
    pid_t pid;
    struct rusage rusage;
    int status;
    
    struct mxq_task *task;
    
    while ((pid = wait3(&status, WNOHANG, &rusage)) > 0) {
        
        task = mxq_task_find_by_pid(pid);
        
        assert(task);
        
        gettimeofday(&task->stats.elapsed, NULL);

        if (task->stats.elapsed.tv_usec < task->stats.starttime.tv_usec) {
            task->stats.elapsed.tv_usec += 1000000L;
            assert(task->stats.elapsed.tv_sec > 0);
            task->stats.elapsed.tv_sec -= 1;
        }
        task->stats.elapsed.tv_sec  -=  task->stats.starttime.tv_sec;
        task->stats.elapsed.tv_usec -=  task->stats.starttime.tv_usec;
        task->stats.exit_status = status;
        task->stats.rusage      = rusage;
        task->status            = 3;
    };
}

static void dummy_handler(int sig)
{
    return;
} 


struct mxq_task_list_item *add_task_to_tasklist(struct mxq_task *task)
{
    struct mxq_task_list_item *t;
    struct mxq_task_list_item *l;

    t = malloc(sizeof(*t));
    if (!t) {
        return NULL;
    }
    
    t->next = NULL;
    t->task = task;
    
    if (!tasks) {
        tasks = t;
    } else {
        for (l = tasks; l->next ; l = l->next);
        l->next = t;
    }

    return t;
}


void setup_reaper(void)
{
    struct sigaction sa;
    
    sigemptyset(&sa.sa_mask);

    sa.sa_flags = 0;
    sa.sa_handler = child_handler;

    sigaction(SIGCHLD, &sa, NULL);
    
    signal(SIGUSR1, dummy_handler);
}


#define MXQ_TOTIME(x) ((double)(( (double)(x).tv_sec*1000000L + (double)(x).tv_usec)/1000000L))

int mxq_mysql_finish_reaped_tasks(MYSQL *mysql)
{
    struct mxq_task *t;
    struct mxq_task_list_item *l;
    struct mxq_task_list_item *prev = NULL;
    struct mxq_task_list_item *next = NULL;
    int cnt = 0;
    int res;
    
    char *exit_status;
    int exit_code = -1;

    for (l = tasks; l;) {
        next = l->next;
        t = l->task;
        if (t->status >= 3) {
            if (!prev) {
                tasks = next;
            } else {
                prev->next = next;
            }
        
            log_msg(0, "task=%d action=finish-task pid=%d\n", t->id, t->stats.pid);       
        
            // finish_task 
            // update db stats and status..
        
            if (WIFEXITED(t->stats.exit_status)) {
                exit_status = "exited";
                exit_code = WEXITSTATUS(t->stats.exit_status);
            } else if(WIFSIGNALED(t->stats.exit_status)) {
                exit_status = "killed";
                exit_code = WTERMSIG(t->stats.exit_status);
            } else if(WIFSTOPPED(t->stats.exit_status)) {
                exit_status = "stopped";
                exit_code = WSTOPSIG(t->stats.exit_status);
            } else {
                assert(WIFCONTINUED(t->stats.exit_status));
                exit_status = "continued";
            }

            if (!streq(t->stdout, "/dev/null")) {
                res = rename(t->stdouttmp, t->stdout);
                if (res == -1) {
                    log_msg(0, "task=%d rename(%s, %s) failed (%s)\n", t->id, t->stdouttmp, t->stdout, strerror(errno));
                } else {
                    log_msg(0, "task=%d action=rename-stdout stdouttmp=%s stdout=%s\n", t->id, t->stdouttmp, t->stdout);
                }
            }
            if (!streq(t->stderr, "/dev/null") && !streq(t->stderr, t->stdout)) {
                res = rename(t->stderrtmp, t->stderr);
                if (res == -1) {
                    log_msg(0, "task=%d rename(%s, %s) failed (%s)\n", t->id, t->stderrtmp, t->stderr, strerror(errno));
                } else {
                    log_msg(0, "task=%d action=rename-stderr stdouttmp=%s stdout=%s\n", t->id, t->stderrtmp, t->stderr);
                }
            }
        
            mxq_mysql_finish_task(mysql, t);
        
            log_msg(0, "task=%d exit_status=%s exit_code=%d status=%d usr=%lf sys=%lf real=%lf \n", 
               t->id,
               exit_status,
               exit_code,
               MXQ_TOTIME(t->stats.rusage.ru_utime),
               MXQ_TOTIME(t->stats.rusage.ru_stime),
               MXQ_TOTIME(t->stats.elapsed)
               );
            cnt++;
            mxq_free_task(t);
            free(t);
            free(l);
            l = NULL;
        }
        
        prev = l;
        l = next;
    }

    return cnt;
}

int main(int argc, char *argv[])
{
    _cleanup_close_mysql_ MYSQL *mysql = NULL;

    struct mxq_task *task = NULL;

    struct mxq_mysql mmysql;
    int status;

    unsigned int num_rows;
    unsigned int num_fields;

    char *server_id = "localhost-1";
    
    struct timeval s1;
    struct timeval s2;

    int res;
    pid_t pid;

    mmysql.default_file  = MXQ_MYSQL_DEFAULT_FILE;
    mmysql.default_group = "mxq_submit";

    mysql = mxq_mysql_connect(&mmysql);

    setup_reaper();

    int i = 1000000;
    
    int threads_max     = 6;
    int threads_current = 0;
    
    int fh;
    
    while (1) {

        threads_current -= mxq_mysql_finish_reaped_tasks(mysql);

        if (! (i--)) break;

        if (threads_current == threads_max) {
            log_msg(0, "MAIN: waiting for tasks to finish (%d of %d running)\n", threads_current, threads_max);
            sleep(1);
            continue;
        }
        
        assert(threads_current <= threads_max);
        
        if (!task) {
            if (!(task = mxq_mysql_load_next_task(mysql, mxq_hostname(), server_id))) {
                log_msg(0, "MAIN: action=wait_for_task slots_running=%d slots_available=%d  \n", threads_current, threads_max);
                sleep(1);
                continue;
            }
            log_msg(0, "task %d: task loaded..\n", task->id);
        }
        
        if (threads_current + 3 > threads_max) {
            log_msg(0, "task=%d slots_running=%d slots_available=%d slots_needed=%d slots_needed_by_task=%d\n", 
                    task->id, 
                    threads_current, 
                    threads_max, 
                    (threads_current + 3 - threads_max),
                    3);
            sleep(1);
            continue;
        }

        pid = fork();
        if (pid < 0) {
            perror("fork");
            return 1;
        } else if (pid == 0) {
            char **argv;
            
            log_msg(0, "task=%d action=wait-for-parent ppid=%d\n", task->id, getppid());
            pause();
            
            mxq_mysql_close(mysql);
            
            res = setreuid(task->job->uid, task->job->uid);
            if (res == -1) {
                log_msg(0, "task %d: setreuid(%d, %d) failed (%s)\n", task->id, task->job->uid, task->job->uid, strerror(errno));
                _exit(EX__MAX + 1);
            }
            
            res = setregid(task->gid, task->gid);
            if (res == -1) {
                log_msg(0, "task %d: setregid(%d, %d) failed (%s)\n", task->id, task->gid, task->gid, strerror(errno));
                _exit(EX__MAX + 1);
            }
            
            res = chdir(task->workdir);
            if (res == -1) {
                log_msg(0, "task %d: chdir(%s) failed (%s)\n", task->id, task->workdir, strerror(errno));
                _exit(EX__MAX + 1);
            }
            
            argv = stringtostringvec(task->argc, task->argv);
            log_msg(0, "task=%d action=delayed-execute command=%s uid=%d gid=%d umask=%04o workdir=%s\n", task->id, argv[0], task->job->uid, task->gid, task->umask, task->workdir);
            umask(task->umask);

            log_msg(0, "task=%d action=redirect-stderr stderr=%s\n", task->id, task->stderrtmp);
            
            
            if (!streq(task->stderrtmp, "/dev/null")) {
                res = unlink(task->stderrtmp);
                if (res == -1 && errno != ENOENT) {
                    log_msg(0, "task=%d unlink(%s) failed (%s)\n", task->id, task->stderrtmp, strerror(errno));
                    _exit(EX__MAX + 1);
                }
            }
            fh = open(task->stderrtmp, O_WRONLY|O_CREAT|O_NOFOLLOW|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
            if (fh == -1) {
                log_msg(0, "task=%d open(%s) failed (%s)\n", task->id, task->stderrtmp, strerror(errno));
                _exit(EX__MAX + 1);
            }
            if (fh != STDERR_FILENO) {
                res = dup2(fh, STDERR_FILENO);
                if (res == -1) {
                    log_msg(0, "task=%d dup2(fh=%d, %d) failed (%s)\n", task->id, fh, STDERR_FILENO, strerror(errno));
                    _exit(EX__MAX + 1);
                }
                res = close(fh);
                if (res == -1) {
                    log_msg(0, "task=%d close(fh=%d) failed (%s)\n", task->id, fh, strerror(errno));
                    _exit(EX__MAX + 1);
            }
            }

            log_msg(0, "task=%d action=redirect-stdout stdout=%s\n", task->id, task->stdouttmp);

            if (!streq(task->stdouttmp, "/dev/null") && !streq(task->stdouttmp, task->stderrtmp)) {
                res = unlink(task->stdouttmp);
                if (res == -1 && errno != ENOENT) {
                    log_msg(0, "task=%d unlink(%s) failed (%s)\n", task->id, task->stdouttmp, strerror(errno));
                    _exit(EX__MAX + 1);
                }
            }

            fh = open(task->stdouttmp, O_WRONLY|O_CREAT|O_NOFOLLOW|O_TRUNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
            if (fh == -1) {
                log_msg(0, "task=%d open(%s) failed (%s)\n", task->id, task->stdouttmp, strerror(errno));
                _exit(EX__MAX + 1);
            }
            if (fh != STDOUT_FILENO) {
                res = dup2(fh, STDOUT_FILENO);
                if (res == -1) {
                    log_msg(0, "task=%d dup2(fh=%d, %d) failed (%s)\n", task->id, fh, STDOUT_FILENO, strerror(errno));
                    _exit(EX__MAX + 1);
            }
                res = close(fh);
                if (res == -1) {
//                    log_msg(0, "task=%d close(fh=%d) failed (%s)\n", task->id, fh, strerror(errno));
                    _exit(EX__MAX + 1);
                }
            }

            execvp(argv[0], argv);
            log_msg(0, "task %d: execvp failed (%s)\n", task->id, strerror(errno));
            _exit(EX__MAX + 1);
        }

        mxq_mysql_close(mysql);
        mysql = mxq_mysql_connect(&mmysql);
        
        threads_current++;

        gettimeofday(&task->stats.starttime, NULL);
        task->stats.pid = pid;
        task->status    = 2;

        add_task_to_tasklist(task);

        res = mxq_mysql_task_started(mysql, task->id, pid);
        if (res < 0) {
            return 1;
        }
        log_msg(0, "task=%d action=signal-child pid=%d signal=%s(%d)\n", task->id, pid, "SIGUSR1", SIGUSR1);
        kill(pid, SIGUSR1);
        
        task = NULL;
    };

    log_msg(0, NULL);

    return 0;
}



