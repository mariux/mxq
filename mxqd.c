
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <errno.h>


#include <sysexits.h>

#include <sys/file.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/prctl.h>

#include <signal.h>
#include <pwd.h>
#include <grp.h>

#include "bee_getopt.h"

#include "mx_flock.h"
#include "mx_util.h"

#include "mxq.h"
#include "mxq_group.h"
#include "mxq_job.h"
#include "mxq_mysql.h"
#include "mxqd.h"

#ifndef MXQ_VERSION
#define MXQ_VERSION "0.00"
#endif

#ifndef MXQ_VERSIONFULL
#define MXQ_VERSIONFULL "MXQ v0.00 super alpha 0"
#endif

#ifndef MXQ_VERSIONDATE
#define MXQ_VERSIONDATE "2015"
#endif

volatile sig_atomic_t global_sigint_cnt=0;
volatile sig_atomic_t global_sigterm_cnt=0;

int mxq_redirect_output(char *stdout_fname, char *stderr_fname);

static void print_version(void)
{
    printf(
    "mxqd - " MXQ_VERSIONFULL "\n"
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
    "  mxqd [options]\n"
    "\n"
    "options:\n"
    "  -j | --slots     <slots>           default: 1\n"
    "  -m | --memory    <memory>          default: 2048 (in MiB)\n"
    "  -x | --max-memory-per-slot <mem>   default: <memory>/<slots>\n"
    "\n"
    "  -N | --server-id <id>              default: main\n"
    "\n"
    "       --pid-file <pidfile>          default: create no pid file\n"
    "       --daemonize                   default: run in foreground\n"
    "       --no-log                      default: write a logfile\n"
    "\n"
    "  -V | --version\n"
    "  -h | --help\n"
    "\n"
    );
}

/**********************************************************************/
int setup_cronolog(char *cronolog, char *link, char *format)
{
    int res;
    int pipe_fd[2];
    int pid;

    res = pipe(pipe_fd);
    if (res == -1) {
        MXQ_LOG_ERROR("can't create pipe for cronolog: (%m)\n");
        return 0;
    }

    pid = fork();
    if (pid < 0) {
        MXQ_LOG_ERROR("cronolog fork failed: %m");
        return 1;
    } else if(pid == 0) {
        res = dup2(pipe_fd[0], STDIN_FILENO);
        if (res == -1) {
            MXQ_LOG_ERROR("dup2(fh=%d, %d) for cronolog stdin failed (%m)\n", pipe_fd[0], STDIN_FILENO);
            return 0;
        }
        close(pipe_fd[0]);
        close(pipe_fd[1]);

        mxq_redirect_output("/dev/null", "/dev/null");

        execl(cronolog, cronolog, "--link", link, format, NULL);
        MXQ_LOG_ERROR("execl('%s', ...) failed (%m)\n", cronolog);
        _exit(EX__MAX + 1);
    }

    res = dup2(pipe_fd[1], STDOUT_FILENO);
    if (res == -1) {
        MXQ_LOG_ERROR("dup2(fh=%d, %d) for cronolog stdout failed (%m)\n", pipe_fd[0], STDOUT_FILENO);
        return 0;
    }
    res = dup2(STDOUT_FILENO, STDERR_FILENO);
    if (res == -1) {
        MXQ_LOG_ERROR("dup2(fh=%d, %d) for cronolog stderr failed (%m)\n", STDOUT_FILENO, STDERR_FILENO);
        return 0;
    }
    close(pipe_fd[0]);
    close(pipe_fd[1]);

    return pid;
}


int setup_stdin(char *fname)
{
    int fh;
    int res;

    fh = open(fname, O_RDONLY|O_NOFOLLOW);
    if (fh == -1) {
        MXQ_LOG_ERROR("open(%s) for stdin failed (%m)\n", fname);
        return 0;
    }

    if (fh != STDIN_FILENO) {
        res = dup2(fh, STDIN_FILENO);
        if (res == -1) {
            MXQ_LOG_ERROR("dup2(fh=%d, %d) failed (%m)\n", fh, STDIN_FILENO);
            return 0;
        }
        res = close(fh);
        if (res == -1) {
            MXQ_LOG_ERROR("close(fh=%d) failed (%m)\n", fh);
            return 0;
        }
    }
    return 1;
}

int write_pid_to_file(char *fname)
{
    int fd;
    int res;

    fd = mx_open_newfile(fname);
    if (fd < 0)
        return fd;

    dprintf(fd, "%d\n", getpid());
    res = fsync(fd);
    if (res == -1)
        return -errno;

    close(fd);
    return 0;
}


int server_init(struct mxq_server *server, int argc, char *argv[])
{
    int res;
    char *arg_server_id = "main";
    char *arg_mysql_default_group;
    char *arg_mysql_default_file;
    char *arg_pidfile = NULL;
    char arg_daemonize = 0;
    char arg_nolog = 0;
    int opt;
    unsigned long threads_total = 1;
    unsigned long memory_total = 2048;
    unsigned long memory_max   = 0;
    int i;

    struct bee_getopt_ctl optctl;
    struct bee_option opts[] = {
                BEE_OPTION_NO_ARG("help",               'h'),
                BEE_OPTION_NO_ARG("version",            'V'),
                BEE_OPTION_NO_ARG("daemonize",            1),
                BEE_OPTION_NO_ARG("no-log",               3),
                BEE_OPTION_REQUIRED_ARG("pid-file",       2),
                BEE_OPTION_REQUIRED_ARG("slots",        'j'),
                BEE_OPTION_REQUIRED_ARG("memory",       'm'),
                BEE_OPTION_REQUIRED_ARG("max-memory-per-slot", 'x'),
                BEE_OPTION_REQUIRED_ARG("server-id",    'N'),
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
            case 1:
                arg_daemonize = 1;
                break;

            case 2:
                arg_pidfile = optctl.optarg;
                break;

            case 3:
                arg_nolog = 1;
                break;

            case 'V':
                print_version();
                exit(EX_USAGE);

            case 'h':
                print_usage();
                exit(EX_USAGE);

            case 'j':
                if (mx_strtoul(optctl.optarg, &threads_total) < 0) {
                    fprintf(stderr, "Error in --slots '%s': %m\n", optctl.optarg);
                    exit(1);
                }
                if (!threads_total)
                    threads_total = 1;
                break;

            case 'm':
                if (mx_strtoul(optctl.optarg, &memory_total) < 0) {
                    fprintf(stderr, "Error in --memory '%s': %m\n", optctl.optarg);
                    exit(1);
                }
                if (!memory_total)
                    memory_total = 2048;
                break;

            case 'x':
                if (mx_strtoul(optctl.optarg, &memory_max) < 0) {
                    fprintf(stderr, "Error in --max-memory-per-slot '%s': %m\n", optctl.optarg);
                    exit(1);
                }
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

    if (arg_daemonize && arg_nolog) {
        fprintf(stderr, "Error while using conflicting options --daemonize and --no-log at once.\n");
        exit(EX_USAGE);
    }

    if (getuid()) {
        fprintf(stderr, "Running mxqd as non-root user is not permitted.\n");
        exit(EX_USAGE);
    }

    memset(server, 0, sizeof(*server));

    server->mmysql.default_file  = arg_mysql_default_file;
    server->mmysql.default_group = arg_mysql_default_group;
    server->hostname = mxq_hostname();
    server->server_id = arg_server_id;

    server->flock = mx_flock(LOCK_EX, "/dev/shm/mxqd.%s.%s.lck", server->hostname, server->server_id);
    if (!server->flock) {
        return -1;
    }

    if (!server->flock->locked) {
        fprintf(stderr, "MXQ Server '%s' on host '%s' is already running. Exiting.\n", server->server_id, server->hostname);
        exit(2);
    }

    if (arg_daemonize) {
        res = daemon(0, 1);
        if (res == -1) {
            perror("daemon(0,1)");
            exit(EX_UNAVAILABLE);
        }
    }

    if (arg_pidfile) {
        res = write_pid_to_file(arg_pidfile);
        if (res < 0) {
            fprintf(stderr, "MAIN: pidfile (%s) setup failed: %m.  Exiting.\n", arg_pidfile);
            exit(EX_IOERR);
        }

        server->pidfilename = arg_pidfile;
    }

    res = prctl(PR_SET_CHILD_SUBREAPER, 1);
    if (res == -1) {
        fprintf(stderr, "MAIN: prctl(PR_SET_CHILD_SUBREAPER) setup failed: %m.  Exiting.\n");
        exit(EX_IOERR);
    }

    setup_stdin("/dev/null");

    if (!arg_nolog) {
        res = setup_cronolog("/usr/sbin/cronolog", "/var/log/mxqd_log", "/var/log/%Y/mxqd_log-%Y-%m");
        if (!res) {
            MXQ_LOG_ERROR("MAIN: cronolog setup failed. exiting.\n");
            exit(EX_IOERR);
        }
    }


    server->slots = threads_total;;
    server->memory_total = memory_total;
    server->memory_max_per_slot = memory_max;
    server->memory_avg_per_slot = (long double)server->memory_total / (long double)server->slots;

    if (server->memory_max_per_slot < server->memory_avg_per_slot)
       server->memory_max_per_slot = server->memory_avg_per_slot;

    if (server->memory_max_per_slot > server->memory_total)
       server->memory_max_per_slot = server->memory_total;


    return 1;
}

/**********************************************************************/

void group_init(struct mxq_group_list *group)
{
    struct mxq_server *s;
    struct mxq_group *g;

    long double memory_threads;
    long double memory_per_thread;
    long double memory_max_available;
    unsigned long slots_per_job;
    unsigned long jobs_max;
    unsigned long slots_max;
    unsigned long memory_max;

    assert(group);
    assert(group->user);
    assert(group->user->server);

    s = group->user->server;
    g = &group->group;

    memory_per_thread    = (long double)g->job_memory / (long double) g->job_threads;
    memory_max_available = (long double)s->memory_total * (long double)s->memory_max_per_slot / memory_per_thread;

    if (memory_max_available > s->memory_total)
        memory_max_available = s->memory_total;

    slots_per_job = ceill((long double)g->job_memory / s->memory_avg_per_slot);

    if (slots_per_job < g->job_threads)
       slots_per_job = g->job_threads;

    memory_threads = memory_max_available / memory_per_thread;

    if (memory_per_thread > s->memory_max_per_slot) {
        jobs_max = memory_threads + 0.5;
    } else if (memory_per_thread > s->memory_avg_per_slot) {
        jobs_max = memory_threads + 0.5;
    } else {
        jobs_max = s->slots;
    }
    jobs_max /= g->job_threads;

    slots_max = jobs_max * slots_per_job;
    memory_max = jobs_max * g->job_memory;

    if (group->memory_per_thread != memory_per_thread
       || group->memory_max_available != memory_max_available
       || group->memory_max_available != memory_max_available
       || group->slots_per_job != slots_per_job
       || group->jobs_max != jobs_max
       || group->slots_max != slots_max
       || group->memory_max != memory_max)
        MXQ_LOG_INFO("  group=%s(%u):%lu jobs_max=%lu slots_max=%lu memory_max=%lu slots_per_job=%lu :: group %sinitialized.\n",
                    g->user_name, g->user_uid, g->group_id, jobs_max, slots_max, memory_max, slots_per_job,
                    group->orphaned?"re":"");

    group->orphaned = 0;
    group->memory_per_thread = memory_per_thread;
    group->memory_max_available = memory_max_available;
    group->slots_per_job = slots_per_job;
    group->jobs_max = jobs_max;
    group->slots_max = slots_max;
    group->memory_max = memory_max;
}

/**********************************************************************/

struct mxq_job_list *server_remove_job_by_pid(struct mxq_server *server, pid_t pid)
{
    struct mxq_user_list  *user;
    struct mxq_group_list *group;
    struct mxq_job_list   *job, *prev;

    for (user=server->users; user; user=user->next) {
        for (group=user->groups; group; group=group->next) {
            for (job=group->jobs, prev=NULL; job; prev=job,job=job->next) {
                if (job->job.host_pid == pid) {
                    if (prev) {
                        prev->next = job->next;
                    } else {
                        assert(group->jobs);
                        assert(group->jobs == job);

                        group->jobs = job->next;
                    }

                    group->job_cnt--;
                    user->job_cnt--;
                    server->job_cnt--;

                    group->slots_running  -= job->job.host_slots;
                    user->slots_running   -= job->job.host_slots;
                    server->slots_running -= job->job.host_slots;

                    group->threads_running  -= group->group.job_threads;
                    user->threads_running   -= group->group.job_threads;
                    server->threads_running -= group->group.job_threads;

                    group->group.group_jobs_running--;

                    group->jobs_running--;
                    user->jobs_running--;
                    server->jobs_running--;

                    group->memory_used  -= group->group.job_memory;
                    user->memory_used   -= group->group.job_memory;
                    server->memory_used -= group->group.job_memory;

                    return job;
                }
            }
        }
    }
    return NULL;
}

/**********************************************************************/

struct mxq_user_list *user_list_find_uid(struct mxq_user_list *list, uint32_t  uid)
{
    struct mxq_user_list *u;

    for (u = list; u; u = u->next) {
        assert(u->groups);
        if (u->groups[0].group.user_uid == uid) {
            return u;
        }
    }
    return NULL;
}

/**********************************************************************/

struct mxq_group_list *group_list_find_group(struct mxq_group_list *list, struct mxq_group *group)
{
    struct mxq_group_list *g;

    assert(group);

    for (g = list; g; g = g->next) {
        if (g->group.group_id == group->group_id) {
            return g;
        }
    }
    return NULL;
}

/**********************************************************************/

struct mxq_job_list *group_add_job(struct mxq_group_list *group, struct mxq_job *job)
{
    struct mxq_job_list *j;
    struct mxq_job_list *jlist;

    struct mxq_server *server;
    struct mxq_user_list *user;

    struct mxq_group *mxqgrp;

    assert(group);
    assert(group->user);
    assert(group->user->server);
    assert(job->job_status == MXQ_JOB_STATUS_RUNNING);

    mxqgrp = &group->group;
    user   = group->user;
    server = user->server;

    j = calloc(1, sizeof(*j));
    if (!j) {
        return NULL;
    }
    jlist = group->jobs;

    memcpy(&j->job, job, sizeof(*job));

    j->group = group;
    j->next  = jlist;

    group->jobs = j;

    group->job_cnt++;
    user->job_cnt++;
    server->job_cnt++;

    group->slots_running  += group->slots_per_job;
    user->slots_running   += group->slots_per_job;
    server->slots_running += group->slots_per_job;

    group->threads_running  += mxqgrp->job_threads;
    user->threads_running   += mxqgrp->job_threads;
    server->threads_running += mxqgrp->job_threads;

    mxqgrp->group_jobs_running++;

    group->jobs_running++;
    user->jobs_running++;
    server->jobs_running++;

    group->memory_used += mxqgrp->job_memory;
    user->memory_used += mxqgrp->job_memory;
    server->memory_used += mxqgrp->job_memory;

    return j;
}
/**********************************************************************/

struct mxq_group_list *user_add_group(struct mxq_user_list *user, struct mxq_group *group)
{
    struct mxq_group_list *g;
    struct mxq_group_list *glist;

    assert(user);

    g = calloc(1, sizeof(*g));
    if (!g) {
        return NULL;
    }
    glist = user->groups;

    memcpy(&g->group, group, sizeof(*group));

    g->user = user;
    g->next = glist;

    user->groups = g;
    user->group_cnt++;

    assert(user->server);
    user->server->group_cnt++;

    group_init(g);

    return g;
}

/**********************************************************************/

struct mxq_group_list *server_add_user(struct mxq_server *server, struct mxq_group *group)
{
    struct mxq_user_list  *user;
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;

    assert(server);
    assert(group);

    user = calloc(1, sizeof(*user));
    if (!user)
        return NULL;

    user->server = server;

    glist = user_add_group(user, group);
    if (!glist) {
        free(user);
        return NULL;
    }

    ulist = server->users;

    user->next    = ulist;

    server->users = user;
    server->user_cnt++;

    return glist;
}

/**********************************************************************/

struct mxq_group_list *user_update_groupdata(struct mxq_user_list *user, struct mxq_group *group)
{
    struct mxq_group_list *glist;

    glist = group_list_find_group(user->groups, group);
    if (!glist) {
        return user_add_group(user, group);
    }

    mxq_group_free_content(&glist->group);
    memcpy(&glist->group, group, sizeof(*group));

    group_init(glist);

    return glist;
}

/**********************************************************************/

static struct mxq_group_list *server_update_groupdata(struct mxq_server *server, struct mxq_group *group)
{
    struct mxq_user_list *user;

    user = user_list_find_uid(server->users, group->user_uid);
    if (!user) {
        return server_add_user(server, group);
    }

    return user_update_groupdata(user, group);
}

static int init_child_process(struct mxq_group_list *group, struct mxq_job *j)
{
    struct mxq_group *g;
    struct mxq_server *s;
    struct passwd *passwd;
    pid_t pid;
    int res;
    int fh;

    assert(j);
    assert(group);
    assert(group->user);
    assert(group->user->server);

    s = group->user->server;
    g = &group->group;

    /** restore signal handler **/
    signal(SIGINT,  SIG_DFL);
    signal(SIGTERM, SIG_DFL);
    signal(SIGQUIT, SIG_DFL);
    signal(SIGHUP,  SIG_DFL);
    signal(SIGTSTP, SIG_DFL);
    signal(SIGTTIN, SIG_DFL);
    signal(SIGTTOU, SIG_DFL);
    signal(SIGCHLD, SIG_DFL);

    /* reset SIGPIPE which seems to be ignored by mysqlclientlib (?) */
    signal(SIGPIPE, SIG_DFL);

    /** set sessionid and pgrp leader **/
    pid = setsid();
    if (pid == -1) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu setsid(): %m\n",
            g->user_name, g->user_uid, g->group_id, j->job_id);
    }

    passwd = getpwuid(g->user_uid);
    if (!passwd) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu getpwuid(): %m\n",
            g->user_name, g->user_uid, g->group_id, j->job_id);
        return 0;
    }

    if (!streq(passwd->pw_name, g->user_name)) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu user_uid=%d does not map to user_name=%s but to pw_name=%s: Invalid user mapping\n",
            g->user_name, g->user_uid, g->group_id, j->job_id,
            g->user_uid, g->user_name, passwd->pw_name);
        return 0;
    }


    /** prepare environment **/

    res = clearenv();
    if (res != 0) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu clearenv(): %m\n",
            g->user_name, g->user_uid, g->group_id, j->job_id);
        return 0;
    }

    mx_setenv_forever("USER",     g->user_name);
    mx_setenv_forever("USERNAME", g->user_name);
    mx_setenv_forever("LOGNAME",  g->user_name);
    mx_setenv_forever("PATH",     MXQ_INITIAL_PATH);
    mx_setenv_forever("PWD",      j->job_workdir);
    mx_setenv_forever("HOME",     passwd->pw_dir);
    mx_setenv_forever("SHELL",    passwd->pw_shell);
    mx_setenv_forever("HOSTNAME", mxq_hostname());
    mx_setenvf_forever("JOB_ID",      "%lu",    j->job_id);
    mx_setenvf_forever("MXQ_JOBID",   "%lu",    j->job_id);
    mx_setenvf_forever("MXQ_THREADS", "%d",     g->job_threads);
    mx_setenvf_forever("MXQ_SLOTS",   "%lu",    group->slots_per_job);
    mx_setenvf_forever("MXQ_MEMORY",  "%lu",    g->job_memory);
    mx_setenvf_forever("MXQ_TIME",    "%d",     g->job_time);
    mx_setenvf_forever("MXQ_HOSTID",  "%s::%s", s->hostname, s->server_id);

    fh = open("/proc/self/loginuid", O_WRONLY|O_TRUNC);
    if (fh == -1) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu open(%s) failed: %m\n",
            g->user_name, g->user_uid, g->group_id, j->job_id, "/proc/self/loginuid");
        return 0;
    }
    dprintf(fh, "%d", g->user_uid);
    close(fh);

    res = initgroups(passwd->pw_name, g->user_gid);
    if (res == -1) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu initgroups() failed: %m\n",
            g->user_name, g->user_uid, g->group_id, j->job_id);
        return 0;
    }

    res = setregid(g->user_gid, g->user_gid);
    if (res == -1) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu setregid(%d, %d) failed: %m\n",
            g->user_name, g->user_uid, g->group_id, j->job_id,
            g->user_gid, g->user_gid);
        return 0;
    }

    res = setreuid(g->user_uid, g->user_uid);
    if (res == -1) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu setreuid(%d, %d) failed: %m\n",
            g->user_name, g->user_uid, g->group_id, j->job_id,
            g->user_uid, g->user_uid);
        return 0;
    }

    res = chdir(j->job_workdir);
    if (res == -1) {
        MXQ_LOG_ERROR("job=%s(%d):%lu:%lu chdir(%s) failed: %m\n",
            g->user_name, g->user_uid, g->group_id, j->job_id,
            j->job_workdir);
        return 0;
    }

    umask(j->job_umask);

    return 1;
}

/**********************************************************************/

int mxq_redirect_open(char *fname)
{
    int fh;
    int res;

    int    flags = O_WRONLY|O_CREAT|O_NOFOLLOW|O_TRUNC;
    mode_t mode  = S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH;


    if (!fname) {
        fname = "/dev/null";
    } else if (!streq(fname, "/dev/null")) {
        res = unlink(fname);
        if (res == -1 && errno != ENOENT) {
            MXQ_LOG_ERROR("unlink() failed: %m\n");
           return -2;
        }
        flags |= O_EXCL;
    }

    fh = open(fname, flags, mode);
    if (fh == -1) {
            MXQ_LOG_ERROR("open() failed: %m\n");
    }

    return fh;

}

int mxq_redirect(char *fname, int fd)
{
    int fh;
    int res;

    fh = mxq_redirect_open(fname);
    if (fh < 0)
        return -1;

    res = mx_dup2_close_both(fh, fd);
    if (res < 0)
        return -2;

    return 0;
}

int mxq_redirect_output(char *stdout_fname, char *stderr_fname)
{
    int res;

    res = mxq_redirect(stderr_fname, STDERR_FILENO);
    if (res < 0) {
        return -1;
    }

    if (stdout_fname == stderr_fname) {
        res = mx_dup2_close_new(STDERR_FILENO, STDOUT_FILENO);
        if( res < 0) {
            return -2;
        }
        return 0;
    }

    res = mxq_redirect(stdout_fname, STDOUT_FILENO);
    if (res < 0) {
        return -3;
    }

    return 0;
}

int mxq_redirect_input(char *stdin_fname)
{
    int fh;
    int res;

    fh = open(stdin_fname, O_RDONLY|O_NOFOLLOW);
    if (fh == -1) {
        MXQ_LOG_ERROR("open() failed: %m\n");
        return -1;
    }

    res = mx_dup2_close_both(fh, STDIN_FILENO);
    if (res < 0) {
        return -2;
    }

    return 1;
}


unsigned long start_job(struct mxq_group_list *group)
{
    struct mxq_server *server;
    struct mxq_job mxqjob;
    struct mxq_job_list *job;
    pid_t pid;
    int res;
    char **argv;

    assert(group);
    assert(group->user);
    assert(group->user->server);

    server = group->user->server;

    res = mxq_job_load(server->mysql, &mxqjob, group->group.group_id, server->hostname, server->server_id);

    if (!res) {
        return 0;
    }
    MXQ_LOG_INFO("   job=%s(%d):%lu:%lu :: new job loaded.\n",
            group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob.job_id);

    mxq_mysql_close(server->mysql);

    pid = fork();
    if (pid < 0) {
        MXQ_LOG_ERROR("fork: %m");
        return 0;
    } else if (pid == 0) {
        mxqjob.host_pid = getpid();

        MXQ_LOG_INFO("   job=%s(%d):%lu:%lu host_pid=%d pgrp=%d :: new child process forked.\n",
            group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob.job_id,
            mxqjob.host_pid, getpgrp());

        res = init_child_process(group, &mxqjob);
        if (!res)
            exit(1);

        mxq_job_set_tmpfilenames(&group->group, &mxqjob);


        res = mxq_redirect_input("/dev/null");
        if (res < 0) {
            MXQ_LOG_ERROR("   job=%s(%d):%lu:%lu mxq_redirect_input() failed (%d): %m\n",
                group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob.job_id,
                res);
            _exit(EX__MAX + 1);
        }

        res = mxq_redirect_output(mxqjob.tmp_stdout, mxqjob.tmp_stderr);
        if (res < 0) {
            MXQ_LOG_ERROR("   job=%s(%d):%lu:%lu mxq_redirect_output() failed (%d): %m\n",
                group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob.job_id,
                res);
            _exit(EX__MAX + 1);
        }


        char **argv = str_to_strvec(mxqjob.job_argv_str);
        execvp(argv[0], argv);
        MXQ_LOG_ERROR("execvp: %m");
        exit(1);
    }

    gettimeofday(&mxqjob.stats_starttime, NULL);

    server->mysql = mxq_mysql_connect(&server->mmysql);

    mxqjob.host_pid = pid;
    mxqjob.host_slots = group->slots_per_job;
    res = mxq_job_update_status_running(server->mysql, &mxqjob);
    if (res <= 0) {
        perror("mxq_job_update_status_running()\n");
    }

    do {
        job = group_add_job(group, &mxqjob);
    } while (!job);

    MXQ_LOG_INFO("   job=%s(%d):%lu:%lu :: added running job to watch queue.\n",
        group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob.job_id);

    return 1;
}

/**********************************************************************/

unsigned long start_user(struct mxq_user_list *user, int job_limit, long slots_to_start)
{
    struct mxq_server *server;
    struct mxq_group_list *group;
    struct mxq_group_list *gnext = NULL;
    struct mxq_group *mxqgrp;

    unsigned int prio;
    unsigned char started = 0;
    unsigned long slots_started = 0;
    int jobs_started = 0;

    assert(user);
    assert(user->server);
    assert(user->groups);

    server = user->server;
    group  = user->groups;
    mxqgrp = &group->group;

    prio = mxqgrp->group_priority;

    assert(slots_to_start <= server->slots - server->slots_running);

//    MXQ_LOG_INFO(" user=%s(%d) slots_to_start=%ld job_limit=%d :: trying to start jobs for user.\n",
//            mxqgrp->user_name, mxqgrp->user_uid, slots_to_start, job_limit);

    for (group=user->groups; group && slots_to_start > 0 && (!job_limit || jobs_started < job_limit); group=gnext) {

        mxqgrp  = &group->group;

        assert(group->jobs_running <= mxqgrp->group_jobs);
        assert(group->jobs_running <= group->jobs_max);

        if (group->jobs_running == mxqgrp->group_jobs) {
            gnext = group->next;
            if (!gnext && started) {
                gnext = group->user->groups;
                started = 0;
            }
            continue;
        }

        if (group->jobs_running == group->jobs_max) {
            gnext = group->next;
            if (!gnext && started) {
                gnext = group->user->groups;
                started = 0;
            }
            continue;
        }

        if (mxqgrp->group_jobs-mxqgrp->group_jobs_failed-mxqgrp->group_jobs_finished-mxqgrp->group_jobs_running == 0) {
            gnext = group->next;
            if (!gnext && started) {
                gnext = group->user->groups;
                started = 0;
            }
            continue;
        }

        if (group->slots_per_job > slots_to_start) {
            gnext = group->next;
            if (!gnext && started) {
                gnext = group->user->groups;
                started = 0;
            }
            continue;
        }

        if (mxqgrp->group_priority < prio) {
            if (started) {
                gnext = group->user->groups;
                started = 0;
                continue;
            }
            prio = mxqgrp->group_priority;
        }
        MXQ_LOG_INFO("  group=%s(%d):%lu slots_to_start=%ld slots_per_job=%lu :: trying to start job for group.\n",
                mxqgrp->user_name, mxqgrp->user_uid, mxqgrp->group_id, slots_to_start, group->slots_per_job);

        if (start_job(group)) {

            slots_to_start -= group->slots_per_job;
            jobs_started++;
            slots_started += group->slots_per_job;

            started = 1;
        }

        gnext = group->next;
        if (!gnext && started) {
            gnext = group->user->groups;
            started = 0;
        }
    }
    return slots_started;
}

/**********************************************************************/

unsigned long start_users(struct mxq_server *server)
{
    long slots_to_start;
    unsigned long slots_started;
    int started = 0;
    unsigned long slots_started_total = 0;

    struct mxq_user_list  *user, *unext=NULL;
    struct mxq_group_list *group, *gnext;

    assert(server);

    if (!server->user_cnt)
        return 0;

    //MXQ_LOG_INFO("=== starting jobs on free_slots=%lu slots for user_cnt=%lu users\n", server->slots - server->slots_running, server->user_cnt);

    for (user=server->users; user; user=user->next) {

        slots_to_start = server->slots / server->user_cnt - user->slots_running;

        if (slots_to_start < 0)
            continue;

        if (server->slots - server->slots_running < slots_to_start)
            slots_to_start = server->slots - server->slots_running;

        slots_started = start_user(user, 0, slots_to_start);
        slots_started_total += slots_started;
    }

    for (user=server->users; user && server->slots - server->slots_running; user=unext) {
        slots_to_start = server->slots - server->slots_running;
        slots_started  = start_user(user, 1, slots_to_start);
        slots_started_total += slots_started;
        started = (started || slots_started);

        unext = user->next;
        if (!unext && started) {
            unext = server->users;
            started = 0;
        }
    }

    return slots_started_total;
}

/**********************************************************************/

int remove_orphaned_groups(struct mxq_server *server)
{
    struct mxq_user_list  *user,  *unext, *uprev;
    struct mxq_group_list *group, *gnext, *gprev;
    struct mxq_job_list   *job;
    int cnt=0;

    for (user=server->users, uprev=NULL; user; user=unext) {
        unext = user->next;
        for (group=user->groups, gprev=NULL; group; group=gnext) {
            gnext = group->next;

            if (group->job_cnt) {
                gprev = group;
                continue;
            }

            assert(!group->jobs);

            if (!group->orphaned && group->group.group_jobs-group->group.group_jobs_failed-group->group.group_jobs_finished) {
                group->orphaned = 1;
                gprev = group;
                continue;
            }

            if (gprev) {
                gprev->next = gnext;
            } else {
                assert(group == user->groups);
                user->groups = gnext;
            }

            MXQ_LOG_INFO("group=%s(%d):%lu : Removing orphaned group.\n", group->group.user_name, group->group.user_uid, group->group.group_id);

            user->group_cnt--;
            server->group_cnt--;
            cnt++;
            mxq_group_free_content(&group->group);
            free_null(group);
        }
        if(user->groups) {
            uprev = user;
            continue;
        }

        if (uprev) {
            uprev->next = unext;
        } else {
            assert(user == server->users);
            server->users = unext;
        }
        server->user_cnt--;
        free_null(user);

        MXQ_LOG_INFO("Removed orphaned user. %lu users left.\n", server->user_cnt);
    }
    return cnt;
}

void server_dump(struct mxq_server *server)
{
    struct mxq_user_list  *user;
    struct mxq_group_list *group;
    struct mxq_job_list   *job;

    if (!server->user_cnt)
        return;

    MXQ_LOG_INFO("====================== SERVER DUMP START ======================\n");
    for (user=server->users; user; user=user->next) {
        MXQ_LOG_INFO("    user=%s(%d) slots_running=%lu\n",
            user->groups->group.user_name, user->groups->group.user_uid,
            user->slots_running);
        for (group=user->groups; group; group=group->next) {
            MXQ_LOG_INFO("        group=%s(%d):%lu %s jobs_in_q=%lu\n",
                group->group.user_name, group->group.user_uid, group->group.group_id,
                group->group.group_name, group->group.group_jobs-group->group.group_jobs_failed-group->group.group_jobs_finished-group->group.group_jobs_running);
            for (job=group->jobs; job; job=job->next) {
                MXQ_LOG_INFO("            job=%s(%d):%lu:%lu %s\n",
                    group->group.user_name, group->group.user_uid, group->group.group_id, job->job.job_id,
                    job->job.job_argv_str);
            }
        }
    }

    MXQ_LOG_INFO("memory_used=%lu memory_total=%lu\n", server->memory_used, server->memory_total);
    MXQ_LOG_INFO("slots_running=%lu slots=%lu threads_running=%lu jobs_running=%lu\n", server->slots_running, server->slots, server->threads_running, server->jobs_running);
    MXQ_LOG_INFO("====================== SERVER DUMP END ======================\n");
}

void server_close(struct mxq_server *server)
{
    struct mxq_user_list  *user,  *unext;
    struct mxq_group_list *group, *gnext;
    struct mxq_job_list   *job,   *jnext;

    for (user=server->users; user; user=unext) {
        for (group=user->groups; group; group=gnext) {
            for (job=group->jobs; job; job=jnext) {
                jnext = job->next;
                mxq_job_free_content(&job->job);
                free(job);
            }
            gnext = group->next;
            mxq_group_free_content(&group->group);
            free(group);
        }
        unext = user->next;
        free(user);
    }

    if (server->pidfilename)
        unlink(server->pidfilename);

    mx_funlock(server->flock);
}

int killall(struct mxq_server *server, int sig, unsigned int pgrp)
{
    struct mxq_user_list  *user;
    struct mxq_group_list *group;
    struct mxq_job_list   *job;
    pid_t pid;

    assert(server);

    for (user=server->users; user; user=user->next) {
        for (group=user->groups; group; group=group->next) {
            for (job=group->jobs; job; job=job->next) {
                pid = job->job.host_pid;
                if (pgrp)
                    pid = -pid;
                MXQ_LOG_INFO("Sending signal=%d to job=%s(%d):%lu:%lu %s=%d\n",
                    sig,
                    group->group.user_name, group->group.user_uid, group->group.group_id, job->job.job_id,
                    pgrp?"pgrp":"pid", pid);
                kill(pid, sig);
            }
        }
    }
    return 0;
}

int catchall(struct mxq_server *server) {

    struct rusage rusage;
    struct timeval now;
    int status;
    pid_t pid;
    int cnt = 0;
    struct mxq_job_list *job;
    struct mxq_job *j;
    struct mxq_group *g;
    int res;

    while (1) {
        siginfo_t siginfo;

        siginfo.si_pid = 0;
        res = waitid(P_ALL, 0, &siginfo, WEXITED|WNOHANG|WNOWAIT);

        if (res == -1) {
            MXQ_LOG_ERROR("waitid: %m\n");
            return -1;
        }

        /* no childs changed state => return */
        if (res == 0 && siginfo.si_pid == 0)
            return 0;

        assert(siginfo.si_pid > 1);

        job = server_remove_job_by_pid(server, siginfo.si_pid);
        if (!job) {
            MXQ_LOG_ERROR("unknown pid returned.. si_pid=%d si_uid=%d si_code=%d si_status=%d getpgid(si_pid)=%d getsid(si_pid)=%d\n",
                siginfo.si_pid, siginfo.si_uid, siginfo.si_code, siginfo.si_status,
                getpgid(siginfo.si_pid), getsid(siginfo.si_pid));
            pid = waitpid(siginfo.si_pid, &status, WNOHANG);
            if (pid != siginfo.si_pid)
                MXQ_LOG_ERROR("FIX ME BUG!!! pid=%d errno=%d (%m)\n", pid, errno);
            continue;
        }
        /* valid job returned.. */

        /* kill possible leftovers with SIGKILL */
        res = kill(-siginfo.si_pid, SIGKILL);
        if (res == -1)
            MXQ_LOG_ERROR("kill process group pgrp=%d failed: %m\n", -siginfo.si_pid);

        /* reap child and save new state */
        pid = wait4(siginfo.si_pid, &status, WNOHANG, &rusage);

        if (pid == -1) {
            MXQ_LOG_ERROR("wait4: %m\n");
            return -1;
        }

        if (pid == 0) {
            MXQ_LOG_ERROR("wait4: spurious pid=%d. Continuing anyway. Please FIX.\n", siginfo.si_pid);
            pid = siginfo.si_pid;
        }

        assert(pid == siginfo.si_pid);

        gettimeofday(&now, NULL);

        j = &job->job;
        assert(job->group);
        g = &job->group->group;

        timersub(&now, &j->stats_starttime, &j->stats_realtime);

        j->stats_status = status;
        j->stats_rusage = rusage;

        MXQ_LOG_INFO("   job=%s(%d):%lu:%lu host_pid=%d stats_status=%d :: child process returned.\n",
                g->user_name, g->user_uid, g->group_id, j->job_id, pid, status);

        mxq_job_update_status_exit(server->mysql, j);

        if (j->job_status == MXQ_JOB_STATUS_FINISHED) {
            g->group_jobs_finished++;
        } else if(j->job_status == MXQ_JOB_STATUS_FAILED) {
            g->group_jobs_failed++;
        } else if(j->job_status == MXQ_JOB_STATUS_KILLED) {
            g->group_jobs_failed++;
        }

        mxq_job_set_tmpfilenames(g, j);

        if (!streq(j->job_stdout, "/dev/null")) {
            res = rename(j->tmp_stdout, j->job_stdout);
            if (res == -1) {
                MXQ_LOG_ERROR("   job=%s(%d):%lu:%lu host_pid=%d :: rename(stdout) failed: %m\n",
                        g->user_name, g->user_uid, g->group_id, j->job_id, pid);
            }
        }

        if (!streq(j->job_stderr, "/dev/null") && !streq(j->job_stderr, j->job_stdout)) {
            res = rename(j->tmp_stderr, j->job_stderr);
            if (res == -1) {
                MXQ_LOG_ERROR("   job=%s(%d):%lu:%lu host_pid=%d :: rename(stderr) failed: %m\n",
                        g->user_name, g->user_uid, g->group_id, j->job_id, pid);
            }
        }

        cnt += job->group->slots_per_job;
        mxq_job_free_content(j);
        free(job);
    }

    return cnt;
}

int load_groups(struct mxq_server *server) {
    struct mxq_group *mxqgroups;
    struct mxq_group_list *group;
    int group_cnt;
    int total;
    int i;

    group_cnt = mxq_group_load_groups(server->mysql, &mxqgroups);

    for (i=0, total=0; i<group_cnt; i++) {
        group = server_update_groupdata(server, &mxqgroups[group_cnt-i-1]);
        if (!group) {
            MXQ_LOG_ERROR("Could not add Group to control structures.\n");
        } else {
            total++;
        }
    }
    free(mxqgroups);

    remove_orphaned_groups(server);

    return total;
}

/**********************************************************************/
static void no_handler(int sig) {}

static void sig_handler(int sig)
{
    if (sig == SIGINT) {
      global_sigint_cnt++;
      return;
    }

    if (sig == SIGTERM) {
      global_sigterm_cnt++;
      return;
    }
}

int main(int argc, char *argv[])
{
    struct mxq_group *mxqgroups;

    int group_cnt;

    struct mxq_server server;
    struct mxq_group_list *group;

    unsigned long slots_started  = 0;
    unsigned long slots_returned = 0;

    int i;
    int res;

    /*** server init ***/

    res = server_init(&server, argc, argv);
    if (res < 0) {
        MXQ_LOG_ERROR("MXQ Server: Can't initialize server handle. Exiting.\n");
        exit(1);
    }

    MXQ_LOG_INFO("mxqd - " MXQ_VERSIONFULL "\n");
    MXQ_LOG_INFO("  by Marius Tolzmann <tolzmann@molgen.mpg.de> " MXQ_VERSIONDATE "\n");
    MXQ_LOG_INFO("  Max Planck Institute for Molecular Genetics - Berlin Dahlem\n");
    MXQ_LOG_INFO("hostname=%s server_id=%s :: MXQ server started.\n", server.hostname, server.server_id);
    MXQ_LOG_INFO("slots=%lu memory_total=%lu memory_avg_per_slot=%.0Lf memory_max_per_slot=%ld :: server initialized.\n",
                  server.slots, server.memory_total, server.memory_avg_per_slot, server.memory_max_per_slot);

    /*** database connect ***/

    server.mysql = mxq_mysql_connect(&server.mmysql);

    /*** main loop ***/

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGQUIT, SIG_IGN);
    signal(SIGHUP,  SIG_IGN);
    signal(SIGTSTP, SIG_IGN);
    signal(SIGTTIN, SIG_IGN);
    signal(SIGTTOU, SIG_IGN);
    signal(SIGCHLD, no_handler);

    do {
        slots_returned = catchall(&server);
        if (slots_returned)
            MXQ_LOG_INFO("slots_returned=%lu :: Main Loop freed %lu slots.\n", slots_returned, slots_returned);

        if (slots_started || slots_returned) {
            server_dump(&server);
        }

        group_cnt = load_groups(&server);
//        if (group_cnt)
//            MXQ_LOG_INFO("group_cnt=%d :: %d Groups loaded\n", group_cnt, group_cnt);

        if (!server.group_cnt) {
            assert(!server.jobs_running);
            assert(!group_cnt);
            MXQ_LOG_INFO("Nothing to do. Sleeping for a short while. (1 second)\n");
            sleep(1);
            continue;
        }

        if (server.slots_running == server.slots) {
            MXQ_LOG_INFO("All slots running. Sleeping for a short while (7 seconds).\n");
            sleep(7);
            continue;
        }

        slots_started = start_users(&server);
        if (slots_started)
            MXQ_LOG_INFO("slots_started=%lu :: Main Loop started %lu slots.\n", slots_started, slots_started);

        if (!slots_started && !slots_returned) {
            if (!server.jobs_running) {
                MXQ_LOG_INFO("Tried Hard and nobody is doing anything. Sleeping for a long while (15 seconds).\n");
                sleep(15);
            } else {
                MXQ_LOG_INFO("Tried Hard. But have done nothing. Sleeping for a very short while.\n");
                sleep(3);
            }
            continue;
        }
    } while (!global_sigint_cnt && !global_sigterm_cnt);

    /*** clean up ***/

    MXQ_LOG_INFO("global_sigint_cnt=%d global_sigterm_cnt=%d : Exiting.\n", global_sigint_cnt, global_sigterm_cnt);

    while (server.jobs_running) {
        slots_returned = catchall(&server);
        if (slots_returned) {
           MXQ_LOG_INFO("jobs_running=%lu slots_returned=%lu global_sigint_cnt=%d global_sigterm_cnt=%d : \n",
                   server.jobs_running, slots_returned, global_sigint_cnt, global_sigterm_cnt);
           continue;
        }
        if (global_sigint_cnt)
            killall(&server, SIGTERM, 0);

        MXQ_LOG_INFO("jobs_running=%lu global_sigint_cnt=%d global_sigterm_cnt=%d : Exiting. Wating for jobs to finish. Sleeping for a while.\n",
              server.jobs_running, global_sigint_cnt, global_sigterm_cnt);
        sleep(1);
    }

    mxq_mysql_close(server.mysql);

    server_close(&server);

    MXQ_LOG_INFO("cu, mx.\n");

    log_msg(0, NULL);

    return 0;
}
