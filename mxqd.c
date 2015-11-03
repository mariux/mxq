
#define _GNU_SOURCE

#define MXQ_TYPE_SERVER

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include <sched.h>
#include <ctype.h>

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

#include "mx_getopt.h"

#include "mx_flock.h"
#include "mx_util.h"
#include "mx_log.h"

#include "mxq_group.h"
#include "mxq_job.h"
#include "mx_mysql.h"
#include "mx_proc.h"
#include "mxqd.h"
#include "mxq.h"

#ifndef MXQ_INITIAL_PATH
#  define MXQ_INITIAL_PATH      "/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
#endif

#ifndef MXQ_INITIAL_TMPDIR
#  define MXQ_INITIAL_TMPDIR    "/tmp"
#endif

#define RUNNING_AS_ROOT (getuid() == 0)

volatile sig_atomic_t global_sigint_cnt=0;
volatile sig_atomic_t global_sigterm_cnt=0;
volatile sig_atomic_t global_sigquit_cnt=0;

int mxq_redirect_output(char *stdout_fname, char *stderr_fname);

static void print_usage(void)
{
    mxq_print_generic_version();
    printf(
    "\n"
    "Usage:\n"
    "  %s [options]\n"
    "\n"
    "options:\n"
    "  -j, --slots     <slots>           default: depends on number of cores\n"
    "  -m, --memory    <memory>          default: 2G\n"
    "  -x, --max-memory-per-slot <mem>   default: <memory>/<slots>\n"
    "\n"
    "  -N, --server-id <id>              default: main\n"
    "      --hostname <hostname>         default: $(hostname)\n"
    "\n"
    "      --pid-file <pidfile>          default: create no pid file\n"
    "      --daemonize                   default: run in foreground\n"
#ifdef MXQ_DEVELOPMENT
    "      --log        default (in development): write no logfile\n"
#else
    "      --no-log                      default: write a logfile\n"
#endif
    "      --log-directory <logdir>      default: " MXQ_LOGDIR "\n"
    "      --debug                       default: info log level\n"
    "\n"
    "      --recover-only  (recover from crash and exit)\n"
    "\n"
    "      --initial-path <path>         default: %s\n"
    "      --initial-tmpdir <directory>  default: %s\n"
    "\n"
    "  -V, --version\n"
    "  -h, --help\n"
    "\n"
    "Change how to connect to the mysql server:\n"
    "\n"
    "  -M, --mysql-default-file [mysql-file]    default: %s\n"
    "  -S, --mysql-default-group [mysql-group]  default: %s\n"
    "\n"
    "Directories:\n"
    "    LOGDIR      " MXQ_LOGDIR "\n"
    "\n"
    "Environment:\n"
    "  MXQ_MYSQL_DEFAULT_FILE   change default for [mysql-file]\n"
    "  MXQ_MYSQL_DEFAULT_GROUP  change default for [mysql-group]\n"
    "\n",
    program_invocation_short_name,
    MXQ_INITIAL_PATH,
    MXQ_INITIAL_TMPDIR,
    MXQ_MYSQL_DEFAULT_FILE_STR,
    MXQ_MYSQL_DEFAULT_GROUP_STR
    );
}

static void cpuset_log(char *prefix,cpu_set_t *cpuset)
{
    char *str;
    str=mx_cpuset_to_str(cpuset);
    mx_log_info("%s: [%s]",prefix,str);
    free(str);
}

static void cpuset_init_job(cpu_set_t *job_cpu_set,cpu_set_t *available,cpu_set_t *running,int slots)
{
    int cpu;
    CPU_ZERO(job_cpu_set);
    for (cpu=CPU_SETSIZE-1;slots&&cpu>=0;cpu--) {
        if (CPU_ISSET(cpu,available) && !CPU_ISSET(cpu,running)) {
            CPU_SET(cpu,job_cpu_set);
            slots--;
        }
    }
 }

static void cpuset_clear_running(cpu_set_t *running,cpu_set_t *job) {
    int cpu;
    for (cpu=0;cpu<CPU_SETSIZE;cpu++) {
        if (CPU_ISSET(cpu,job)) {
            CPU_CLR(cpu,running);
        }
    }
}

/**********************************************************************/
int setup_cronolog(char *cronolog, char *logdir, char *rellink, char *relformat)
{
    int res;
    int pipe_fd[2];
    int pid;
    _mx_cleanup_free_ char *link = NULL;
    _mx_cleanup_free_ char *format = NULL;

    if (logdir) {
        link   = mx_strconcat(logdir, "/", rellink);
        format = mx_strconcat(logdir, "/", relformat);
    } else {
        link   = strdup(rellink);
        format = strdup(relformat);
    }

    if (!link || !format) {
        mx_log_err("can't allocate filenames: (%m)");
        return 0;
    }

    res = pipe(pipe_fd);
    if (res == -1) {
        mx_log_err("can't create pipe for cronolog: (%m)");
        return 0;
    }

    pid = fork();
    if (pid < 0) {
        mx_log_err("cronolog fork failed: %m");
        return 0;
    } else if(pid == 0) {
        res = dup2(pipe_fd[0], STDIN_FILENO);
        if (res == -1) {
            mx_log_err("dup2(fh=%d, %d) for cronolog stdin failed (%m)", pipe_fd[0], STDIN_FILENO);
            return 0;
        }
        close(pipe_fd[0]);
        close(pipe_fd[1]);

        mxq_redirect_output("/dev/null", "/dev/null");

        execl(cronolog, cronolog, "--link", link, format, NULL);
        mx_log_err("execl('%s', ...) failed (%m)", cronolog);
        _exit(EX__MAX + 1);
    }

    res = dup2(pipe_fd[1], STDOUT_FILENO);
    if (res == -1) {
        mx_log_err("dup2(fh=%d, %d) for cronolog stdout failed (%m)", pipe_fd[0], STDOUT_FILENO);
        return 0;
    }
    res = dup2(STDOUT_FILENO, STDERR_FILENO);
    if (res == -1) {
        mx_log_err("dup2(fh=%d, %d) for cronolog stderr failed (%m)", STDOUT_FILENO, STDERR_FILENO);
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
        mx_log_err("open(%s) for stdin failed (%m)", fname);
        return 0;
    }

    if (fh != STDIN_FILENO) {
        res = dup2(fh, STDIN_FILENO);
        if (res == -1) {
            mx_log_err("dup2(fh=%d, %d) failed (%m)", fh, STDIN_FILENO);
            return 0;
        }
        res = close(fh);
        if (res == -1) {
            mx_log_err("close(fh=%d) failed (%m)", fh);
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

static int cpuset_init(struct mxq_server *server)
{
    int res;
    int available_cnt;
    int cpu;
    int slots;

    slots=server->slots;

    res=sched_getaffinity(0,sizeof(server->cpu_set_available),&server->cpu_set_available);
    if (res<0) {
        mx_log_err("sched_getaffinity: (%m)");
        return(-errno);
    }
    available_cnt=CPU_COUNT(&server->cpu_set_available);
    if (slots) {
        if (slots>available_cnt) {
            mx_log_err("%d slots requested, but only %d cores available",slots,available_cnt);
            return(-(errno=EINVAL));
        }
    } else {
        if (available_cnt>=16) {
            slots=available_cnt-2;
        } else if (available_cnt>=4) {
            slots=available_cnt-1;
        } else {
            slots=available_cnt;
        }
    }

    for (cpu=0;cpu<CPU_SETSIZE && available_cnt>slots;cpu++) {
        if (CPU_ISSET(cpu,&server->cpu_set_available)) {
            CPU_CLR(cpu,&server->cpu_set_available);
            available_cnt--;
        }
    }
    server->slots=slots;
    return(0);
}

int server_init(struct mxq_server *server, int argc, char *argv[])
{
    int res;
    char *arg_server_id;
    char *arg_hostname;
    char *arg_mysql_default_group;
    char *arg_mysql_default_file;
    char *arg_pidfile = NULL;
    char *arg_logdir = NULL;
    char *arg_initial_path;
    char *arg_initial_tmpdir;
    char arg_daemonize = 0;
    char arg_nolog = 0;
    char arg_recoveronly = 0;
    char *str_bootid;
    int opt;
    unsigned long arg_threads_total = 0;
    unsigned long arg_memory_total = 2048;
    unsigned long arg_memory_max   = 0;
    int i;

    _mx_cleanup_free_ struct mx_proc_pid_stat *pps = NULL;

    struct mx_getopt_ctl optctl;
    struct mx_option opts[] = {
                MX_OPTION_NO_ARG("help",               'h'),
                MX_OPTION_NO_ARG("version",            'V'),
                MX_OPTION_NO_ARG("daemonize",            1),
                MX_OPTION_NO_ARG("no-log",               3),
                MX_OPTION_OPTIONAL_ARG("log",            4),
                MX_OPTION_REQUIRED_ARG("log-directory",  4),
                MX_OPTION_NO_ARG("debug",                5),
                MX_OPTION_NO_ARG("recover-only",         9),
                MX_OPTION_REQUIRED_ARG("pid-file",       2),
                MX_OPTION_REQUIRED_ARG("initial-path",   7),
                MX_OPTION_REQUIRED_ARG("initial-tmpdir", 8),
                MX_OPTION_REQUIRED_ARG("slots",        'j'),
                MX_OPTION_REQUIRED_ARG("memory",       'm'),
                MX_OPTION_REQUIRED_ARG("max-memory-per-slot", 'x'),
                MX_OPTION_REQUIRED_ARG("server-id",    'N'),
                MX_OPTION_REQUIRED_ARG("hostname",       6),
                MX_OPTION_OPTIONAL_ARG("mysql-default-file",  'M'),
                MX_OPTION_OPTIONAL_ARG("mysql-default-group", 'S'),
                MX_OPTION_END
    };

    arg_server_id = "main";
    arg_hostname  = mx_hostname();

#ifdef MXQ_DEVELOPMENT
    arg_nolog = 1;
#endif

    arg_initial_path = MXQ_INITIAL_PATH;
    arg_initial_tmpdir = MXQ_INITIAL_TMPDIR;

    arg_mysql_default_group = getenv("MXQ_MYSQL_DEFAULT_GROUP");
    if (!arg_mysql_default_group)
        arg_mysql_default_group = MXQ_MYSQL_DEFAULT_GROUP;

    arg_mysql_default_file  = getenv("MXQ_MYSQL_DEFAULT_FILE");
    if (!arg_mysql_default_file)
        arg_mysql_default_file = MXQ_MYSQL_DEFAULT_FILE;

    mx_getopt_init(&optctl, argc-1, &argv[1], opts);

//    optctl.flags = MX_FLAG_STOPONUNKNOWN|MX_FLAG_STOPONNOOPT;

    while ((opt=mx_getopt(&optctl, &i)) != MX_GETOPT_END) {
        if (opt == MX_GETOPT_ERROR) {
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

            case 4:
                arg_nolog = 0;
                arg_logdir = optctl.optarg;
                break;

            case 5:
                mx_log_level_set(MX_LOG_DEBUG);
                break;

            case 6:
                arg_hostname = optctl.optarg;
                break;

            case 9:
                arg_recoveronly = 1;
                break;

            case 'V':
                mxq_print_generic_version();
                exit(EX_USAGE);

            case 'h':
                print_usage();
                exit(EX_USAGE);

            case 'j':
                if (mx_strtoul(optctl.optarg, &arg_threads_total) < 0) {
                    mx_log_err("Invalid argument supplied for option --slots '%s': %m", optctl.optarg);
                    exit(1);
                }
                break;

            case 'm':
                if (mx_strtoul(optctl.optarg, &arg_memory_total) < 0) {
                    unsigned long long int bytes;

                    if(mx_strtobytes(optctl.optarg, &bytes) < 0) {
                        mx_log_err("Invalid argument supplied for option --memory '%s': %m", optctl.optarg);
                        exit(1);
                    }
                    arg_memory_total = bytes/1024/1024;
                }
                if (!arg_memory_total)
                    arg_memory_total = 2048;
                break;

            case 'x':
                if (mx_strtoul(optctl.optarg, &arg_memory_max) < 0) {
                    unsigned long long int bytes;

                    if(mx_strtobytes(optctl.optarg, &bytes) < 0) {
                        mx_log_err("Invalid argument supplied for option --max-memory-per-slot '%s': %m", optctl.optarg);
                        exit(1);
                    }
                    arg_memory_max = bytes/1024/1024;
                }
                break;

            case 'N':
                arg_server_id = optctl.optarg;
                break;

            case 7:
                arg_initial_path = optctl.optarg;
                break;

            case 8:
                arg_initial_tmpdir = optctl.optarg;
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

    if (arg_daemonize && arg_nolog) {
        mx_log_err("Error while using conflicting options --daemonize and --no-log at once.");
        exit(EX_USAGE);
    }

    memset(server, 0, sizeof(*server));

    server->hostname = arg_hostname;
    server->server_id = arg_server_id;
    server->initial_path = arg_initial_path;
    server->initial_tmpdir = arg_initial_tmpdir;
    server->recoveronly = arg_recoveronly;

    server->flock = mx_flock(LOCK_EX, "/dev/shm/mxqd.%s.%s.lck", server->hostname, server->server_id);
    if (!server->flock) {
        return -1;
    }

    if (!server->flock->locked) {
        mx_log_err("MXQ Server '%s' on host '%s' is already running. Exiting.", server->server_id, server->hostname);
        exit(2);
    }

    mx_asprintf_forever(&server->finished_jobsdir,"%s/%s",MXQ_FINISHED_JOBSDIR,server->server_id);
    res=mx_mkdir_p(server->finished_jobsdir,0700);
    if (res<0) {
        mx_log_err("MAIN: mkdir %s failed: %m. Exiting.",MXQ_FINISHED_JOBSDIR);
        exit(EX_IOERR);
    }

    if (arg_daemonize) {
        res = daemon(0, 1);
        if (res == -1) {
            mx_log_err("MAIN: daemon(0, 1) failed: %m. Exiting.");
            exit(EX_UNAVAILABLE);
        }
    }

    if (arg_pidfile) {
        res = write_pid_to_file(arg_pidfile);
        if (res < 0) {
            mx_log_err("MAIN: pidfile (%s) setup failed: %m.  Exiting.", arg_pidfile);
            exit(EX_IOERR);
        }

        server->pidfilename = arg_pidfile;
    }

    res = prctl(PR_SET_CHILD_SUBREAPER, 1);
    if (res == -1) {
        mx_log_err("MAIN: prctl(PR_SET_CHILD_SUBREAPER) setup failed: %m.  Exiting.");
        exit(EX_IOERR);
    }

    setup_stdin("/dev/null");

    if (!arg_nolog) {
        if (!arg_logdir)
            arg_logdir = MXQ_LOGDIR;

        if (access(arg_logdir, R_OK|W_OK|X_OK)) {
            if (!RUNNING_AS_ROOT)
                mx_log_warning("Running mxqd as non-root user.");
            mx_log_err("MAIN: can't write to '%s': %m", arg_logdir);
            exit(EX_IOERR);
        }
        res = setup_cronolog("/usr/sbin/cronolog", arg_logdir, "mxqd_log", "%Y/mxqd_log-%Y-%m");
        if (!res) {
            if (!RUNNING_AS_ROOT)
                mx_log_warning("Running mxqd as non-root user.");
            mx_log_err("MAIN: cronolog setup failed. exiting.");
            exit(EX_IOERR);
        }
    }

    if (!RUNNING_AS_ROOT)
        mx_log_warning("Running mxqd as non-root user.");

    res = mx_mysql_initialize(&(server->mysql));
    assert(res == 0);

    mx_mysql_option_set_default_file(server->mysql,  arg_mysql_default_file);
    mx_mysql_option_set_default_group(server->mysql, arg_mysql_default_group);
    mx_mysql_option_set_reconnect(server->mysql, 1);

    res = mx_read_first_line_from_file("/proc/sys/kernel/random/boot_id", &str_bootid);
    assert(res == 36);
    assert(str_bootid);

    server->boot_id = str_bootid;

    res = mx_proc_pid_stat(&pps, getpid());
    assert(res == 0);

    server->starttime = pps->starttime;
    mx_proc_pid_stat_free_content(pps);

    mx_asprintf_forever(&server->host_id, "%s-%llx-%x", server->boot_id, server->starttime, getpid());
    mx_setenv_forever("MXQ_HOSTID", server->host_id);

    server->slots = arg_threads_total;
    res = cpuset_init(server);
    if (res < 0) {
        mx_log_err("MAIN: cpuset_init() failed. exiting.");
        exit(1);
    }
    server->memory_total = arg_memory_total;
    server->memory_max_per_slot = arg_memory_max;

    /* if run as non-root use full memory by default for every job */
    if (!arg_memory_max && !RUNNING_AS_ROOT)
        server->memory_max_per_slot = arg_memory_total;

    server->memory_avg_per_slot = (long double)server->memory_total / (long double)server->slots;

    if (server->memory_max_per_slot < server->memory_avg_per_slot)
        server->memory_max_per_slot = server->memory_avg_per_slot;

    if (server->memory_max_per_slot > server->memory_total)
        server->memory_max_per_slot = server->memory_total;

    return 1;
}

/**********************************************************************/

static void _group_list_init(struct mxq_group_list *glist)
{
    struct mxq_server *server;
    struct mxq_group *group;

    long double memory_threads;
    long double memory_per_thread;
    long double memory_max_available;

    unsigned long slots_per_job;
    unsigned long jobs_max;
    unsigned long slots_max;
    unsigned long memory_max;

    assert(glist);
    assert(glist->user);
    assert(glist->user->server);

    server = glist->user->server;
    group  = &glist->group;

    memory_per_thread    = (long double)group->job_memory / (long double)group->job_threads;
    memory_max_available = (long double)server->memory_total * (long double)server->memory_max_per_slot / memory_per_thread;

    if (memory_max_available > server->memory_total)
        memory_max_available = server->memory_total;

    slots_per_job = ceill((long double)group->job_memory / server->memory_avg_per_slot);

    if (slots_per_job < group->job_threads)
        slots_per_job = group->job_threads;

    memory_threads = memory_max_available / memory_per_thread;

    if (memory_per_thread > server->memory_max_per_slot) {
        jobs_max = memory_threads + 0.5;
    } else if (memory_per_thread > server->memory_avg_per_slot) {
        jobs_max = memory_threads + 0.5;
    } else {
        jobs_max = server->slots;
    }
    jobs_max /= group->job_threads;

    /* limit maximum number of jobs on user/group request */
    if (group->job_max_per_node && jobs_max > group->job_max_per_node)
        jobs_max = group->job_max_per_node;

    slots_max  = jobs_max * slots_per_job;
    memory_max = jobs_max * group->job_memory;

    if (glist->memory_per_thread != memory_per_thread
       || glist->memory_max_available != memory_max_available
       || glist->memory_max_available != memory_max_available
       || glist->slots_per_job != slots_per_job
       || glist->jobs_max != jobs_max
       || glist->slots_max != slots_max
       || glist->memory_max != memory_max) {
        mx_log_info("  group=%s(%u):%lu jobs_max=%lu slots_max=%lu memory_max=%lu slots_per_job=%lu :: group %sinitialized.",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    jobs_max,
                    slots_max,
                    memory_max,
                    slots_per_job,
                    glist->orphaned ? "re" : "");
    }

    glist->memory_per_thread    = memory_per_thread;
    glist->memory_max_available = memory_max_available;

    glist->slots_per_job = slots_per_job;

    glist->jobs_max   = jobs_max;
    glist->slots_max  = slots_max;
    glist->memory_max = memory_max;

    glist->orphaned = 0;
}

static struct mxq_group_list *server_get_group_list_by_group_id(struct mxq_server *server, uint64_t group_id)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;

    struct mxq_group *group;

    for (ulist = server->users; ulist; ulist = ulist->next) {
        for (glist = ulist->groups; glist; glist = glist->next) {
            group = &glist->group;
            if (group->group_id == group_id)
                return glist;
        }
    }
    return NULL;
}

static struct mxq_job_list *server_get_job_list_by_job_id(struct mxq_server *server, uint64_t job_id)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;
    struct mxq_job_list   *jlist;

    struct mxq_job *job;

    for (ulist = server->users; ulist; ulist = ulist->next) {
        for (glist = ulist->groups; glist; glist = glist->next) {
            for (jlist = glist->jobs; jlist; jlist = jlist->next) {
                job = &jlist->job;
                if (job->job_id == job_id)
                    return jlist;
            }
        }
    }
    return NULL;
}

static struct mxq_job_list *server_get_job_list_by_pid(struct mxq_server *server, pid_t pid)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;
    struct mxq_job_list   *jlist;

    struct mxq_job *job;

    assert(server);

    for (ulist = server->users; ulist; ulist = ulist->next) {
        for (glist = ulist->groups; glist; glist = glist->next) {
            for (jlist = glist->jobs; jlist; jlist = jlist->next) {
                job = &jlist->job;
                if (job->host_pid == pid)
                    return jlist;
            }
        }
    }
    return NULL;
}

void job_list_remove_self(struct mxq_job_list *jlist)
{
    struct mxq_group_list *glist;
    struct mxq_user_list  *ulist;
    struct mxq_server     *server;

    struct mxq_job_list **jprevp;

    struct mxq_job   *job;
    struct mxq_group *group;

    assert(jlist);
    assert(jlist->group);
    assert(jlist->group->user);
    assert(jlist->group->user->server);

    glist  = jlist->group;
    ulist  = glist->user;
    server = ulist->server;

    group = &glist->group;
    job   = &jlist->job;

    for (jprevp = &glist->jobs; *jprevp; jprevp = &(*jprevp)->next) {
        if (*jprevp != jlist)
            continue;

        *jprevp = jlist->next;

        glist->job_cnt--;
        ulist->job_cnt--;
        server->job_cnt--;

        glist->slots_running  -= job->host_slots;
        ulist->slots_running  -= job->host_slots;
        server->slots_running -= job->host_slots;

        glist->threads_running  -= group->job_threads;
        ulist->threads_running  -= group->job_threads;
        server->threads_running -= group->job_threads;

        group->group_jobs_running--;

        glist->jobs_running--;
        ulist->jobs_running--;
        server->jobs_running--;

        glist->memory_used  -= group->job_memory;
        ulist->memory_used  -= group->job_memory;
        server->memory_used -= group->job_memory;
        break;
    }
}

struct mxq_job_list *server_remove_job_list_by_pid(struct mxq_server *server, pid_t pid)
{
    struct mxq_job_list *jlist;

    assert(server);

    jlist = server_get_job_list_by_pid(server, pid);
    if (jlist) {
        job_list_remove_self(jlist);
    }
    return jlist;
}

/**********************************************************************/

static struct mxq_user_list *_user_list_find_by_uid(struct mxq_user_list *ulist, uint32_t uid)
{
    for (; ulist; ulist = ulist->next) {
        assert(ulist->groups);

        if (ulist->groups[0].group.user_uid == uid) {
            return ulist;
        }
    }
    return NULL;
}

/**********************************************************************/

static struct mxq_user_list *server_find_user_by_uid(struct mxq_server *server, uint32_t uid)
{
    assert(server);

    return _user_list_find_by_uid(server->users, uid);
}

/**********************************************************************/

struct mxq_group_list *_group_list_find_by_group(struct mxq_group_list *glist, struct mxq_group *group)
{
    assert(group);

    for (; glist; glist = glist->next) {
        if (glist->group.group_id == group->group_id) {
            return glist;
        }
    }
    return NULL;
}

/**********************************************************************/

struct mxq_job_list *group_list_add_job(struct mxq_group_list *glist, struct mxq_job *job)
{
    struct mxq_server *server;

    struct mxq_job_list  *jlist;
    struct mxq_user_list *ulist;

    struct mxq_group *group;

    assert(glist);
    assert(glist->user);
    assert(glist->user->server);
    assert(job->job_status == MXQ_JOB_STATUS_RUNNING);

    group  = &glist->group;
    ulist  = glist->user;
    server = ulist->server;

    jlist = mx_calloc_forever(1, sizeof(*jlist));

    memcpy(&jlist->job, job, sizeof(*job));

    jlist->group = glist;

    jlist->next  = glist->jobs;
    glist->jobs  = jlist;

    glist->job_cnt++;
    ulist->job_cnt++;
    server->job_cnt++;

    glist->slots_running  += glist->slots_per_job;
    ulist->slots_running  += glist->slots_per_job;
    server->slots_running += glist->slots_per_job;

    glist->threads_running  += group->job_threads;
    ulist->threads_running  += group->job_threads;
    server->threads_running += group->job_threads;

    CPU_OR(&server->cpu_set_running, &server->cpu_set_running, &job->host_cpu_set);

    group->group_jobs_running++;
    group->group_jobs_inq--;

    glist->jobs_running++;
    ulist->jobs_running++;
    server->jobs_running++;

    glist->memory_used  += group->job_memory;
    ulist->memory_used  += group->job_memory;
    server->memory_used += group->job_memory;

    return jlist;
}
/**********************************************************************/

static struct mxq_group_list *_user_list_add_group(struct mxq_user_list *ulist, struct mxq_group *group)
{
    struct mxq_group_list *glist;
    struct mxq_server *server;

    assert(ulist);
    assert(ulist->server);

    server = ulist->server;

    glist = mx_calloc_forever(1, sizeof(*glist));

    memcpy(&glist->group, group, sizeof(*group));

    glist->user = ulist;

    glist->next = ulist->groups;
    ulist->groups = glist;

    ulist->group_cnt++;
    server->group_cnt++;

    _group_list_init(glist);

    return glist;
}

/**********************************************************************/

static struct mxq_group_list *_server_add_group(struct mxq_server *server, struct mxq_group *group)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;

    assert(server);
    assert(group);

    ulist = mx_calloc_forever(1, sizeof(*ulist));

    ulist->server = server;

    ulist->next   = server->users;
    server->users = ulist;

    server->user_cnt++;

    glist = _user_list_add_group(ulist, group);
    assert(glist);

    return glist;
}

/**********************************************************************/

static struct mxq_group_list *_user_list_update_group(struct mxq_user_list *ulist, struct mxq_group *group)
{
    struct mxq_group_list *glist;

    assert(ulist);
    assert(group);

    glist = _group_list_find_by_group(ulist->groups, group);
    if (!glist) {
        return _user_list_add_group(ulist, group);
    }

    mxq_group_free_content(&glist->group);

    memcpy(&glist->group, group, sizeof(*group));

    _group_list_init(glist);

    return glist;
}

/**********************************************************************/

static struct mxq_group_list *server_update_group(struct mxq_server *server, struct mxq_group *group)
{
    struct mxq_user_list *ulist;

    ulist = _user_list_find_by_uid(server->users, group->user_uid);
    if (!ulist) {
        return _server_add_group(server, group);
    }

    return _user_list_update_group(ulist, group);
}

static void reset_signals()
{
    signal(SIGINT,  SIG_DFL);
    signal(SIGTERM, SIG_DFL);
    signal(SIGQUIT, SIG_DFL);
    signal(SIGHUP,  SIG_DFL);
    signal(SIGTSTP, SIG_DFL);
    signal(SIGTTIN, SIG_DFL);
    signal(SIGTTOU, SIG_DFL);
    signal(SIGCHLD, SIG_DFL);
    signal(SIGPIPE, SIG_DFL);
}

static int init_child_process(struct mxq_group_list *group, struct mxq_job *j)
{
    struct mxq_group *g;
    struct mxq_server *s;
    struct passwd *passwd;
    int res;
    int fh;
    struct rlimit rlim;

    assert(j);
    assert(group);
    assert(group->user);
    assert(group->user->server);

    s = group->user->server;
    g = &group->group;

    reset_signals();

    passwd = getpwuid(g->user_uid);
    if (!passwd) {
        mx_log_err("job=%s(%d):%lu:%lu getpwuid(): %m",
            g->user_name, g->user_uid, g->group_id, j->job_id);
        return 0;
    }

    if (!mx_streq(passwd->pw_name, g->user_name)) {
        mx_log_err("job=%s(%d):%lu:%lu user_uid=%d does not map to user_name=%s but to pw_name=%s: Invalid user mapping",
            g->user_name, g->user_uid, g->group_id, j->job_id,
            g->user_uid, g->user_name, passwd->pw_name);
        return 0;
    }


    /** prepare environment **/

    res = clearenv();
    if (res != 0) {
        mx_log_err("job=%s(%d):%lu:%lu clearenv(): %m",
            g->user_name, g->user_uid, g->group_id, j->job_id);
        return 0;
    }

    mx_setenv_forever("USER",     g->user_name);
    mx_setenv_forever("USERNAME", g->user_name);
    mx_setenv_forever("LOGNAME",  g->user_name);
    mx_setenv_forever("PATH",     s->initial_path);
    mx_setenv_forever("TMPDIR",   s->initial_tmpdir);
    mx_setenv_forever("PWD",      j->job_workdir);
    mx_setenv_forever("HOME",     passwd->pw_dir);
    mx_setenv_forever("SHELL",    passwd->pw_shell);
    mx_setenv_forever("HOSTNAME", mx_hostname());
    mx_setenvf_forever("JOB_ID",      "%lu",    j->job_id);
    mx_setenvf_forever("MXQ_JOBID",   "%lu",    j->job_id);
    mx_setenvf_forever("MXQ_THREADS", "%d",     g->job_threads);
    mx_setenvf_forever("MXQ_SLOTS",   "%lu",    group->slots_per_job);
    mx_setenvf_forever("MXQ_MEMORY",  "%lu",    g->job_memory);
    mx_setenvf_forever("MXQ_TIME",    "%d",     g->job_time);
    mx_setenv_forever("MXQ_HOSTID",   s->host_id);
    mx_setenv_forever("MXQ_HOSTNAME", s->hostname);
    mx_setenv_forever("MXQ_SERVERID", s->server_id);

    fh = open("/proc/self/loginuid", O_WRONLY|O_TRUNC);
    if (fh == -1) {
        mx_log_err("job=%s(%d):%lu:%lu open(%s) failed: %m",
            g->user_name, g->user_uid, g->group_id, j->job_id, "/proc/self/loginuid");
        return 0;
    }
    dprintf(fh, "%d", g->user_uid);
    close(fh);

    /* set memory limits */
    rlim.rlim_cur = g->job_memory*1024*1024;
    rlim.rlim_max = g->job_memory*1024*1024;

    res = setrlimit(RLIMIT_AS, &rlim);
    if (res == -1)
        mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_AS, ...) failed: %m",
                    g->user_name, g->user_uid, g->group_id, j->job_id);

    res = setrlimit(RLIMIT_DATA, &rlim);
    if (res == -1)
        mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_DATA, ...) failed: %m",
                    g->user_name, g->user_uid, g->group_id, j->job_id);

    res = setrlimit(RLIMIT_RSS, &rlim);
    if (res == -1)
        mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_RSS, ...) failed: %m",
                    g->user_name, g->user_uid, g->group_id, j->job_id);

    /* disable core files */
    rlim.rlim_cur = 0;
    rlim.rlim_cur = 0;

    res = setrlimit(RLIMIT_CORE,  &rlim);
    if (res == -1)
        mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_CORE, ...) failed: %m",
                    g->user_name, g->user_uid, g->group_id, j->job_id);

    /* set single threaded time limits */
    if (g->job_threads == 1) {
            /* set cpu time limits - hardlimit is 105% of softlimit */
            rlim.rlim_cur = g->job_time*60;
            rlim.rlim_cur = g->job_time*63;

            res = setrlimit(RLIMIT_CPU,  &rlim);
            if (res == -1)
                mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_CPU, ...) failed: %m",
                            g->user_name, g->user_uid, g->group_id, j->job_id);
    }

    if(RUNNING_AS_ROOT) {

        res = initgroups(passwd->pw_name, g->user_gid);
        if (res == -1) {
            mx_log_err("job=%s(%d):%lu:%lu initgroups() failed: %m",
                g->user_name, g->user_uid, g->group_id, j->job_id);
            return 0;
        }

        res = setregid(g->user_gid, g->user_gid);
        if (res == -1) {
            mx_log_err("job=%s(%d):%lu:%lu setregid(%d, %d) failed: %m",
                g->user_name, g->user_uid, g->group_id, j->job_id,
                g->user_gid, g->user_gid);
            return 0;
        }

        res = setreuid(g->user_uid, g->user_uid);
        if (res == -1) {
            mx_log_err("job=%s(%d):%lu:%lu setreuid(%d, %d) failed: %m",
                g->user_name, g->user_uid, g->group_id, j->job_id,
                g->user_uid, g->user_uid);
            return 0;
        }
    }

    res = chdir(j->job_workdir);
    if (res == -1) {
        mx_log_err("job=%s(%d):%lu:%lu chdir(%s) failed: %m",
            g->user_name, g->user_uid, g->group_id, j->job_id,
            j->job_workdir);
        return 0;
    }

    umask(j->job_umask);

    res=sched_setaffinity(0,sizeof(j->host_cpu_set),&j->host_cpu_set);
    if (res<0) mx_log_warning("sched_setaffinity: $m");

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
    } else if (!mx_streq(fname, "/dev/null")) {
        res = unlink(fname);
        if (res == -1 && errno != ENOENT) {
            mx_log_err("unlink() failed: %m");
           return -2;
        }
        flags |= O_EXCL;
    }

    fh = open(fname, flags, mode);
    if (fh == -1) {
            mx_log_err("open() failed: %m");
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
        mx_log_err("open() failed: %m");
        return -1;
    }

    res = mx_dup2_close_both(fh, STDIN_FILENO);
    if (res < 0) {
        return -2;
    }

    return 1;
}

int user_process(struct mxq_group_list *group,struct mxq_job *mxqjob)
{
    int res;
    char **argv;

    res = init_child_process(group, mxqjob);
    if (!res)
        return(-1);

    mxq_job_set_tmpfilenames(&group->group, mxqjob);

    res = mxq_redirect_input("/dev/null");
    if (res < 0) {
        mx_log_err("   job=%s(%d):%lu:%lu mxq_redirect_input() failed (%d): %m",
            group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob->job_id,
            res);
        return(res);
    }

    res = mxq_redirect_output(mxqjob->tmp_stdout, mxqjob->tmp_stderr);
    if (res < 0) {
        mx_log_err("   job=%s(%d):%lu:%lu mxq_redirect_output() failed (%d): %m",
            group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob->job_id,
            res);
        return(res);
    }

    argv = mx_strvec_from_str(mxqjob->job_argv_str);
    if (!argv) {
        mx_log_err("job=%s(%d):%lu:%lu Can't recaculate commandline. str_to_strvev(%s) failed: %m",
            group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob->job_id,
            mxqjob->job_argv_str);
        return(-errno);
    }

    res=execvp(argv[0], argv);
    mx_log_err("job=%s(%d):%lu:%lu execvp(\"%s\", ...): %m",
        group->group.user_name, group->group.user_uid, group->group.group_id, mxqjob->job_id,
        argv[0]);
    return(res);
}

int reaper_process(struct mxq_server *server,struct mxq_group_list  *group,struct mxq_job *job) {
    pid_t pid;
    struct rusage rusage;
    int status;
    pid_t  waited_pid;
    int    waited_status;
    struct timeval now;
    struct timeval realtime;
    _mx_cleanup_free_ char *finished_job_filename=NULL;
    _mx_cleanup_free_ char *finished_job_tmpfilename=NULL;
    FILE *out;
    int res;

    reset_signals();

    signal(SIGINT,  SIG_IGN);
    signal(SIGTERM, SIG_IGN);
    signal(SIGHUP,  SIG_IGN);
    signal(SIGXCPU, SIG_IGN);

    res = setsid();
    if (res<0) {
	mx_log_warning("reaper_process setsid: %m");
    }

    res=prctl(PR_SET_CHILD_SUBREAPER, 1);
    if (res<0) {
        mx_log_err("set subreaper: %m");
        return res;
    }

    pid = fork();
    if (pid < 0) {
        mx_log_err("fork: %m");
        return(pid);
    } else if (pid == 0) {
        res=user_process(group,job);
        _exit(EX__MAX+1);
    }
    gettimeofday(&job->stats_starttime, NULL);

    while (1) {
        waited_pid=wait(&waited_status);
        if (waited_pid<0) {
            if (errno==ECHILD) {
                break;
            } else {
                mx_log_warning("reaper: wait: %m");
                sleep(1);
            }
        }
        if (waited_pid==pid) {
            status=waited_status;
        }
    }
    gettimeofday(&now, NULL);
    timersub(&now, &job->stats_starttime, &realtime);
    res=getrusage(RUSAGE_CHILDREN,&rusage);
    if (res<0) {
        mx_log_err("reaper: getrusage: %m");
        return(res);
    }

    mx_asprintf_forever(&finished_job_filename,"%s/%lu.stat",server->finished_jobsdir,job->job_id);
    mx_asprintf_forever(&finished_job_tmpfilename,"%s.tmp",finished_job_filename);

    out=fopen(finished_job_tmpfilename,"w");
    if (!out) {
        mx_log_fatal("%s: %m",finished_job_tmpfilename);
        return (-errno);
    }

    fprintf(out,"1 %d %d %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",
        getpid(),
        status,
        realtime.tv_sec,realtime.tv_usec,
        rusage.ru_utime.tv_sec,rusage.ru_utime.tv_usec,
        rusage.ru_stime.tv_sec,rusage.ru_stime.tv_usec,
        rusage.ru_maxrss,
        rusage.ru_ixrss,
        rusage.ru_idrss,
        rusage.ru_isrss,
        rusage.ru_minflt,
        rusage.ru_majflt,
        rusage.ru_nswap,
        rusage.ru_inblock,
        rusage.ru_oublock,
        rusage.ru_msgsnd,
        rusage.ru_msgrcv,
        rusage.ru_nsignals,
        rusage.ru_nvcsw,
        rusage.ru_nivcsw
    );
    fflush(out);
    fsync(fileno(out));
    fclose(out);
    res=rename(finished_job_tmpfilename,finished_job_filename);
    if (res<0)  {
        mx_log_fatal("rename %s: %m",finished_job_tmpfilename);
        return(res);
    }
    return(0);
}

unsigned long start_job(struct mxq_group_list *glist)
{
    struct mxq_server *server;
    struct mxq_job_list *jlist;

    struct mxq_job _mxqjob;
    struct mxq_job *job;

    struct mxq_group *group;

    pid_t pid;
    int res;

    assert(glist);
    assert(glist->user);
    assert(glist->user->server);

    server = glist->user->server;
    group  = &glist->group;
    job    = &_mxqjob;

    res = mxq_load_job_from_group_for_server(server->mysql, job, group->group_id, server->hostname, server->server_id, server->host_id);
    if (!res) {
        return 0;
    }
    mx_log_info("   job=%s(%d):%lu:%lu :: new job loaded.",
            group->user_name, group->user_uid, group->group_id, job->job_id);

    cpuset_init_job(&job->host_cpu_set, &server->cpu_set_available, &server->cpu_set_running, glist->slots_per_job);
    job->host_cpu_set_str = mx_cpuset_to_str(&job->host_cpu_set);

    mx_log_info("job assigned cpus: [%s]", job->host_cpu_set_str);

    mx_mysql_disconnect(server->mysql);

    pid = fork();
    if (pid < 0) {
        mx_log_err("fork: %m");
        return 0;
    } else if (pid == 0) {
        job->host_pid = getpid();

        mx_log_info("   job=%s(%d):%lu:%lu host_pid=%d pgrp=%d :: new child process forked.",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    job->host_pid,
                    getpgrp());

        res = reaper_process(server, glist, job);
        _exit(res<0 ? EX__MAX+1 : 0);
    }

    gettimeofday(&job->stats_starttime, NULL);

    mx_mysql_connect_forever(&(server->mysql));

    job->host_pid   = pid;
    job->host_slots = glist->slots_per_job;
    res = mxq_set_job_status_running(server->mysql, job);
    if (res < 0)
        mx_log_err("job=%s(%d):%lu:%lu mxq_job_update_status_running(): %m",
            group->user_name, group->user_uid, group->group_id, job->job_id);
    if (res == 0)
        mx_log_err("job=%s(%d):%lu:%lu  mxq_job_update_status_running(): Job not found.",
            group->user_name, group->user_uid, group->group_id, job->job_id);

    jlist = group_list_add_job(glist, job);
    assert(jlist);

    mx_log_info("   job=%s(%d):%lu:%lu :: added running job to watch queue.",
        group->user_name, group->user_uid, group->group_id, job->job_id);

    return 1;
}

/**********************************************************************/

unsigned long start_user(struct mxq_user_list *ulist, int job_limit, long slots_to_start)
{
    struct mxq_server *server;
    struct mxq_group_list *glist;
    struct mxq_group_list *gnext = NULL;
    struct mxq_group *group;

    unsigned int prio;
    unsigned char started = 0;
    unsigned long slots_started = 0;
    int jobs_started = 0;

    assert(ulist);
    assert(ulist->server);
    assert(ulist->groups);

    server = ulist->server;
    glist  = ulist->groups;
    group  = &glist->group;

    prio = group->group_priority;

    assert(slots_to_start <= server->slots - server->slots_running);

    mx_log_debug(" user=%s(%d) slots_to_start=%ld job_limit=%d :: trying to start jobs for user.",
            group->user_name, group->user_uid, slots_to_start, job_limit);

    for (glist = ulist->groups; glist && slots_to_start > 0 && (!job_limit || jobs_started < job_limit); glist = gnext) {

        group  = &glist->group;

        assert(glist->jobs_running <= group->group_jobs);
        assert(glist->jobs_running <= glist->jobs_max);

        if (glist->jobs_running == group->group_jobs) {
            gnext = glist->next;
            if (!gnext && started) {
                gnext = ulist->groups;
                started = 0;
            }
            continue;
        }

        if (glist->jobs_running == glist->jobs_max) {
            gnext = glist->next;
            if (!gnext && started) {
                gnext = ulist->groups;
                started = 0;
            }
            continue;
        }

        if (mxq_group_jobs_inq(group) == 0) {
            gnext = glist->next;
            if (!gnext && started) {
                gnext = ulist->groups;
                started = 0;
            }
            continue;
        }

        if (glist->slots_per_job > slots_to_start) {
            gnext = glist->next;
            if (!gnext && started) {
                gnext = ulist->groups;
                started = 0;
            }
            continue;
        }

        if (group->group_priority < prio) {
            if (started) {
                gnext = ulist->groups;
                started = 0;
                continue;
            }
            prio = group->group_priority;
        }
        mx_log_info("  group=%s(%d):%lu slots_to_start=%ld slots_per_job=%lu :: trying to start job for group.",
                group->user_name, group->user_uid, group->group_id, slots_to_start, glist->slots_per_job);

        if (start_job(glist)) {

            slots_to_start -= glist->slots_per_job;
            jobs_started++;
            slots_started += glist->slots_per_job;

            started = 1;
        }

        gnext = glist->next;
        if (!gnext && started) {
            gnext = ulist->groups;
            started = 0;
        }
    }
    return slots_started;
}

/**********************************************************************/

unsigned long start_users(struct mxq_server *server)
{
    unsigned long slots_started;
    unsigned long slots_started_total = 0;
    long slots_to_start;
    int started = 0;

    struct mxq_user_list *ulist;
    struct mxq_user_list *unext = NULL;

    assert(server);

    if (!server->user_cnt)
        return 0;

    mx_log_debug("=== starting jobs on free_slots=%lu slots for user_cnt=%lu users", server->slots - server->slots_running, server->user_cnt);

    for (ulist = server->users; ulist; ulist = ulist->next) {

        slots_to_start = server->slots / server->user_cnt - ulist->slots_running;

        if (slots_to_start < 0)
            continue;

        if (slots_to_start > (server->slots - server->slots_running))
            slots_to_start = (server->slots - server->slots_running);

        slots_started = start_user(ulist, 0, slots_to_start);
        slots_started_total += slots_started;
    }

    for (ulist = server->users; ulist && server->slots - server->slots_running; ulist = unext) {
        slots_to_start = server->slots - server->slots_running;
        slots_started  = start_user(ulist, 1, slots_to_start);
        slots_started_total += slots_started;
        started = (started || slots_started);

        unext = ulist->next;
        if (!unext && started) {
            unext = server->users;
            started = 0;
        }
    }

    return slots_started_total;
}

/**********************************************************************/

int remove_orphaned_group_lists(struct mxq_server *server)
{
    struct mxq_user_list  *ulist, *unext, *uprev;
    struct mxq_group_list *glist, *gnext, *gprev;

    struct mxq_group *group;

    int cnt=0;

    for (ulist = server->users, uprev = NULL; ulist; ulist = unext) {
        unext = ulist->next;

        for (glist = ulist->groups, gprev = NULL; glist; glist = gnext) {
            gnext = glist->next;
            group = &glist->group;

            if (glist->job_cnt) {
                gprev = glist;
                continue;
            }

            assert(!glist->jobs);

            if (!glist->orphaned && mxq_group_jobs_active(group)) {
                glist->orphaned = 1;
                gprev = glist;
                continue;
            }

            if (gprev) {
                gprev->next = gnext;
            } else {
                assert(glist == ulist->groups);
                ulist->groups = gnext;
            }

            mx_log_info("group=%s(%d):%lu : Removing orphaned group.",
                        group->user_name,
                        group->user_uid,
                        group->group_id);

            ulist->group_cnt--;
            server->group_cnt--;
            cnt++;
            mxq_group_free_content(group);
            mx_free_null(glist);
        }

        if(ulist->groups) {
            uprev = ulist;
            continue;
        }

        if (uprev) {
            uprev->next = unext;
        } else {
            assert(ulist == server->users);
            server->users = unext;
        }

        server->user_cnt--;
        mx_free_null(ulist);

        mx_log_info("Removed orphaned user. %lu users left.", server->user_cnt);
    }
    return cnt;
}

void server_dump(struct mxq_server *server)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;
    struct mxq_job_list   *jlist;

    struct mxq_group *group;
    struct mxq_job   *job;

    if (!server->user_cnt)
        return;

    mx_log_info("====================== SERVER DUMP START ======================");
    for (ulist = server->users; ulist; ulist = ulist->next) {
        if (!ulist->groups) {
            mx_log_fatal("BUG: missing group in userlist.");
            continue;
        }
        group = &ulist->groups[0].group;
        mx_log_info("    user=%s(%d) slots_running=%lu",
                group->user_name,
                group->user_uid,
                ulist->slots_running);

        for (glist = ulist->groups; glist; glist = glist->next) {
            group = &glist->group;
            mx_log_info("        group=%s(%d):%lu %s jobs_in_q=%lu",
                group->user_name,
                group->user_uid,
                group->group_id,
                group->group_name,
                mxq_group_jobs_inq(group));
            for (jlist = glist->jobs; jlist; jlist = jlist->next) {
                job = &jlist->job;
                mx_log_info("            job=%s(%d):%lu:%lu %s",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    job->job_argv_str);
            }
        }
    }

    mx_log_info("memory_used=%lu memory_total=%lu",
                server->memory_used,
                server->memory_total);
    mx_log_info("slots_running=%lu slots=%lu threads_running=%lu jobs_running=%lu",
                server->slots_running,
                server->slots,
                server->threads_running,
                server->jobs_running);
    cpuset_log("cpu set running",
                &server->cpu_set_running);
    mx_log_info("====================== SERVER DUMP END ======================");
}

void server_close(struct mxq_server *server)
{
    struct mxq_user_list  *ulist, *unext;
    struct mxq_group_list *glist, *gnext;
    struct mxq_job_list   *jlist, *jnext;

    for (ulist = server->users; ulist; ulist = unext) {
        for (glist = ulist->groups; glist; glist = gnext) {
            for (jlist = glist->jobs; jlist; jlist = jnext) {
                jnext = jlist->next;
                mxq_job_free_content(&jlist->job);
                mx_free_null(jlist);
            }
            gnext = glist->next;
            mxq_group_free_content(&glist->group);
            mx_free_null(glist);
        }
        unext = ulist->next;
        mx_free_null(ulist);
    }

    if (server->pidfilename)
        unlink(server->pidfilename);

    mx_funlock(server->flock);

    mx_free_null(server->boot_id);
    mx_free_null(server->host_id);
    mx_free_null(server->finished_jobsdir);
}

int killall(struct mxq_server *server, int sig, unsigned int pgrp)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;
    struct mxq_job_list   *jlist;

    struct mxq_group *group;
    struct mxq_job   *job;

    pid_t pid;

    assert(server);

    for (ulist = server->users; ulist; ulist = ulist->next) {
        for (glist = ulist->groups; glist; glist = glist->next) {
            group = &glist->group;

            for (jlist = glist->jobs; jlist; jlist = jlist->next) {
                job = &jlist->job;
                pid = job->host_pid;
                if (pgrp)
                    pid = -pid;
                mx_log_info("Sending signal=%d to job=%s(%d):%lu:%lu %s=%d",
                    sig,
                    group->user_name, group->user_uid, group->group_id, job->job_id,
                    pgrp?"pgrp":"pid", pid);
                kill(pid, sig);
            }
        }
    }
    return 0;
}

int killall_over_time(struct mxq_server *server)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;
    struct mxq_job_list   *jlist;

    struct mxq_group *group;
    struct mxq_job   *job;

    struct timeval now;
    struct timeval delta;

    pid_t pid;

    assert(server);

    if (!server->jobs_running)
        return 0;

    /* limit killing to every >= 60 seconds */
    mx_within_rate_limit_or_return(60, 1);

    mx_log_debug("killall_over_time: Sending signals to all jobs running longer than requested.");

    gettimeofday(&now, NULL);

    for (ulist = server->users; ulist; ulist = ulist->next) {
        for (glist = ulist->groups; glist; glist = glist->next) {
            group = &glist->group;

            for (jlist = glist->jobs; jlist; jlist = jlist->next) {
                job = &jlist->job;

                timersub(&now, &job->stats_starttime, &delta);

                if (delta.tv_sec <= group->job_time*60)
                    continue;

                pid = job->host_pid;

                if (delta.tv_sec <= group->job_time*61) {
                    mx_log_info("killall_over_time(): Sending signal=XCPU to job=%s(%d):%lu:%lu pid=%d",
                        group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                    kill(pid, SIGXCPU);
                    continue;
                }

                mx_log_info("killall_over_time(): Sending signal=XCPU to job=%s(%d):%lu:%lu pgrp=%d",
                    group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                kill(-pid, SIGXCPU);

                if (delta.tv_sec <= group->job_time*63)
                    continue;

                mx_log_info("killall_over_time(): Sending signal=TERM to job=%s(%d):%lu:%lu pid=%d",
                    group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                kill(pid, SIGTERM);

                mx_log_info("killall_over_time(): Sending signal=HUP to job=%s(%d):%lu:%lu pgrp=%d",
                    group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                kill(-pid, SIGHUP);

                if (delta.tv_sec <= group->job_time*64)
                    continue;

                mx_log_info("killall_over_time(): Sending signal=TERM to job=%s(%d):%lu:%lu pgrp=%d",
                    group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                kill(-pid, SIGTERM);

                if (delta.tv_sec <= group->job_time*66)
                    continue;

                mx_log_info("killall_over_time(): Sending signal=KILL to job=%s(%d):%lu:%lu pid=%d",
                    group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                kill(pid, SIGKILL);

                mx_log_info("killall_over_time(): Sending signal=KILL to job=%s(%d):%lu:%lu pgrp=%d",
                    group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                kill(-pid, SIGKILL);
            }
        }
    }
    return 0;
}

int killall_over_memory(struct mxq_server *server)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;
    struct mxq_job_list   *jlist;

    struct mxq_group *group;
    struct mxq_job   *job;

    struct mx_proc_tree *ptree = NULL;
    struct mx_proc_info *pinfo;

    long pagesize;
    int res;

    assert(server);

    if (!server->jobs_running)
        return 0;

    /* limit killing to every >= 10 seconds */
    mx_within_rate_limit_or_return(10, 0);

    pagesize = sysconf(_SC_PAGESIZE);
    if (!pagesize) {
        mx_log_warning("killall_over_memory(): Can't get _SC_PAGESIZE. Assuming 4096.");
        pagesize = 4096;
    }

    res = mx_proc_tree(&ptree);
    if (res < 0) {
        mx_log_err("killall_over_memory(): Reading process tree failed: %m");
        return res;
    }

    for (ulist = server->users; ulist; ulist = ulist->next) {
        for (glist = ulist->groups; glist; glist = glist->next) {
            group = &glist->group;

            for (jlist = glist->jobs; jlist; jlist = jlist->next) {
                unsigned long long int memory;
                int signal;

                job = &jlist->job;

                /* sigterm has already been send last round ? */
                if (jlist->max_sum_rss/1024 > group->job_memory)
                    signal = SIGKILL;
                else
                    signal = SIGTERM;

                pinfo = mx_proc_tree_proc_info(ptree, job->host_pid);
                if (!pinfo) {
                    mx_log_warning("killall_over_memory(): Can't find process with pid %llu in process tree",
                        job->host_pid);
                    continue;
                }

                memory = pinfo->sum_rss * pagesize / 1024;

                if (jlist->max_sum_rss < memory)
                    jlist->max_sum_rss = memory;

                if (jlist->max_sum_rss/1024 <= group->job_memory)
                    continue;

                mx_log_info("killall_over_memory(): used(%lluMiB) > requested(%lluMiB): Sending signal=%d to job=%s(%d):%lu:%lu pid=%d",
                    jlist->max_sum_rss/1024,
                    group->job_memory,
                    signal,
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    job->host_pid);

                kill(job->host_pid, signal);
            }
        }
    }
    mx_proc_tree_free(&ptree);
    return 0;
}

int killall_cancelled(struct mxq_server *server, int sig, unsigned int pgrp)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;
    struct mxq_job_list   *jlist;

    struct mxq_group *group;
    struct mxq_job   *job;

    pid_t pid;

    assert(server);

    for (ulist = server->users; ulist; ulist = ulist->next) {
        for (glist = ulist->groups; glist; glist = glist->next) {
            group = &glist->group;

            if (group->group_status != MXQ_GROUP_STATUS_CANCELLED)
                continue;

            if (glist->jobs)
                mx_log_debug("Cancelling all running jobs in group=%s(%d):%lu",
                    group->user_name, group->user_uid, group->group_id);

            for (jlist = glist->jobs; jlist; jlist = jlist->next) {
                job = &jlist->job;

                pid = job->host_pid;
                if (pgrp)
                    pid = -pid;
                mx_log_info("  Sending signal=%d to job=%s(%d):%lu:%lu %s=%d",
                    sig,
                    group->user_name, group->user_uid, group->group_id, job->job_id,
                    pgrp?"pgrp":"pid", pid);
                kill(pid, sig);
            }
        }
    }
    return 0;
}

static int job_has_finished (struct mxq_server *server,struct mxq_group *g,struct mxq_job_list *job)
{
        int cnt=0;
        int res;
        struct mxq_job *j=&job->job;

        mxq_set_job_status_exited(server->mysql, j);

        if (j->job_status == MXQ_JOB_STATUS_FINISHED) {
            g->group_jobs_finished++;
        } else if(j->job_status == MXQ_JOB_STATUS_FAILED) {
            g->group_jobs_failed++;
        } else if(j->job_status == MXQ_JOB_STATUS_KILLED) {
            g->group_jobs_failed++;
        }

        mxq_job_set_tmpfilenames(g, j);

        if (!mx_streq(j->job_stdout, "/dev/null")) {
            res = rename(j->tmp_stdout, j->job_stdout);
            if (res == -1) {
                mx_log_err("   job=%s(%d):%lu:%lu host_pid=%d :: rename(stdout) failed: %m",
                        g->user_name, g->user_uid, g->group_id, j->job_id, j->host_pid);
            }
        }

        if (!mx_streq(j->job_stderr, "/dev/null") && !mx_streq(j->job_stderr, j->job_stdout)) {
            res = rename(j->tmp_stderr, j->job_stderr);
            if (res == -1) {
                mx_log_err("   job=%s(%d):%lu:%lu host_pid=%d :: rename(stderr) failed: %m",
                        g->user_name, g->user_uid, g->group_id, j->job_id, j->host_pid);
            }
        }

        cnt += job->group->slots_per_job;
        cpuset_clear_running(&server->cpu_set_running,&j->host_cpu_set);
        mxq_job_free_content(j);
        free(job);
        return cnt;
}

static int job_is_lost (struct mxq_server *server,struct mxq_group *g,struct mxq_job_list *job)
{
        int cnt=0;
        int res;
        struct mxq_job *j=&job->job;

        mxq_set_job_status_unknown(server->mysql, j);
        g->group_jobs_unknown++;

        mxq_job_set_tmpfilenames(g, j);

        if (!mx_streq(j->job_stdout, "/dev/null")) {
            res = rename(j->tmp_stdout, j->job_stdout);
            if (res == -1) {
                mx_log_err("   job=%s(%d):%lu:%lu host_pid=%d :: rename(stdout) failed: %m",
                        g->user_name, g->user_uid, g->group_id, j->job_id, j->host_pid);
            }
        }

        if (!mx_streq(j->job_stderr, "/dev/null") && !mx_streq(j->job_stderr, j->job_stdout)) {
            res = rename(j->tmp_stderr, j->job_stderr);
            if (res == -1) {
                mx_log_err("   job=%s(%d):%lu:%lu host_pid=%d :: rename(stderr) failed: %m",
                        g->user_name, g->user_uid, g->group_id, j->job_id, j->host_pid);
            }
        }

        cnt += job->group->slots_per_job;
        cpuset_clear_running(&server->cpu_set_running,&j->host_cpu_set);
        mxq_job_free_content(j);
        free(job);
        return cnt;
}

static char *fspool_get_filename (struct mxq_server *server,long unsigned int job_id)
{
    char *fspool_filename;
    mx_asprintf_forever(&fspool_filename,"%s/%lu.stat",server->finished_jobsdir,job_id);
    return fspool_filename;
}

void fspool_unlink(struct mxq_server *server,int job_id) {
    char *fspool_filename=fspool_get_filename(server,job_id);
    unlink(fspool_filename);
    free(fspool_filename);
}

static int fspool_process_file(struct mxq_server *server,char *filename,int job_id) {
    FILE *in;
    int res;

    pid_t pid;
    int   status;
    struct rusage rusage;
    struct timeval realtime;

    struct mxq_job_list *job;
    struct mxq_job *j;
    struct mxq_group *g;

    in=fopen(filename,"r");
    if (!in) {
        return -errno;
    }
    res=fscanf(in,"1 %d %d %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld",
        &pid,
        &status,
        &realtime.tv_sec,&realtime.tv_usec,
        &rusage.ru_utime.tv_sec,&rusage.ru_utime.tv_usec,
        &rusage.ru_stime.tv_sec,&rusage.ru_stime.tv_usec,
        &rusage.ru_maxrss,
        &rusage.ru_ixrss,
        &rusage.ru_idrss,
        &rusage.ru_isrss,
        &rusage.ru_minflt,
        &rusage.ru_majflt,
        &rusage.ru_nswap,
        &rusage.ru_inblock,
        &rusage.ru_oublock,
        &rusage.ru_msgsnd,
        &rusage.ru_msgrcv,
        &rusage.ru_nsignals,
        &rusage.ru_nvcsw,
        &rusage.ru_nivcsw);
    fclose(in);
    if (res!=22) {
        mx_log_err("%s : parse error (res=%d)",filename,res);
        if (!errno)
            errno=EINVAL;
        return -errno;
    }

    mx_log_info("job finished (via fspool) : job %d pid %d status %d",job_id,pid,status);

    job = server_remove_job_list_by_pid(server, pid);
    if (!job) {
        mx_log_warning("fspool_process_file: %s : job unknown on server",filename);
        return(-1);
    }
    j = &job->job;
    assert(job->group);
    assert(j->job_id=job_id);
    g = &job->group->group;

    j->stats_realtime=realtime;
    j->stats_status=status;
    j->stats_rusage=rusage;

    job_has_finished(server,g,job);
    fspool_unlink(server,job_id);
    return(0);
}

static int fspool_is_valid_name_parse(const char *name,int *job_id) {
    const char *c=name;
    if (!*c)
        return 0;
    if (!isdigit(*c++))
        return 0;
    while(isdigit(*c)) {
        c++;
    }
    if (strcmp(c,".stat")) {
        return 0;
    }
    if (job_id) {
        *job_id=atol(name);
    }
    return 1;
}

static int fspool_is_valid_name(const struct dirent *d)
{
    return fspool_is_valid_name_parse(d->d_name,NULL);
}

static int fspool_scan(struct mxq_server *server) {
    int cnt=0;
    int entries;
    struct dirent **namelist;
    int i;
    int res;


    entries=scandir(server->finished_jobsdir,&namelist,&fspool_is_valid_name,&alphasort);
    if (entries<0) {
        mx_log_err("scandir %s: %m",server->finished_jobsdir);
        return cnt;
    }

    for (i=0;i<entries;i++) {
        char *filename;
        int job_id;
        mx_asprintf_forever(&filename,"%s/%s",server->finished_jobsdir,namelist[i]->d_name);
        fspool_is_valid_name_parse(namelist[i]->d_name,&job_id);
        res=fspool_process_file(server,filename,job_id);
        if (res==0) {
            cnt++;
        }
        free(namelist[i]);
        free(filename);
    }

    free(namelist);
    return cnt;
}

static int file_exists(char *name) {
    int res;
    struct stat stat_buf;

    res=stat(name,&stat_buf);
    if (res<0) {
        if (errno==ENOENT) {
            return 0;
        } else {
            mx_log_warning("%s: %m",name);
            return 1;
        }
    } else {
        return 1;
    }
}

static int fspool_file_exists(struct mxq_server *server,uint64_t job_id) {
    _mx_cleanup_free_ char *fspool_filename=NULL;
    fspool_filename=fspool_get_filename(server,job_id);
    return file_exists(fspool_filename);
}

static int lost_scan_one(struct mxq_server *server)
{
    struct mxq_user_list  *ulist;
    struct mxq_group_list *glist;
    struct mxq_job_list   *jlist;

    struct mxq_job *job;

    int res;

    for (ulist = server->users; ulist; ulist = ulist->next) {
        for (glist = ulist->groups; glist; glist = glist->next) {
            for (jlist = glist->jobs; jlist; jlist = jlist->next) {
                job = &jlist->job;
                res = kill(job->host_pid, 0);
                if (res >= 0)
                    continue;

                /* PID is not */

                if (errno != ESRCH)
                    return -errno;

                if (!fspool_file_exists(server, job->job_id)) {
                    mx_log_warning("pid %u: process is gone. cancel job %d",
                                jlist->job.host_pid,
                                jlist->job.job_id);
                    server_remove_job_list_by_pid(server, job->host_pid);

                    job->job_status = MXQ_JOB_STATUS_UNKNOWN;

                    job_is_lost(server, &glist->group, jlist);
                    return 1;
                }
            }
        }
    }
    return 0;
}

static int lost_scan(struct mxq_server *server)
{
    int res;
    int count=0;
    do {
        res=lost_scan_one(server);
        if (res<0)
            return res;
        count+=res;
    } while (res>0);
    return count;
}


static int server_reload_running(struct mxq_server *server)
{
    _mx_cleanup_free_ struct mxq_job *jobs = NULL;

    struct mxq_job_list   *jlist;
    struct mxq_group_list *glist;
    struct mxq_user_list  *ulist;

    struct mxq_group *grps = NULL;
    struct mxq_group *group;
    struct mxq_job   *job;

    int group_cnt;
    int job_cnt;

    int j;

    job_cnt = mxq_load_jobs_running_on_server(server->mysql, &jobs, server->hostname, server->server_id);
    if (job_cnt < 0)
        return job_cnt;

    for (j=0; j < job_cnt; j++) {
        job = &jobs[j];

        job->stats_starttime.tv_sec = job->date_start;

        jlist = server_get_job_list_by_job_id(server, job->job_id);
        if (!jlist) {
            glist = server_get_group_list_by_group_id(server, job->group_id);
            if (!glist) {
                group_cnt = mxq_load_group(server->mysql, &grps, job->group_id);
                if (group_cnt != 1)
                    continue;
                group = &grps[0];
                ulist = server_find_user_by_uid(server, group->user_uid);
                if (!ulist) {
                    glist = _server_add_group(server, group);
                } else {
                    glist = _user_list_add_group(ulist, group);
                }
                mx_free_null(grps);
            }
            jlist = glist->jobs;
        }
        group_list_add_job(glist, job);
    }
    return job_cnt;
}

int catchall(struct mxq_server *server)
{
    struct mxq_job_list *jlist;
    struct mxq_job *job;
    struct mxq_group *group;

    struct rusage rusage;
    struct timeval now;
    int status;
    pid_t pid;
    int cnt = 0;
    int res;

    while (1) {
        siginfo_t siginfo;

        siginfo.si_pid = 0;
        res = waitid(P_ALL, 0, &siginfo, WEXITED|WNOHANG|WNOWAIT);

        if (res == -1) {
            /* no childs (left) => break loop */
            if (errno == ECHILD)
                break;
            mx_log_err("waitid: %m");
            return 0;
        }

        /* no (more) childs changed state => break loop */
        if (res == 0 && siginfo.si_pid == 0)
            break;

        assert(siginfo.si_pid > 1);

        jlist = server_get_job_list_by_pid(server, siginfo.si_pid);
        if (!jlist) {
            mx_log_warning("unknown pid returned.. si_pid=%d si_uid=%d si_code=%d si_status=%d getpgid(si_pid)=%d getsid(si_pid)=%d",
                            siginfo.si_pid,
                            siginfo.si_uid,
                            siginfo.si_code,
                            siginfo.si_status,
                            getpgid(siginfo.si_pid),
                            getsid(siginfo.si_pid));
            /* collect child, ignore status */
            pid = waitpid(siginfo.si_pid, NULL, WNOHANG);
            if (pid != siginfo.si_pid)
                mx_log_err("FIX ME BUG!!! pid=%d errno=%d (%m)", pid, errno);
            continue;
        }

        assert(jlist);
        assert(jlist->group);

        job   = &jlist->job;
        group = &jlist->group->group;

        if (fspool_file_exists(server, job->job_id)) {
            waitpid(siginfo.si_pid, &status, WNOHANG);
            continue;
        }
        mx_log_err("reaper died. status=%d. Cleaning up job from catchall.",status);
        job_list_remove_self(jlist);

        /* reap child and save new state */
        pid = wait4(siginfo.si_pid, &status, WNOHANG, &rusage);

        if (pid == -1) {
            mx_log_err("wait4: %m");
            return -1;
        }

        if (pid == 0) {
            mx_log_err("wait4: spurious pid=%d. Continuing anyway. Please FIX.", siginfo.si_pid);
            pid = siginfo.si_pid;
        }

        assert(pid == siginfo.si_pid);

        gettimeofday(&now, NULL);


        timersub(&now, &job->stats_starttime, &job->stats_realtime);
        job->stats_max_sumrss = jlist->max_sum_rss;
        job->stats_status = status;
        job->stats_rusage = rusage;

        mx_log_info("   job=%s(%d):%lu:%lu host_pid=%d stats_status=%d :: child process returned.",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    pid,
                    status);

        fspool_unlink(server, job->job_id);

        cnt += job_has_finished(server, group, jlist);
    }

    return cnt;
}

int load_groups(struct mxq_server *server)
{
    struct mxq_group_list *glist;
    struct mxq_group *grps;
    struct mxq_group *group;

    int grp_cnt;
    int total;
    int i;

    assert(server);

    grps = NULL;

    if (RUNNING_AS_ROOT)
        grp_cnt = mxq_load_running_groups(server->mysql, &grps);
    else
        grp_cnt = mxq_load_running_groups_for_user(server->mysql, &grps, getuid());

    for (i=0, total=0; i < grp_cnt; i++) {
        group = &grps[grp_cnt-i-1];
        glist = server_update_group(server, group);
        if (!glist) {
            mx_log_err("Could not add Group to control structures.");
        } else {
            total++;
        }
    }
    free(grps);

    remove_orphaned_group_lists(server);

    return total;
}

int recover_from_previous_crash(struct mxq_server *server)
{
    int res;

    assert(server);
    assert(server->mysql);
    assert(server->hostname);
    assert(server->server_id);

    res = mxq_unassign_jobs_of_server(server->mysql, server->hostname, server->server_id);
    if (res < 0) {
        mx_log_info("mxq_unassign_jobs_of_server() failed: %m");
        return res;
    }
    if (res > 0)
        mx_log_info("hostname=%s server_id=%s :: recovered from previous crash: unassigned %d jobs.",
            server->hostname, server->server_id, res);

    res=server_reload_running(server);
    if (res<0) {
        mx_log_err("recover: server_reload_running: %m");
        return res;
    }
    if (res>0)
        mx_log_info("recover: reload %d running jobs from database", res);

    res=fspool_scan(server);
    if (res<0) {
        mx_log_err("recover: server_fspool_scan: %m");
        return res;
    }
    if (res>0)
        mx_log_info("recover: processed %d finished jobs from fspool",res);

    res=lost_scan(server);
    if (res<0) {
        mx_log_err("recover: lost_scan: %m");
        return(res);
    }
    if (res>0)
        mx_log_warning("recover: %d jobs vanished from the system",res);

    return 0;
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

    if (sig == SIGQUIT) {
      global_sigquit_cnt++;
      return;
    }
}

int main(int argc, char *argv[])
{
    int group_cnt;

    struct mxq_server server;

    unsigned long slots_started  = 0;
    unsigned long slots_returned = 0;

    int res;
    int fail = 0;

    /*** server init ***/

    mx_log_level_set(MX_LOG_INFO);

    res = server_init(&server, argc, argv);
    if (res < 0) {
        mx_log_err("MXQ Server: Can't initialize server handle. Exiting.");
        exit(1);
    }

    mx_log_info("mxqd - " MXQ_VERSIONFULL);
    mx_log_info("  by Marius Tolzmann <marius.tolzmann@molgen.mpg.de> 2013-" MXQ_VERSIONDATE);
    mx_log_info("     and Donald Buczek <buczek@molgen.mpg.de> 2015-" MXQ_VERSIONDATE);
    mx_log_info("  Max Planck Institute for Molecular Genetics - Berlin Dahlem");
#ifdef MXQ_DEVELOPMENT
    mx_log_warning("DEVELOPMENT VERSION: Do not use in production environments.");
#endif
    mx_log_info("hostname=%s server_id=%s :: MXQ server started.", server.hostname, server.server_id);
    mx_log_info("  host_id=%s", server.host_id);
    mx_log_info("slots=%lu memory_total=%lu memory_avg_per_slot=%.0Lf memory_max_per_slot=%ld :: server initialized.",
                  server.slots, server.memory_total, server.memory_avg_per_slot, server.memory_max_per_slot);
    cpuset_log("cpu set available",&server.cpu_set_available);

    /*** database connect ***/

    mx_mysql_connect_forever(&(server.mysql));

    /*** main loop ***/

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGQUIT, sig_handler);
    signal(SIGHUP,  SIG_IGN);
    signal(SIGTSTP, SIG_IGN);
    signal(SIGTTIN, SIG_IGN);
    signal(SIGTTOU, SIG_IGN);
    signal(SIGCHLD, no_handler);

    res = recover_from_previous_crash(&server);
    if (res < 0) {
        mx_log_warning("recover_from_previous_crash() failed. Aborting execution.");
        fail = 1;
    }

    if (server.recoveronly)
        fail = 1;

    while (!global_sigint_cnt && !global_sigterm_cnt && !global_sigquit_cnt && !fail) {
        slots_returned = catchall(&server);
        slots_returned += fspool_scan(&server);

        if (slots_returned)
            mx_log_info("slots_returned=%lu :: Main Loop freed %lu slots.", slots_returned, slots_returned);

        if (slots_started || slots_returned) {
            server_dump(&server);
            slots_started = 0;
        }

        group_cnt = load_groups(&server);
        if (group_cnt)
           mx_log_debug("group_cnt=%d :: %d Groups loaded", group_cnt, group_cnt);

        killall_cancelled(&server, SIGTERM, 0);
        killall_over_time(&server);
        killall_over_memory(&server);

        if (!server.group_cnt) {
            assert(!server.jobs_running);
            assert(!group_cnt);
            mx_log_info("Nothing to do. Sleeping for a short while. (1 second)");
            sleep(1);
            continue;
        }

        if (server.slots_running == server.slots) {
            mx_log_info("All slots running. Sleeping for a short while (7 seconds).");
            sleep(7);
            continue;
        }

        slots_started = start_users(&server);
        if (slots_started)
            mx_log_info("slots_started=%lu :: Main Loop started %lu slots.", slots_started, slots_started);

        if (!slots_started && !slots_returned && !global_sigint_cnt && !global_sigterm_cnt) {
            if (!server.jobs_running) {
                mx_log_info("Tried Hard and nobody is doing anything. Sleeping for a long while (15 seconds).");
                sleep(15);
            } else {
                mx_log_info("Tried Hard. But have done nothing. Sleeping for a very short while (3 seconds).");
                sleep(3);
            }
            continue;
        }
    }
    /*** clean up ***/

    mx_log_info("global_sigint_cnt=%d global_sigterm_cnt=%d global_sigquit_cnt=%d: Exiting.", global_sigint_cnt, global_sigterm_cnt,global_sigquit_cnt);

    if (global_sigterm_cnt||global_sigint_cnt) {
        while (server.jobs_running) {
            slots_returned = catchall(&server);
            slots_returned += fspool_scan(&server);

            if (slots_returned) {
                mx_log_info("jobs_running=%lu slots_returned=%lu global_sigint_cnt=%d global_sigterm_cnt=%d :",
                    server.jobs_running, slots_returned, global_sigint_cnt, global_sigterm_cnt);
                continue;
            }
            if (global_sigint_cnt)
                killall(&server, SIGTERM, 1);

            killall_cancelled(&server, SIGTERM, 0);
            killall_over_time(&server);
            killall_over_memory(&server);
            mx_log_info("jobs_running=%lu global_sigint_cnt=%d global_sigterm_cnt=%d : Exiting. Wating for jobs to finish. Sleeping for a while.",
                server.jobs_running, global_sigint_cnt, global_sigterm_cnt);
            sleep(1);
        }
    }

    mx_mysql_finish(&(server.mysql));

    server_close(&server);

    mx_log_info("cu, mx.");

    mx_log_finish();

    return 0;
}
