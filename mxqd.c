
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

#include "mxqd_control.h"

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
volatile sig_atomic_t global_sigrestart_cnt=0;


int mxq_redirect_output(char *stdout_fname, char *stderr_fname);
void server_free(struct mxq_server *server);

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
    char *reexecuting;
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
                MX_OPTION_NO_ARG("no-daemonize",        10),
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

    memset(server, 0, sizeof(*server));

    reexecuting = getenv("MXQ_HOSTID");
    if (reexecuting)
        mx_log_warning("Welcome back. Server is restarting. Ignoring some options by default now.");

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
            return -EX_USAGE;
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
                if (arg_logdir && *arg_logdir != '/') {
                    mx_log_err("Invalid argument supplied for option --log-dir '%s': Path has to be absolute", optctl.optarg);
                    return -EX_USAGE;
                }
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

            case 10:
                arg_daemonize = 0;
                break;

            case 'V':
                mxq_print_generic_version();
                return -EX_USAGE;

            case 'h':
                print_usage();
                return -EX_USAGE;

            case 'j':
                if (mx_strtoul(optctl.optarg, &arg_threads_total) < 0) {
                    mx_log_err("Invalid argument supplied for option --slots '%s': %m", optctl.optarg);
                    return -EX_USAGE;
                }
                break;

            case 'm':
                if (mx_strtoul(optctl.optarg, &arg_memory_total) < 0) {
                    unsigned long long int bytes;

                    if(mx_strtobytes(optctl.optarg, &bytes) < 0) {
                        mx_log_err("Invalid argument supplied for option --memory '%s': %m", optctl.optarg);
                        return -EX_USAGE;
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
                        return -EX_USAGE;
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

    if (reexecuting) {
        arg_daemonize = 0; /* we already daemonized */
        arg_nolog     = 1; /* we reuse last log */
    }

    if (arg_daemonize && arg_nolog) {
        mx_log_err("Error while using conflicting options --daemonize and --no-log at once.");
        return -EX_USAGE;
    }

    server->hostname = arg_hostname;
    server->server_id = arg_server_id;
    server->initial_path = arg_initial_path;
    server->initial_tmpdir = arg_initial_tmpdir;
    server->recoveronly = arg_recoveronly;

    server->flock = mx_flock(LOCK_EX, "/dev/shm/mxqd.%s.%s.lck", server->hostname, server->server_id);
    if (!server->flock) {
        mx_log_err("mx_flock(/dev/shm/mxqd.%s.%s.lck) failed: %m", server->hostname, server->server_id);
        return -EX_UNAVAILABLE;
    }

    if (!server->flock->locked) {
        mx_log_err("MXQ Server '%s' on host '%s' is already running. Exiting.", server->server_id, server->hostname);
        return -EX_UNAVAILABLE;
    }

    mx_asprintf_forever(&server->finished_jobsdir,"%s/%s",MXQ_FINISHED_JOBSDIR,server->server_id);
    res=mx_mkdir_p(server->finished_jobsdir,0700);
    if (res<0) {
        mx_log_err("MAIN: mkdir %s failed: %m. Exiting.",MXQ_FINISHED_JOBSDIR);
        return -EX_IOERR;
    }

    if (arg_daemonize) {
        res = daemon(0, 1);
        if (res == -1) {
            mx_log_err("MAIN: daemon(0, 1) failed: %m. Exiting.");
            return -EX_OSERR;
        }
    }

    if (arg_pidfile) {
        res = write_pid_to_file(arg_pidfile);
        if (res < 0) {
            mx_log_err("MAIN: pidfile (%s) setup failed: %m.  Exiting.", arg_pidfile);
            return -EX_IOERR;
        }

        server->pidfilename = arg_pidfile;
    }

    res = prctl(PR_SET_CHILD_SUBREAPER, 1);
    if (res == -1) {
        mx_log_err("MAIN: prctl(PR_SET_CHILD_SUBREAPER) setup failed: %m.  Exiting.");
        return -EX_OSERR;
    }

    setup_stdin("/dev/null");

    if (!arg_nolog) {
        if (!arg_logdir)
            arg_logdir = MXQ_LOGDIR;

        if (access(arg_logdir, R_OK|W_OK|X_OK)) {
            if (!RUNNING_AS_ROOT)
                mx_log_warning("Running mxqd as non-root user.");
            mx_log_err("MAIN: can't write to '%s': %m", arg_logdir);
            return -EX_IOERR;
        }
        res = setup_cronolog("/usr/sbin/cronolog", arg_logdir, "mxqd_log", "%Y/mxqd_log-%Y-%m");
        if (!res) {
            if (!RUNNING_AS_ROOT)
                mx_log_warning("Running mxqd as non-root user.");
            mx_log_err("MAIN: cronolog setup failed. exiting.");
            return -EX_IOERR;
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
        return -EX_OSERR;
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

    return 0;
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
    signal(SIGUSR1, SIG_DFL);
    signal(SIGUSR2, SIG_DFL);
}

static int init_child_process(struct mxq_group_list *glist, struct mxq_job *job)
{
    struct mxq_server *server;
    struct mxq_group  *group;
    struct passwd *passwd;
    int res;
    int fh;
    struct rlimit rlim;

    assert(job);
    assert(glist);
    assert(glist->user);
    assert(glist->user->server);

    server = glist->user->server;
    group  = &glist->group;

    reset_signals();

    passwd = getpwuid(group->user_uid);
    if (!passwd) {
        mx_log_err("job=%s(%d):%lu:%lu getpwuid(): %m",
            group->user_name, group->user_uid, group->group_id, job->job_id);
        return 0;
    }

    if (!mx_streq(passwd->pw_name, group->user_name)) {
        mx_log_warning("job=%s(%d):%lu:%lu user_uid=%d does not map to user_name=%s but to pw_name=%s: Invalid user mapping",
                        group->user_name,
                        group->user_uid,
                        group->group_id,
                        job->job_id,
                        group->user_uid,
                        group->user_name,
                        passwd->pw_name);

        passwd = getpwnam(group->user_name);
        if (!passwd) {
            mx_log_err("job=%s(%d):%lu:%lu getpwnam(): %m",
                group->user_name, group->user_uid, group->group_id, job->job_id);
            return 0;
        }
        if (passwd->pw_uid != group->user_uid) {
            mx_log_fatal("job=%s(%d):%lu:%lu user_name=%s does not map to uid=%d but to pw_uid=%d. Aborting Child execution.",
                            group->user_name,
                            group->user_uid,
                            group->group_id,
                            job->job_id,
                            group->user_name,
                            group->user_uid,
                            passwd->pw_uid);

            return 0;
        }
    }

    /** prepare environment **/

    res = clearenv();
    if (res != 0) {
        mx_log_err("job=%s(%d):%lu:%lu clearenv(): %m",
            group->user_name, group->user_uid, group->group_id, job->job_id);
        return 0;
    }

    mx_setenv_forever("USER",     group->user_name);
    mx_setenv_forever("USERNAME", group->user_name);
    mx_setenv_forever("LOGNAME",  group->user_name);
    mx_setenv_forever("PATH",     server->initial_path);
    mx_setenv_forever("TMPDIR",   server->initial_tmpdir);
    mx_setenv_forever("PWD",      job->job_workdir);
    mx_setenv_forever("HOME",     passwd->pw_dir);
    mx_setenv_forever("SHELL",    passwd->pw_shell);
    mx_setenv_forever("HOSTNAME", mx_hostname());
    mx_setenvf_forever("JOB_ID",      "%lu",    job->job_id);
    mx_setenvf_forever("MXQ_JOBID",   "%lu",    job->job_id);
    mx_setenvf_forever("MXQ_THREADS", "%d",     group->job_threads);
    mx_setenvf_forever("MXQ_SLOTS",   "%lu",    glist->slots_per_job);
    mx_setenvf_forever("MXQ_MEMORY",  "%lu",    group->job_memory);
    mx_setenvf_forever("MXQ_TIME",    "%d",     group->job_time);
    mx_setenv_forever("MXQ_HOSTID",   server->host_id);
    mx_setenv_forever("MXQ_HOSTNAME", server->hostname);
    mx_setenv_forever("MXQ_SERVERID", server->server_id);

    fh = open("/proc/self/loginuid", O_WRONLY|O_TRUNC);
    if (fh == -1) {
        mx_log_err("job=%s(%d):%lu:%lu open(%s) failed: %m",
            group->user_name, group->user_uid, group->group_id, job->job_id, "/proc/self/loginuid");
        return 0;
    }
    dprintf(fh, "%d", group->user_uid);
    close(fh);

    /* set memory limits */
    rlim.rlim_cur = group->job_memory*1024*1024;
    rlim.rlim_max = group->job_memory*1024*1024;

    res = setrlimit(RLIMIT_AS, &rlim);
    if (res == -1)
        mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_AS, ...) failed: %m",
                    group->user_name, group->user_uid, group->group_id, job->job_id);

    res = setrlimit(RLIMIT_DATA, &rlim);
    if (res == -1)
        mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_DATA, ...) failed: %m",
                    group->user_name, group->user_uid, group->group_id, job->job_id);

    res = setrlimit(RLIMIT_RSS, &rlim);
    if (res == -1)
        mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_RSS, ...) failed: %m",
                    group->user_name, group->user_uid, group->group_id, job->job_id);

    /* disable core files */
    rlim.rlim_cur = 0;
    rlim.rlim_cur = 0;

    res = setrlimit(RLIMIT_CORE,  &rlim);
    if (res == -1)
        mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_CORE, ...) failed: %m",
                    group->user_name, group->user_uid, group->group_id, job->job_id);

    /* set single threaded time limits */
    if (group->job_threads == 1) {
            /* set cpu time limits - hardlimit is 105% of softlimit */
            rlim.rlim_cur = group->job_time*60;
            rlim.rlim_cur = group->job_time*63;

            res = setrlimit(RLIMIT_CPU,  &rlim);
            if (res == -1)
                mx_log_err("job=%s(%d):%lu:%lu setrlimit(RLIMIT_CPU, ...) failed: %m",
                            group->user_name, group->user_uid, group->group_id, job->job_id);
    }

    if(RUNNING_AS_ROOT) {

        res = initgroups(passwd->pw_name, group->user_gid);
        if (res == -1) {
            mx_log_err("job=%s(%d):%lu:%lu initgroups() failed: %m",
                group->user_name, group->user_uid, group->group_id, job->job_id);
            return 0;
        }

        res = setregid(group->user_gid, group->user_gid);
        if (res == -1) {
            mx_log_err("job=%s(%d):%lu:%lu setregid(%d, %d) failed: %m",
                group->user_name, group->user_uid, group->group_id, job->job_id,
                group->user_gid, group->user_gid);
            return 0;
        }

        res = setreuid(group->user_uid, group->user_uid);
        if (res == -1) {
            mx_log_err("job=%s(%d):%lu:%lu setreuid(%d, %d) failed: %m",
                group->user_name, group->user_uid, group->group_id, job->job_id,
                group->user_uid, group->user_uid);
            return 0;
        }
    }

    res = chdir(job->job_workdir);
    if (res == -1) {
        mx_log_err("job=%s(%d):%lu:%lu chdir(%s) failed: %m",
            group->user_name, group->user_uid, group->group_id, job->job_id,
            job->job_workdir);
        return 0;
    }

    umask(job->job_umask);

    res=sched_setaffinity(0,sizeof(job->host_cpu_set),&job->host_cpu_set);
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
            mx_log_err("%s: unlink() failed: %m", fname);
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

int user_process(struct mxq_group_list *glist, struct mxq_job *job)
{
    int res;
    char **argv;

    struct mxq_group *group;

    group = &glist->group;

    res = init_child_process(glist, job);
    if (!res)
        return(-1);

    mxq_job_set_tmpfilenames(group, job);

    res = mxq_redirect_input("/dev/null");
    if (res < 0) {
        mx_log_err("   job=%s(%d):%lu:%lu mxq_redirect_input() failed (%d): %m",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    res);
        return(res);
    }

    res = mxq_redirect_output(job->tmp_stdout, job->tmp_stderr);
    if (res < 0) {
        mx_log_err("   job=%s(%d):%lu:%lu mxq_redirect_output() failed (%d): %m",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    res);
        return(res);
    }

    argv = mx_strvec_from_str(job->job_argv_str);
    if (!argv) {
        mx_log_err("job=%s(%d):%lu:%lu Can't recaculate commandline. str_to_strvev(%s) failed: %m",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    job->job_argv_str);
        return -errno;
    }

    res = execvp(argv[0], argv);
    mx_log_err("job=%s(%d):%lu:%lu execvp(\"%s\", ...): %m",
                group->user_name,
                group->user_uid,
                group->group_id,
                job->job_id,
                argv[0]);
    return res;
}

int reaper_process(struct mxq_server *server,struct mxq_group_list *glist, struct mxq_job *job) {
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

    struct mxq_group *group;

    group = &glist->group;

    reset_signals();

    signal(SIGINT,  SIG_IGN);
    signal(SIGTERM, SIG_IGN);
    signal(SIGHUP,  SIG_IGN);
    signal(SIGXCPU, SIG_IGN);

    res = setsid();
    if (res < 0) {
        mx_log_warning("reaper_process setsid: %m");
        return res;
    }

    res = prctl(PR_SET_CHILD_SUBREAPER, 1);
    if (res < 0) {
        mx_log_err("set subreaper: %m");
        return res;
    }

    pid = fork();
    if (pid < 0) {
        mx_log_err("fork: %m");
        return pid;
    } else if (pid == 0) {
        mx_log_info("starting user process.");
        res = user_process(glist, job);
        _exit(EX__MAX+1);
    }
    gettimeofday(&job->stats_starttime, NULL);

    while (1) {
        waited_pid = wait(&waited_status);
        if (waited_pid < 0) {
            if (errno==ECHILD) {
                break;
            } else {
                mx_log_warning("reaper: wait: %m");
                sleep(1);
            }
        }
        if (waited_pid == pid) {
            status = waited_status;
        }
    }
    gettimeofday(&now, NULL);
    timersub(&now, &job->stats_starttime, &realtime);
    res = getrusage(RUSAGE_CHILDREN, &rusage);
    if (res < 0) {
        mx_log_err("reaper: getrusage: %m");
        return(res);
    }

    mx_asprintf_forever(&finished_job_filename, "%s/%lu.stat", server->finished_jobsdir, job->job_id);
    mx_asprintf_forever(&finished_job_tmpfilename, "%s.tmp", finished_job_filename);

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
    mx_free_null(job->host_cpu_set_str);
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

        mx_log_info("starting reaper process.");
        mx_mysql_finish(&server->mysql);

        res = reaper_process(server, glist, job);

        mxq_job_free_content(job);

        mx_log_info("shutting down reaper, bye bye.");
        mx_log_finish();
        server_free(server);
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

long start_user_with_least_running_global_slot_count(struct mxq_server *server)
{
    struct mxq_user_list *ulist;
    struct mxq_group_list *glist;
    unsigned long slots_started = 0;
    unsigned long slots_free;
    unsigned long global_slots_per_user;
    int waiting = 0;

    assert(server);

    if (!server->user_cnt)
        return 0;

    server_sort_users_by_running_global_slot_count(server);
    slots_free = server->slots - server->slots_running;

    if (!slots_free)
        return 0;

    global_slots_per_user = server->global_slots_running / server->user_cnt;

    for (ulist = server->users; ulist; ulist = ulist->next) {
        /* if other users are waiting and this user is already using
         * more slots then avg user in cluster do not start anything
         * (next users are using even more atm because list is sorted) */
        if (waiting && ulist->global_slots_running > global_slots_per_user)
            return -1;

        slots_started = start_user(ulist, 1, slots_free);
        if (slots_started)
            return slots_started;

        if (waiting)
            continue;

        for (glist = ulist->groups; glist; glist = glist->next) {
            if (glist->jobs_max > glist->jobs_running) {
                waiting = 1;
                break;
            }
        }
    }
    return 0;
}

/**********************************************************************/

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
        mx_log_info("    user=%s(%d) slots_running=%lu global_slots_running=%lu global_threads_running=%lu",
                group->user_name,
                group->user_uid,
                ulist->slots_running,
                ulist->global_slots_running,
                ulist->global_threads_running);

        for (glist = ulist->groups; glist; glist = glist->next) {
            group = &glist->group;

            mx_log_info("        group=%s(%d):%lu %s jobs_max=%lu slots_per_job=%d jobs_in_q=%lu",
                group->user_name,
                group->user_uid,
                group->group_id,
                group->group_name,
                glist->jobs_max,
                glist->slots_per_job,
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
    mx_log_info("global_slots_running=%lu global_threads_running=%lu",
                server->global_slots_running,
                server->global_threads_running);
    cpuset_log("cpu set running",
                &server->cpu_set_running);
    mx_log_info("====================== SERVER DUMP END ======================");
}

void server_free(struct mxq_server *server)
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

    mx_free_null(server->boot_id);
    mx_free_null(server->host_id);
    mx_free_null(server->finished_jobsdir);
    mx_flock_free(server->flock);

    mx_log_finish();
}

void server_close(struct mxq_server *server)
{
    if (server->pidfilename)
        unlink(server->pidfilename);

    mx_funlock(server->flock);
    server->flock = NULL;

    server_free(server);
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

                mx_log_info("killall_over_time(): Sending signal=XCPU to job=%s(%d):%lu:%lu pgrp=%d",
                    group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                kill(-pid, SIGCONT);
                kill(-pid, SIGXCPU);

                if (delta.tv_sec <= group->job_time*63)
                    continue;

                mx_log_info("killall_over_time(): Sending signal=TERM to job=%s(%d):%lu:%lu pgrp=%d",
                    group->user_name, group->user_uid, group->group_id, job->job_id, pid);
                kill(-pid, SIGCONT);
                kill(-pid, SIGTERM);

                if (delta.tv_sec <= group->job_time*66+60*10)
                    continue;

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
                if (jlist->max_sumrss/1024 > group->job_memory)
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

                if (jlist->max_sumrss < memory)
                    jlist->max_sumrss = memory;

                if (jlist->max_sumrss/1024 <= group->job_memory)
                    continue;

                mx_log_info("killall_over_memory(): used(%lluMiB) > requested(%lluMiB): Sending signal=%d to job=%s(%d):%lu:%lu pgrp=%d",
                    jlist->max_sumrss/1024,
                    group->job_memory,
                    signal,
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    job->host_pid);

                kill(-job->host_pid, SIGCONT);
                kill(-job->host_pid, signal);
            }
        }
    }
    mx_proc_tree_free(&ptree);
    return 0;
}

int killall_cancelled(struct mxq_server *server)
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
                mx_log_info("  Sending signal=TERM to job=%s(%d):%lu:%lu pgrp=%d",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    pid);
                kill(-pid, SIGCONT);
                kill(-pid, SIGTERM);
            }
        }
    }
    return 0;
}

static void rename_outfiles(struct mxq_group *group, struct mxq_job *job)
{
    int res;

    mxq_job_set_tmpfilenames(group, job);

    if (!mx_streq(job->job_stdout, "/dev/null")) {
        res = rename(job->tmp_stdout, job->job_stdout);
        if (res == -1) {
            mx_log_err("   job=%s(%d):%lu:%lu host_pid=%d :: rename(stdout) failed: %m",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    job->host_pid);
        }
    }

    if (!mx_streq(job->job_stderr, "/dev/null") && !mx_streq(job->job_stderr, job->job_stdout)) {
        res = rename(job->tmp_stderr, job->job_stderr);
        if (res == -1) {
            mx_log_err("   job=%s(%d):%lu:%lu host_pid=%d :: rename(stderr) failed: %m",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    job->host_pid);
        }
    }
}

static int job_has_finished(struct mxq_server *server, struct mxq_group *group, struct mxq_job_list *jlist)
{
        int cnt;
        struct mxq_job *job;

        job=&jlist->job;

        mxq_set_job_status_exited(server->mysql, job);

        rename_outfiles(group, job);

        cnt = jlist->group->slots_per_job;
        cpuset_clear_running(&server->cpu_set_running, &job->host_cpu_set);
        mxq_job_free_content(job);
        free(jlist);
        return cnt;
}

static int job_is_lost(struct mxq_server *server,struct mxq_group *group, struct mxq_job_list *jlist)
{
        int cnt;
        struct mxq_job *job;

        assert(jlist->group);
        assert(!jlist->next);

        job = &jlist->job;

        mxq_set_job_status_unknown(server->mysql, job);
        group->group_jobs_unknown++;

        rename_outfiles(group, job);

        cnt = jlist->group->slots_per_job;
        cpuset_clear_running(&server->cpu_set_running, &job->host_cpu_set);
        mxq_job_free_content(job);
        free(jlist);
        return cnt;
}

static char *fspool_get_filename (struct mxq_server *server,long unsigned int job_id)
{
    char *fspool_filename;
    mx_asprintf_forever(&fspool_filename,"%s/%lu.stat",server->finished_jobsdir,job_id);
    return fspool_filename;
}

static int fspool_process_file(struct mxq_server *server,char *filename,int job_id) {
    FILE *in;
    int res;

    pid_t pid;
    int   status;
    struct rusage rusage;
    struct timeval realtime;

    struct mxq_job_list *jlist;
    struct mxq_job *job;
    struct mxq_group *group;

    in=fopen(filename,"r");
    if (!in) {
        return -errno;
    }
    errno=0;
    res=fscanf(in,"1 %d %d %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld",
        &pid,
        &status,
        &realtime.tv_sec,
        &realtime.tv_usec,
        &rusage.ru_utime.tv_sec,
        &rusage.ru_utime.tv_usec,
        &rusage.ru_stime.tv_sec,
        &rusage.ru_stime.tv_usec,
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

    jlist = server_remove_job_list_by_pid(server, pid);
    if (!jlist) {
        mx_log_warning("fspool_process_file: %s : job unknown on server", filename);
        return -(errno=ENOENT);
    }

    job = &jlist->job;
    if (job->job_id != job_id) {
        mx_log_warning("fspool_process_file: %s: job_id(pid)[%ld] != job_id(filename)[%ld]",
                        filename,
                        job->job_id,
                        job_id);
        return -(errno=EINVAL);
    }

    assert(jlist->group);

    group = &jlist->group->group;

    job->stats_max_sumrss = jlist->max_sumrss;

    job->stats_realtime = realtime;
    job->stats_status   = status;
    job->stats_rusage   = rusage;

    job_has_finished(server, group, jlist);
    unlink(filename);
    return(0);
}

static int fspool_is_valid_name_parse(const char *name, unsigned long long int *job_id) {
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
        *job_id = strtoull(name, NULL, 10);
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
    unsigned long long int job_id;
    char *filename;


    entries=scandir(server->finished_jobsdir,&namelist,&fspool_is_valid_name,&alphasort);
    if (entries<0) {
        mx_log_err("scandir %s: %m",server->finished_jobsdir);
        return cnt;
    }

    for (i=0;i<entries;i++) {
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


static int load_running_jobs(struct mxq_server *server)
{
    _mx_cleanup_free_ struct mxq_job *jobs = NULL;

    struct mxq_job_list   *jlist;
    struct mxq_group_list *glist;

    struct mxq_job   *job;

    int job_cnt;

    int j;

    job_cnt = mxq_load_jobs_running_on_server(server->mysql, &jobs, server->hostname, server->server_id);
    if (job_cnt < 0)
        return job_cnt;

    for (j=0; j < job_cnt; j++) {
        job = &jobs[j];

        job->stats_starttime.tv_sec = job->date_start;

        jlist = server_get_job_list_by_job_id(server, job->job_id);
        if (jlist)
            continue;

        glist = server_get_group_list_by_group_id(server, job->group_id);
        if (!glist) {
            mx_log_fatal("BUG17: group %lu of job %lu not loaded. skipping job.",
                        job->group_id, job->job_id);
            return -(errno=EUCLEAN);
        } else {
            group_list_add_job(glist, job);
        }
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
        job->stats_max_sumrss = jlist->max_sumrss;
        job->stats_status = status;
        job->stats_rusage = rusage;

        mx_log_info("   job=%s(%d):%lu:%lu host_pid=%d stats_status=%d :: child process returned.",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    job->job_id,
                    pid,
                    status);

        cnt += job_has_finished(server, group, jlist);
    }

    return cnt;
}

int load_running_groups(struct mxq_server *server)
{
    struct mxq_group_list *glist;
    struct mxq_group *grps;
    struct mxq_group *group;
    struct passwd *passwd;

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

        passwd = getpwnam(group->user_name);
        if (!passwd) {
            mx_log_fatal("group=%s(%d):%lu Can't find user with name '%s': getpwnam(): %m. Ignoring group.",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    group->user_name);
            continue;
        }

        glist = server_update_group(server, group);
        if (!glist) {
            mx_log_err("Could not add Group to control structures.");
        } else {
            total++;
        }
    }
    free(grps);

    server_remove_orphaned_groups(server);

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

    res = load_running_groups(server);
    mx_log_info("recover: %d running groups loaded.", res);

    res = load_running_jobs(server);
    if (res < 0) {
        mx_log_err("recover: load_running_jobs: %m");
        return res;
    }
    if (res > 0)
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

    if (sig == SIGUSR1) {
      global_sigrestart_cnt++;
      return;
    }
}

int main(int argc, char *argv[])
{
    int group_cnt;

    struct mxq_server __server;
    struct mxq_server *server = &__server;

    unsigned long slots_started  = 0;
    unsigned long slots_returned = 0;

    int res;
    int fail = 0;

    int saved_argc;
    _mx_cleanup_free_ char *saved_argv_str = NULL;
    _mx_cleanup_free_ char *saved_cwd = NULL;


    /*** server init ***/

    saved_argc     = argc;
    saved_argv_str = mx_strvec_to_str(argv);
    saved_cwd      = get_current_dir_name();

    mx_log_level_set(MX_LOG_INFO);

    res = server_init(server, argc, argv);
    if (res < 0) {
        server_close(server);
        exit(-res);
    }

    mx_log_info("mxqd - " MXQ_VERSIONFULL);
    mx_log_info("  by Marius Tolzmann <marius.tolzmann@molgen.mpg.de> 2013-" MXQ_VERSIONDATE);
    mx_log_info("     and Donald Buczek <buczek@molgen.mpg.de> 2015-" MXQ_VERSIONDATE);
    mx_log_info("  Max Planck Institute for Molecular Genetics - Berlin Dahlem");
#ifdef MXQ_DEVELOPMENT
    mx_log_warning("DEVELOPMENT VERSION: Do not use in production environments.");
#endif
    mx_log_info("hostname=%s server_id=%s :: MXQ server started.",
                    server->hostname,
                    server->server_id);
    mx_log_info("  host_id=%s", server->host_id);
    mx_log_info("slots=%lu memory_total=%lu memory_avg_per_slot=%.0Lf memory_max_per_slot=%ld :: server initialized.",
                    server->slots,
                    server->memory_total,
                    server->memory_avg_per_slot,
                    server->memory_max_per_slot);
    cpuset_log("cpu set available", &(server->cpu_set_available));

    /*** database connect ***/

    mx_mysql_connect_forever(&(server->mysql));

    /*** main loop ***/

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGQUIT, sig_handler);
    signal(SIGUSR1, sig_handler);
    signal(SIGTSTP, SIG_IGN);
    signal(SIGTTIN, SIG_IGN);
    signal(SIGTTOU, SIG_IGN);
    signal(SIGCHLD, no_handler);

    res = recover_from_previous_crash(server);
    if (res < 0) {
        mx_log_warning("recover_from_previous_crash() failed. Aborting execution.");
        fail = 1;
    }

    if (server->recoveronly)
        fail = 1;

    server_dump(server);

    while (!global_sigint_cnt && !global_sigterm_cnt && !global_sigquit_cnt && !global_sigrestart_cnt && !fail) {
        slots_returned  = catchall(server);
        slots_returned += fspool_scan(server);

        if (slots_returned)
            mx_log_info("slots_returned=%lu :: Main Loop freed %lu slots.", slots_returned, slots_returned);

        if (slots_started || slots_returned) {
            server_dump(server);
            slots_started = 0;
        }

        group_cnt = load_running_groups(server);
        if (group_cnt)
           mx_log_debug("group_cnt=%d :: %d Groups loaded", group_cnt, group_cnt);

        killall_cancelled(server);
        killall_over_time(server);
        killall_over_memory(server);

        if (!server->group_cnt) {
            assert(!server->jobs_running);
            assert(!group_cnt);
            mx_log_info("Nothing to do. Sleeping for a short while. (1 second)");
            sleep(1);
            continue;
        }

        if (server->slots_running == server->slots) {
            mx_log_info("All slots running. Sleeping for a short while (7 seconds).");
            sleep(7);
            continue;
        }

        slots_started = start_user_with_least_running_global_slot_count(server);
        if (slots_started == -1) {
            mx_log_debug("no slots_started => we have users waiting for free slots.");
            slots_started = 0;
        } else if (slots_started) {
            mx_log_info("slots_started=%lu :: Main Loop started %lu slots.", slots_started, slots_started);
        }

        if (!slots_started && !slots_returned && !global_sigint_cnt && !global_sigterm_cnt) {
            if (!server->jobs_running) {
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

    mx_log_info("global_sigint_cnt=%d global_sigterm_cnt=%d global_sigquit_cnt=%d global_sigrestart_cnt=%d: Exiting.",
                    global_sigint_cnt,
                    global_sigterm_cnt,
                    global_sigquit_cnt,
                    global_sigrestart_cnt);

    while (server->jobs_running && (global_sigterm_cnt || global_sigint_cnt) && !global_sigrestart_cnt) {
        slots_returned  = catchall(server);
        slots_returned += fspool_scan(server);

        if (slots_returned) {
            mx_log_info("jobs_running=%lu slots_returned=%lu global_sigint_cnt=%d global_sigterm_cnt=%d :",
                            server->jobs_running,
                            slots_returned,
                            global_sigint_cnt,
                            global_sigterm_cnt);
            continue;
        }
        if (global_sigint_cnt)
            killall(server, SIGTERM, 1);

        killall_cancelled(server);
        killall_over_time(server);
        killall_over_memory(server);
        mx_log_info("jobs_running=%lu global_sigint_cnt=%d global_sigterm_cnt=%d : Exiting. Wating for jobs to finish. Sleeping for a while.",
                            server->jobs_running,
                            global_sigint_cnt,
                            global_sigterm_cnt);
        sleep(1);
    }

    mx_mysql_finish(&(server->mysql));

    server_close(server);

    while (global_sigrestart_cnt) {
        char **saved_argv;
        saved_argc     = argc;
        saved_argv_str = mx_strvec_to_str(argv);
        saved_cwd      = get_current_dir_name();

        mx_log_info("Reexecuting mxqd... ");

        res = chdir(saved_cwd);
        if (res < 0) {
            mx_log_fatal("Aborting restart: chdir(%s) failed: %m", saved_cwd);
            break;
        }

        saved_argv = mx_strvec_from_str(saved_argv_str);
        if (!saved_argv) {
            mx_log_fatal("Can't recaculate commandline. str_to_strvev(%s) failed: %m", saved_argv_str);
            break;
        }

        mx_log_info("-------------------------------------------------------------");
        mx_log_info(" Reexecuting %s", saved_argv_str);
        mx_log_info("-------------------------------------------------------------");

        res = execvp(saved_argv[0], saved_argv);
        mx_log_fatal("execvp(\"%s\", ...): %m", saved_argv[0]);
        break;

    }

    mx_log_info("cu, mx.");

    mx_log_finish();

    exit(0);
}
