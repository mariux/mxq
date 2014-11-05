
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <errno.h>

#include <sys/file.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>

#include "mx_flock.h"

#include "mxq.h"
#include "mxq_group.h"
#include "mxq_job.h"
#include "mxq_mysql.h"
#include "mxq_server.h"

/**********************************************************************/

int server_init(struct mxq_server *server)
{
    int res;

    memset(server, 0, sizeof(*server));

    server->hostname = "localhost";
    server->server_id = "default";

    server->flock = mx_flock(LOCK_EX, "/dev/shm/mxq_server.%s.%s.lck", server->hostname, server->server_id);
    if (!server->flock) {
        return -1;
    }

    if (!server->flock->locked) {
        return -2;
    }

    server->slots = 48;
    server->memory_total = 128*1024;
    server->memory_max_per_slot = 4*1024;
    server->memory_avg_per_slot = server->memory_total / server->slots;

    if (server->memory_max_per_slot < server->memory_avg_per_slot)
       server->memory_max_per_slot = server->memory_avg_per_slot;

    return 1;
}

/**********************************************************************/

void group_init(struct mxq_group_list *group)
{
    struct mxq_server *s;
    struct mxq_group *g;

    long double memory_slots;

    assert(group);
    assert(group->user);
    assert(group->user->server);

    s = group->user->server;
    g = &group->group;

    group->memory_per_thread = (long double)g->job_memory / (long double) g->job_threads;
    group->memory_max_available = s->memory_total * s->memory_max_per_slot / group->memory_per_thread;

    if (group->memory_max_available > s->memory_total)
        group->memory_max_available = s->memory_total;

    group->slots_per_job = ceill((long double)g->job_memory / s->memory_avg_per_slot);

    if (group->slots_per_job < g->job_threads)
       group->slots_per_job = g->job_threads;

    memory_slots = group->memory_max_available / group->memory_per_thread;

    if (group->memory_per_thread > s->memory_max_per_slot) {
        group->jobs_max = memory_slots + 0.5;
    } else if (group->memory_per_thread > s->memory_avg_per_slot) {
        group->jobs_max = memory_slots + 0.5;
    } else {
        group->jobs_max = s->slots;
    }

    group->jobs_max /= g->job_threads;
    group->slots_max = group->jobs_max * group->slots_per_job;
    group->memory_max = group->jobs_max * g->job_memory;

    MXQ_LOG_INFO("  group=%s(%u):%lu jobs_max=%lu slots_max=%lu memory_max=%lu slots_per_job=%lu :: group initialized.\n",
                    g->user_name, g->user_uid, g->group_id, group->jobs_max, group->slots_max, group->memory_max, group->slots_per_job);
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

/**********************************************************************/

unsigned long start_job(struct mxq_group_list *group)
{
    struct mxq_server *server;
    struct mxq_job mxqjob;
    struct mxq_job_list *job;
    pid_t pid;
    int res;

    assert(group);
    assert(group->user);
    assert(group->user->server);

    server = group->user->server;

    res = mxq_job_load(server->mysql, &mxqjob, group->group.group_id, server->hostname, server->server_id);

    if (res) {

        mxq_mysql_close(server->mysql);

        pid = fork();
        if (pid < 0) {
            perror("fork");
            return 0;
        } else if (pid == 0) {
            int x;
            srandom(getpid());
            x = random() % 10;
//            printf("child sleeping for %d sec\n", x);
            sleep(x);
            exit(0);
        }

        gettimeofday(&mxqjob.stats_starttime, NULL);

        server->mysql = mxq_mysql_connect(&server->mmysql);

        mxqjob.host_pid = pid;
        mxqjob.host_slots = group->slots_per_job;
        res = mxq_job_update_status(server->mysql, &mxqjob, MXQ_JOB_STATUS_RUNNING);
        if (res <= 0) {
            perror("mxq_job_update_status(MXQ_JOB_STATUS_RUNNING)\n");
        }

        do {
            job = group_add_job(group, &mxqjob);
        } while (!job);

        return 1;
    }

//    printf("No job started in group %ld\n", group->group.group_id);
    return 0;
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

//    printf("starting jobs for user %s\n", mxqgrp->user_name);
//    printf("  - setting initial priority = %d\n", prio);
//    printf("  - setting initial slots to start = %ld\n", slots_to_start);

    MXQ_LOG_INFO(" user=%s(%d) slots_to_start=%ld job_limit=%d :: trying to start jobs for user.\n",
            mxqgrp->user_name, mxqgrp->user_uid, slots_to_start, job_limit);

    for (group=user->groups; group && slots_to_start > 0 && (!job_limit || jobs_started < job_limit); group=gnext) {

        mxqgrp  = &group->group;

        assert(group->jobs_running <= mxqgrp->group_jobs);
        assert(group->jobs_running <= group->jobs_max);

        if (group->jobs_running == mxqgrp->group_jobs) {
//            printf("    - skipping0 group %lu..\n", mxqgrp->group_id);
            gnext = group->next;
            if (!gnext && started) {
//                printf("   - rewinding0 ..\n");
                gnext = group->user->groups;
                started = 0;
            }
            continue;
        }

        if (group->jobs_running == group->jobs_max) {
//            printf("    - skipping1 group %lu..\n", mxqgrp->group_id);
            gnext = group->next;
            if (!gnext && started) {
//                printf("   - rewinding1 ..\n");
                gnext = group->user->groups;
                started = 0;
            }
            continue;
        }

        if (mxqgrp->group_jobs-mxqgrp->group_jobs_failed-mxqgrp->group_jobs_finished-mxqgrp->group_jobs_running == 0) {
//            printf("    - skipping2 group %lu..\n", mxqgrp->group_id);
            gnext = group->next;
            if (!gnext && started) {
//                printf("   - rewinding1.2 ..\n");
                gnext = group->user->groups;
                started = 0;
            }
            continue;
        }

        if (group->slots_per_job > slots_to_start) {
//            printf("    - skipping5 group %lu..\n", mxqgrp->group_id);
            gnext = group->next;
            if (!gnext && started) {
//                printf("   - rewinding1.3 ..\n");
                gnext = group->user->groups;
                started = 0;
            }
            continue;
        }

        if (mxqgrp->group_priority < prio) {
            if (started) {
//                printf("   - rewinding2 ..\n");
                gnext = group->user->groups;
                started = 0;
                continue;
            }
            prio = mxqgrp->group_priority;
//            printf("  - adjusting priority to %d\n", prio);
        }
        MXQ_LOG_INFO("  group=%s(%d):%lu slots_to_start=%ld slots_per_job=%lu :: trying to start job for group.\n",
                mxqgrp->user_name, mxqgrp->user_uid, mxqgrp->group_id, slots_to_start, group->slots_per_job);

        if (start_job(group)) {

            slots_to_start -= group->slots_per_job;
            jobs_started++;
            slots_started += group->slots_per_job;

            //MXQ_LOG_INFO("      -> started one job with %lu slots => %lu grp %lu usr %lu srv slots running (slots to start = %ld)\n", group->slots_per_job, group->slots_running, user->slots_running, server->slots_running, slots_to_start);
            started = 1;
        } else {
//            printf("XXXXXXXXXXXXXXXXXXXXX\n");
//            printf("XXX group_jobs          = %5ld\n", mxqgrp->group_jobs);
//            printf("XXX group_jobs_running  = %5ld\n", mxqgrp->group_jobs_running);
//            printf("XXX group_jobs_finished = %5ld\n", mxqgrp->group_jobs_finished);
//            printf("XXX group_jobs_failed   = %5ld\n", mxqgrp->group_jobs_failed);
//            printf("XXX jobs queued         = %5ld\n", mxqgrp->group_jobs-mxqgrp->group_jobs_failed-mxqgrp->group_jobs_finished-mxqgrp->group_jobs_running);
        }

        gnext = group->next;
        if (!gnext && started) {
//            printf("   - rewinding3 ..\n");
            gnext = group->user->groups;
            started = 0;
        }
    }
    return slots_started;
}

/**********************************************************************/

void start_users(struct mxq_server *server)
{
    long slots_to_start;
    unsigned long slots_started;
    int started = 0;

    struct mxq_user_list  *user, *unext=NULL;
    struct mxq_group_list *group, *gnext;

    assert(server);

/*
    for (user=server->users; user; user=user->next) {
        printf("user: server(%p)<-user(%p) %s\n", user->server, user, user->groups[0].group.user_name);
        for (group=user->groups; group; group=group->next) {
            printf("   group: user(%p)<-group(%p) %lu\n", group->user, group, group->group.group_id);
            printf("      job_threads = %d\n", group->group.job_threads);
            printf("      job_memory = %lu\n", group->group.job_memory);
            printf("      memory_per_thread = %.0Lf\n", group->memory_per_thread);
            printf("      memory_avg_per_slot = %.0Lf\n", group->user->server->memory_avg_per_slot);
            printf("      slots_per_job = %lu\n", group->slots_per_job);
            printf("      memory_max_available = %.0Lf / %lu\n", group->memory_max_available, group->user->server->memory_total);
            printf("      memory_max = %lu\n", group->memory_max);
            printf("      slots_max = %lu / %lu\n", group->slots_max, group->user->server->slots);
            printf("      jobs_max = %lu\n", group->jobs_max);
        }
    }
*/

    MXQ_LOG_INFO("=== starting jobs on free_slots=%lu slots for user_cnt=%lu users\n", server->slots - server->slots_running, server->user_cnt);

    for (user=server->users; user; user=user->next) {

        slots_to_start = server->slots / server->user_cnt - user->slots_running;

        if (slots_to_start < 0)
            continue;

        if (server->slots - server->slots_running < slots_to_start)
            slots_to_start = server->slots - server->slots_running;

        slots_started = start_user(user, 0, slots_to_start);

//        printf("  => %ld of %ld slots started (%ld unused slots)\n", slots_started, slots_to_start, slots_to_start-slots_started);
    }

    for (user=server->users; user && server->slots - server->slots_running; user=unext) {
        slots_to_start = server->slots - server->slots_running;
        slots_started  = start_user(user, 1, slots_to_start);

        started = (started || slots_started);

//        printf("  => %ld of %ld slots started (%ld unused slots)\n", slots_started, slots_to_start, slots_to_start-slots_started);

        unext = user->next;
        if (!unext && started) {
//            printf("  *** user rewind\n\n");
            unext = server->users;
            started = 0;
        }
    }

    printf("server-stats:\n\t%6lu of %6lu MiB\tallocated\n", server->memory_used, server->memory_total);
    printf("\t%6lu of %6lu slots\tallocated for %lu running threads (%lu jobs)\n", server->slots_running, server->slots, server->threads_running, server->jobs_running);
}

/**********************************************************************/

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

    mx_funlock(server->flock);
}

int catchall(struct mxq_server *server) {

    struct rusage rusage;
    struct timeval now;
    int status;
    pid_t pid;
    int cnt = 0;
    struct mxq_job_list *job;

    while (server->jobs_running) {
        pid = wait3(&status, WNOHANG, &rusage);

        if (pid < 0) {
            MXQ_LOG_ERROR("wait3: %m\n");
            return -1;
        }

        if (pid == 0)
            return 0;

        job = server_remove_job_by_pid(server, pid);
        if (!job) {
            MXQ_LOG_ERROR("unknown pid returned.. pid=%d\n", pid);
            continue;
        }

        gettimeofday(&now, NULL);

        timersub(&now, &job->job.stats_starttime, &job->job.stats_realtime);

        job->job.stats_status   = status;
        job->job.stats_rusage   = rusage;

        MXQ_LOG_INFO("   job=%s(%d):%lu:%lu host_pid=%d stats_status=%d :: child process returned.\n",
                job->group->group.user_name, job->group->group.user_uid, job->group->group.group_id, job->job.job_id, pid, status);

        mxq_job_update_status(server->mysql, &job->job, MXQ_JOB_STATUS_EXIT);

        mxq_job_free_content(&job->job);
        free(job);
        cnt++;
    }

    return cnt;
}

/**********************************************************************/
static void no_handler(int sig) {}

int main(int argc, char *argv[])
{
    struct mxq_group *mxqgroups;

    int group_cnt;

    struct mxq_server server;
    struct mxq_group_list *group;

    int i;
    int res;

    /*** server init ***/

    signal(SIGCHLD, no_handler);

    res = server_init(&server);
    if (res < 0) {
        if (res == -2) {
            MXQ_LOG_ERROR("MXQ Server '%s' on host '%s' is already running. Exiting.\n", server.hostname, server.server_id);
            exit(2);
        }
        MXQ_LOG_ERROR("MXQ Server: Can't initialize server handle. Exiting.\n");
        exit(1);
    }

    MXQ_LOG_INFO("hostname=%s server_id=%s :: MXQ server started.\n", server.hostname, server.server_id);
    MXQ_LOG_INFO("slots=%lu memory_total=%lu memory_avg_per_slot=%.0Lf memory_max_per_slot=%ld :: server initialized.\n",
                  server.slots, server.memory_total, server.memory_avg_per_slot, server.memory_max_per_slot);

    /*** database connect ***/

    server.mmysql.default_file  = NULL;
    server.mmysql.default_group = "mxq_submit";
    server.mysql = mxq_mysql_connect(&server.mmysql);

    /*** main loop ***/

    group_cnt = mxq_group_load_groups(server.mysql, &mxqgroups);

    //MXQ_LOG_INFO("group_cnt=%d :: groups loaded.\n", group_cnt);

    for (i=0; i<group_cnt; i++) {
        group = server_update_groupdata(&server, &mxqgroups[group_cnt-i-1]);
//        printf("new group %p user_cnt=%lu group_cnt=%lu job_cnt=%lu\n", group, server.user_cnt, server.group_cnt, server.job_cnt);
    }
    free(mxqgroups);

    start_users(&server);

    while (server.jobs_running) {
        int cnt;

        cnt = catchall(&server);

        if (!cnt) {
            MXQ_LOG_INFO("waiting for %ld jobs (%ld slots) to finish ...\n", server.jobs_running, server.slots_running);
            sleep(20);
        }
    }

    /*** clean up ***/

    mxq_mysql_close(server.mysql);

    server_close(&server);

    return 0;
}
