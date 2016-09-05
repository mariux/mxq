#define _GNU_SOURCE

#include <sched.h>
#include <math.h>
#include <assert.h>

#include "mxq_job.h"
#include "mxq_group.h"

#include "mxqd.h"

static void _group_list_init(struct mxq_group_list *glist)
{
    struct mxq_server *server;
    struct mxq_group *group;

    long double memory_per_job_thread;
    long double memory_available_for_group;

    unsigned long slots_per_job;
    unsigned long slots_per_job_memory;
    unsigned long slots_per_job_cpu;
    unsigned long jobs_max;
    unsigned long slots_max;
    unsigned long memory_max;

    assert(glist);
    assert(glist->user);
    assert(glist->user->server);

    server = glist->user->server;
    group  = &glist->group;

    memory_per_job_thread = (long double)group->job_memory / (long double)group->job_threads;

    /* max_memory_per_server_slot_soft < memory_per_job_thread => limit total memory for group default: avg_memory_per_server_slot*/
    /* max_memory_per_server_slot_hard < memory_per_job_thread => do not start jobs for group  default: memory_total */

    /* memory_available_for_group = memory_total * max_memory_per_server_slot_soft / memory_per_job_thread */
    memory_available_for_group = (long double)server->memory_total * (long double)server->memory_limit_slot_soft / memory_per_job_thread;

    if (memory_available_for_group > (long double)server->memory_total)
        memory_available_for_group = (long double)server->memory_total;

    /* memory_slots_per_job = memory_per_job / memory_per_server_slot */
    /* cpu_slots_per_job    = job_threads */
    /* slots_per_job        = max(memory_slots_per_job, cpu_slots_per_job) */

    slots_per_job_memory = (unsigned long)ceill((long double)group->job_memory / server->memory_avg_per_slot);
    slots_per_job_cpu    = group->job_threads;

    if (slots_per_job_memory < slots_per_job_cpu)
        slots_per_job = slots_per_job_cpu;
    else
        slots_per_job = slots_per_job_memory;

    if (memory_per_job_thread > server->memory_limit_slot_hard) {
        jobs_max = 0;
    } else if (memory_per_job_thread > server->memory_avg_per_slot) {
        jobs_max = (unsigned long)ceill(memory_available_for_group / (long double)group->job_memory);
    } else {
        jobs_max = server->slots / group->job_threads;
    }

    if (jobs_max > server->slots / slots_per_job)
        jobs_max = server->slots / slots_per_job;

    /* limit maximum number of jobs on user/group request */
    if (group->job_max_per_node && jobs_max > group->job_max_per_node)
        jobs_max = group->job_max_per_node;

    /* max time constraint on server */
    if (server->maxtime && group->job_time > server->maxtime)
        jobs_max=0;

    slots_max  = jobs_max * slots_per_job;
    memory_max = jobs_max * group->job_memory;

    if (glist->memory_per_job_thread != memory_per_job_thread
       || glist->memory_available_for_group != memory_available_for_group
       || glist->slots_per_job != slots_per_job
       || glist->jobs_max != jobs_max
       || glist->slots_max != slots_max
       || glist->memory_max != memory_max) {
        mx_log_info("  group=%s(%u):%lu jobs_max=%lu slots_max=%lu memory_max=%lu slots_per_job=%lu memory_per_job_thread=%Lf :: group %sinitialized.",
                    group->user_name,
                    group->user_uid,
                    group->group_id,
                    jobs_max,
                    slots_max,
                    memory_max,
                    slots_per_job,
                    memory_per_job_thread,
                    glist->orphaned ? "re" : "");
    }

    glist->memory_per_job_thread      = memory_per_job_thread;
    glist->memory_available_for_group = memory_available_for_group;

    glist->slots_per_job = slots_per_job;

    glist->jobs_max   = jobs_max;
    glist->slots_max  = slots_max;
    glist->memory_max = memory_max;

    glist->orphaned = 0;
}

struct mxq_group_list *server_get_group_list_by_group_id(struct mxq_server *server, uint64_t group_id)
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

struct mxq_job_list *server_get_job_list_by_job_id(struct mxq_server *server, uint64_t job_id)
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

struct mxq_job_list *server_get_job_list_by_pid(struct mxq_server *server, pid_t pid)
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
        jlist->next = NULL;

        glist->job_cnt--;
        ulist->job_cnt--;
        server->job_cnt--;

        glist->slots_running  -= job->host_slots;
        ulist->slots_running  -= job->host_slots;
        server->slots_running -= job->host_slots;

        glist->threads_running  -= group->job_threads;
        ulist->threads_running  -= group->job_threads;
        server->threads_running -= group->job_threads;

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

struct mxq_job_list *server_remove_job_list_by_job_id(struct mxq_server *server, uint64_t job_id)
{
    struct mxq_job_list *jlist;

    assert(server);

    jlist = server_get_job_list_by_job_id(server, job_id);
    if (jlist) {
        job_list_remove_self(jlist);
    }
    return jlist;
}

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

struct mxq_job_list *group_list_add_job(struct mxq_group_list *glist, struct mxq_job *job)
{
    struct mxq_server *server;

    struct mxq_job_list  *jlist;
    struct mxq_user_list *ulist;

    struct mxq_group *group;

    assert(glist);
    assert(glist->user);
    assert(glist->user->server);
    assert(job->job_status == MXQ_JOB_STATUS_RUNNING || job->job_status == MXQ_JOB_STATUS_LOADED);

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

    glist->jobs_running++;
    ulist->jobs_running++;
    server->jobs_running++;

    glist->memory_used  += group->job_memory;
    ulist->memory_used  += group->job_memory;
    server->memory_used += group->job_memory;

    return jlist;
}

/*
 * given a mxq_user_list element, find the tail of its groups list.
 * returns the address of the pointer containing NULL
 */
static struct mxq_group_list **group_list_tail_ptr(struct mxq_user_list *ulist)
{
    struct mxq_group_list **tail_ptr=&ulist->groups;
    while (*tail_ptr) {
        tail_ptr=&(*tail_ptr)->next;
    }
    return tail_ptr;
}

/*
 * create a new mxq_group_list element from a mxq_group and add it to the users groups
 * update user and server counters
 */
struct mxq_group_list *_user_list_add_group(struct mxq_user_list *ulist, struct mxq_group *group)
{
    struct mxq_group_list *glist;
    struct mxq_server *server;

    assert(ulist);
    assert(ulist->server);

    server = ulist->server;

    glist = mx_calloc_forever(1, sizeof(*glist));

    memcpy(&glist->group, group, sizeof(*group));

    glist->user = ulist;

    *group_list_tail_ptr(ulist)=glist;

    ulist->group_cnt++;
    server->group_cnt++;

    glist->global_slots_running   = group->group_slots_running;
    glist->global_threads_running = group->group_jobs_running * group->job_threads;

    ulist->global_slots_running   += glist->global_slots_running;
    ulist->global_threads_running += glist->global_threads_running;

    server->global_slots_running   += glist->global_slots_running;
    server->global_threads_running += glist->global_threads_running;

    _group_list_init(glist);

    return glist;
}

struct mxq_group_list *_server_add_group(struct mxq_server *server, struct mxq_group *group)
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

static struct mxq_group_list *_user_list_update_group(struct mxq_user_list *ulist, struct mxq_group *group)
{
    struct mxq_group_list *glist;
    struct mxq_server *server;

    assert(ulist);
    assert(group);
    assert(ulist->server);

    server = ulist->server;

    glist = _group_list_find_by_group(ulist->groups, group);
    if (!glist) {
        return _user_list_add_group(ulist, group);
    }

    server->global_slots_running   -= glist->global_slots_running;
    server->global_threads_running -= glist->global_threads_running;

    ulist->global_slots_running   -= glist->global_slots_running;
    ulist->global_threads_running -= glist->global_threads_running;

    glist->global_slots_running   = group->group_slots_running;
    glist->global_threads_running = group->group_jobs_running * group->job_threads;

    ulist->global_slots_running   += glist->global_slots_running;
    ulist->global_threads_running += glist->global_threads_running;

    server->global_slots_running   += glist->global_slots_running;
    server->global_threads_running += glist->global_threads_running;

    mxq_group_free_content(&glist->group);

    memcpy(&glist->group, group, sizeof(*group));

    _group_list_init(glist);

    return glist;
}

struct mxq_group_list *server_update_group(struct mxq_server *server, struct mxq_group *group)
{
    struct mxq_user_list *ulist;

    ulist = _user_list_find_by_uid(server->users, group->user_uid);
    if (!ulist) {
        return _server_add_group(server, group);
    }

    return _user_list_update_group(ulist, group);
}

int server_remove_orphaned_groups(struct mxq_server *server)
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
            ulist->global_slots_running   -= glist->global_slots_running;
            ulist->global_threads_running -= glist->global_threads_running;
            server->group_cnt--;
            server->global_slots_running   -= glist->global_slots_running;
            server->global_threads_running -= glist->global_threads_running;
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

void server_sort_users_by_running_global_slot_count(struct mxq_server *server)
{
    struct mxq_user_list *ulist;
    struct mxq_user_list *unext;
    struct mxq_user_list *uprev;
    struct mxq_user_list *uroot;
    struct mxq_user_list *current;

    assert(server);

    if (!server->user_cnt)
        return;

    for (ulist = server->users, uroot = NULL; ulist; ulist = unext) {
        unext = ulist->next;

        ulist->next = NULL;

        if (!uroot) {
            uroot = ulist;
            continue;
        }

        for (current = uroot, uprev = NULL; (current || uprev); uprev = current, current = current->next) {
            if (!current) {
                uprev->next = ulist;
                break;
            }
            if (ulist->global_slots_running > current->global_slots_running) {
                continue;
            }
            if (ulist->global_slots_running == current->global_slots_running
                && ulist->global_threads_running > current->global_threads_running) {
                continue;
            }

            ulist->next = current;

            if (!uprev) {
                uroot = ulist;
            } else {
                uprev->next = ulist;
            }
            break;
        }
    }

    server->users = uroot;
}
