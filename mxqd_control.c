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

    if (jobs_max > server->slots / slots_per_job)
        jobs_max = server->slots / slots_per_job;

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

struct mxq_user_list *server_find_user_by_uid(struct mxq_server *server, uint32_t uid)
{
    assert(server);

    return _user_list_find_by_uid(server->users, uid);
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

    glist->next = ulist->groups;
    ulist->groups = glist;

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
