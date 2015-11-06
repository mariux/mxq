#ifndef _MXQD_CONTROL_H
#define _MXQD_CONTROL_H

#include "mxqd.h"
#include "mxq_group.h"
#include "mxq_job.h"

void job_list_remove_self(struct mxq_job_list *jlist);
struct mxq_job_list *server_remove_job_list_by_pid(struct mxq_server *server, pid_t pid);
struct mxq_user_list *server_find_user_by_uid(struct mxq_server *server, uint32_t uid);
struct mxq_group_list *_group_list_find_by_group(struct mxq_group_list *glist, struct mxq_group *group);
struct mxq_job_list *group_list_add_job(struct mxq_group_list *glist, struct mxq_job *job);
struct mxq_group_list *server_update_group(struct mxq_server *server, struct mxq_group *group);

struct mxq_group_list *server_get_group_list_by_group_id(struct mxq_server *server, uint64_t group_id);
struct mxq_job_list *server_get_job_list_by_job_id(struct mxq_server *server, uint64_t job_id);
struct mxq_job_list *server_get_job_list_by_pid(struct mxq_server *server, pid_t pid);

/*
static void _group_list_init(struct mxq_group_list *glist)
static struct mxq_user_list *_user_list_find_by_uid(struct mxq_user_list *ulist, uint32_t uid)

static struct mxq_group_list *_user_list_update_group(struct mxq_user_list *ulist, struct mxq_group *group)
*/
int server_remove_orphaned_groups(struct mxq_server *server);

struct mxq_group_list *_server_add_group(struct mxq_server *server, struct mxq_group *group);
struct mxq_group_list *_user_list_add_group(struct mxq_user_list *ulist, struct mxq_group *group);


void server_sort_users_by_running_global_slot_count(struct mxq_server *server);

#endif
