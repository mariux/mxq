#ifndef _MXQD_CONTROL_H
#define _MXQD_CONTROL_H

#include "mxqd.h"
#include "mxq_group.h"
#include "mxq_job.h"

struct mxq_job_list   *server_remove_job_list_by_pid(struct mxq_server *server, pid_t pid);
struct mxq_group_list *server_update_group(struct mxq_server *server, struct mxq_group *group);

struct mxq_group_list *server_get_group_list_by_group_id(struct mxq_server *server, uint64_t group_id);
struct mxq_job_list   *server_get_job_list_by_job_id(struct mxq_server *server, uint64_t job_id);
struct mxq_job_list   *server_get_job_list_by_pid(struct mxq_server *server, pid_t pid);

void server_sort_users_by_running_global_slot_count(struct mxq_server *server);

struct mxq_job_list *group_list_add_job(struct mxq_group_list *glist, struct mxq_job *job);
int server_remove_orphaned_groups(struct mxq_server *server);

void job_list_remove_self(struct mxq_job_list *jlist);



#endif
