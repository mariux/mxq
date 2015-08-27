#ifndef __MXQ_GROUP_H__
#define __MXQ_GROUP_H__ 1

#include <stdint.h>
#include <mysql.h>

#include "mx_mysql.h"

struct mxq_group {
   uint64_t  group_id;

   char *    group_name;
   unsigned long  _group_name_length;

   uint8_t   group_status;
   uint64_t  group_flags;
   uint16_t  group_priority;

   uint32_t  user_uid;
   char *    user_name;
   unsigned long  _user_name_length;

   uint32_t  user_gid;
   char *    user_group;
   unsigned long  _user_group_length;

   char *    job_command;
   unsigned long  _job_command_length;

   uint16_t  job_threads;
   uint64_t  job_memory;
   uint32_t  job_time;

   uint64_t  group_jobs;
   uint64_t  group_jobs_inq;
   uint64_t  group_jobs_running;
   uint64_t  group_jobs_finished;
   uint64_t  group_jobs_failed;
   uint64_t  group_jobs_cancelled;
   uint64_t  group_jobs_unknown;

   uint64_t  group_jobs_restarted;

   uint64_t  group_slots_running;

   uint64_t  stats_max_maxrss;

   struct timeval stats_max_utime;
   struct timeval stats_max_stime;
   struct timeval stats_max_real;

   uint64_t  stats_wait_sec;
   uint64_t  stats_run_sec;
   uint64_t  stats_idle_sec;
};

#define MXQ_GROUP_STATUS_OK            0
#define MXQ_GROUP_STATUS_CANCELLED    99

#define MXQ_GROUP_FLAG_CLOSED (1<<0)

#define MXQ_GROUP_FLAG_HAS_DEPENDENCY (1<<1)
#define MXQ_GROUP_FLAG_IS_DEPENDENCY  (1<<2)

void mxq_group_free_content(struct mxq_group *g);

inline uint64_t mxq_group_jobs_done(struct mxq_group *g);
inline uint64_t mxq_group_jobs_active(struct mxq_group *g);
inline uint64_t mxq_group_jobs_inq(struct mxq_group *g);

int mxq_load_group(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t group_id);
int mxq_load_all_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups);
int mxq_load_active_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups);
int mxq_load_all_groups_for_user(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t user_uid);
int mxq_load_active_groups_for_user(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t user_uid);
int mxq_load_running_groups(struct mx_mysql *mysql, struct mxq_group **mxq_groups);
int mxq_load_running_groups_for_user(struct mx_mysql *mysql, struct mxq_group **mxq_groups, uint64_t user_uid);

#endif
