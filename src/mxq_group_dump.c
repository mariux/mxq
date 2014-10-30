
#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <assert.h>

#include <mysql.h>

#include "mxq_util.h"
#include "mxq_mysql.h"

int main(int argc, char *argv[])
{
    struct mxq_mysql mmysql;
    MYSQL *mysql;
    struct mxq_group *groups;
    int group_cnt;
    int i;

    long server_slots = 48;
    long server_memory_total = 8*1024;
    long double server_memory_avg_per_slot = server_memory_total / server_slots;
    long server_memory_max_per_slot = 1*1024;

    assert(server_memory_max_per_slot >= server_memory_avg_per_slot);


    mmysql.default_file  = NULL;
    mmysql.default_group = "mxq_submit";

    mysql = mxq_mysql_connect(&mmysql);

    group_cnt = mxq_group_load_groups(mysql, &groups);

    for (i=0; i<group_cnt; i++) {
        long double job_memory_per_thread = (long double)groups[i].job_memory / (long double)groups[i].job_threads;

        long double factor_overcommit = (long double)server_memory_max_per_slot / (long double)job_memory_per_thread;
        long double job_memory_total = (long double)server_memory_total * factor_overcommit;
        long double job_memory_slots = job_memory_total / (long double)job_memory_per_thread;
        long job_slots;

        if (job_memory_per_thread > server_memory_max_per_slot) {
            printf(" *** MEMORY LIMIT 1 *** \n");
            job_slots = job_memory_slots + 0.5;
        } else if (job_memory_per_thread > server_memory_avg_per_slot) {
            printf(" *** MEMORY LIMIT 2 *** \n");
            job_slots = job_memory_slots + 0.5;
        } else {
            job_slots = server_slots;
        }

        job_slots /= groups[i].job_threads;
        job_slots *= groups[i].job_threads;




        printf("Mt = %ld / %d = %Lf\n", groups[i].job_memory, groups[i].job_threads, job_memory_per_thread);
        printf("Fo = %Lf / %ld = %Lf\n", job_memory_per_thread, server_memory_max_per_slot, factor_overcommit);

        printf("%ld\t%s\t%s\tthreads=%d\tmem=%ld\ttime=%d\tpri=%d\tcnt=%ld\tMt=%Lf\t%ld\t%s\n\n",
                                groups[i].group_id,   groups[i].user_name, groups[i].group_name,     groups[i].job_threads,
                                groups[i].job_memory, groups[i].job_time,  groups[i].group_priority, groups[i].group_jobs,
                                job_memory_per_thread, job_slots, groups[i].job_command);
        mxq_group_free_content(&groups[i]);
    }

    free(groups);

    mxq_mysql_close(mysql);

    return 1;
};

