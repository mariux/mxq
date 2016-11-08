
#include <stdio.h>

#include <assert.h>
#include <errno.h>

#include <mysql.h>

#include "mx_log.h"

#include "mx_util.h"
#include "mx_mysql.h"

#include "mxq_daemon.h"

#define DAEMON_FIELDS_CNT 18
#define DAEMON_FIELDS \
            " daemon_id," \
            " daemon_name," \
            " status," \
            " hostname," \
            " mxq_version," \
            " boot_id," \
            " pid_starttime," \
            " daemon_pid," \
            " daemon_slots," \
            " daemon_memory," \
            " daemon_maxtime" \
            " daemon_memory_limit_slot_soft," \
            " daemon_memory_limit_slot_hard," \
            " daemon_jobs_running," \
            " daemon_slots_running," \
            " daemon_threads_running," \
            " daemon_memory_used," \
            " UNIX_TIMESTAMP(mtime)        as mtime," \
            " UNIX_TIMESTAMP(daemon_start) as daemon_start," \
            " UNIX_TIMESTAMP(daemon_stop)  as daemon_stop"

#undef _to_string
#undef status_str
#define _to_string(s) #s
#define status_str(x) _to_string(x)

static int bind_result_daemon_fields(struct mx_mysql_bind *result, struct mxq_daemon *daemon)
{
    int res = 0;
    int idx = 0;

    res = mx_mysql_bind_init_result(result, DAEMON_FIELDS_CNT);
    assert(res >= 0);

    res += mx_mysql_bind_var(result, idx++, uint32, &(daemon->daemon_id));
    res += mx_mysql_bind_var(result, idx++, string, &(daemon->daemon_name));
    res += mx_mysql_bind_var(result, idx++,  uint8, &(daemon->status));
    res += mx_mysql_bind_var(result, idx++, string, &(daemon->hostname));
    res += mx_mysql_bind_var(result, idx++, string, &(daemon->mxq_version));
    res += mx_mysql_bind_var(result, idx++, string, &(daemon->boot_id));

    res += mx_mysql_bind_var(result, idx++, uint64, &(daemon->pid_starttime));
    res += mx_mysql_bind_var(result, idx++, uint32, &(daemon->daemon_pid));

    res += mx_mysql_bind_var(result, idx++, uint32, &(daemon->daemon_slots));
    res += mx_mysql_bind_var(result, idx++, uint64, &(daemon->daemon_memory));
    res += mx_mysql_bind_var(result, idx++, uint64, &(daemon->daemon_maxtime));

    res += mx_mysql_bind_var(result, idx++, uint64, &(daemon->daemon_memory_limit_slot_soft));
    res += mx_mysql_bind_var(result, idx++, uint64, &(daemon->daemon_memory_limit_slot_hard));

    res += mx_mysql_bind_var(result, idx++, uint32, &(daemon->daemon_jobs_running));
    res += mx_mysql_bind_var(result, idx++, uint32, &(daemon->daemon_slots_running));
    res += mx_mysql_bind_var(result, idx++, uint32, &(daemon->daemon_threads_running));
    res += mx_mysql_bind_var(result, idx++, uint64, &(daemon->daemon_memory_used));

    res += mx_mysql_bind_var(result, idx++,  int64, &(daemon->mtime.tv_sec));
    res += mx_mysql_bind_var(result, idx++,  int64, &(daemon->daemon_start.tv_sec));
    res += mx_mysql_bind_var(result, idx++,  int64, &(daemon->daemon_stop.tv_sec));

    return res;
}

void mxq_daemon_free_content(struct mxq_daemon *daemon)
{
        mx_free_null(daemon->daemon_name);
        mx_free_null(daemon->hostname);
        mx_free_null(daemon->mxq_version);
        mx_free_null(daemon->boot_id);
}

int mxq_daemon_register(struct mx_mysql *mysql, struct mxq_daemon *daemon)
{
    struct mx_mysql_stmt *stmt = NULL;
    unsigned long long num_rows = 0;
    unsigned long long insert_id = 0;
    int res;
    int idx;

    assert(daemon);

    assert(daemon->daemon_name);
    assert(daemon->hostname);
    assert(daemon->mxq_version);
    assert(daemon->boot_id);

    assert(*daemon->daemon_name);
    assert(*daemon->hostname);
    assert(*daemon->mxq_version);
    assert(*daemon->boot_id);

    assert(daemon->pid_starttime);

    assert(daemon->daemon_pid);
    assert(daemon->daemon_slots);
    assert(daemon->daemon_memory);
    assert(daemon->daemon_memory_limit_slot_soft <= daemon->daemon_memory_limit_slot_hard);
    assert(daemon->daemon_memory_limit_slot_hard <= daemon->daemon_memory);

    stmt = mx_mysql_statement_prepare(mysql,
            "INSERT INTO"
                " mxq_daemon"
            " SET"
                " daemon_name   = ?,"
                " status        = ?,"
                " hostname      = ?,"
                " mxq_version   = ?,"
                " boot_id       = ? ,"
                " pid_starttime = ?,"
                " daemon_pid    = ?,"
                " daemon_slots  = ?,"
                " daemon_memory = ?,"
                " daemon_maxtime= ?,"
                " daemon_memory_limit_slot_soft = ?,"
                " daemon_memory_limit_slot_hard = ?,"
                " daemon_jobs_running    = 0,"
                " daemon_slots_running   = 0,"
                " daemon_threads_running = 0,"
                " daemon_memory_used     = 0,"
                " mtime        = NULL,"
                " daemon_start = NULL,"
                " daemon_stop  = 0"
                );
    if (!stmt) {
        mx_log_err("mx_mysql_statement_prepare(): %m");
        mx_mysql_statement_close(&stmt);
        return -errno;
    }

    idx  = 0;

    res  = mx_mysql_statement_param_bind(stmt, idx++, string, &(daemon->daemon_name));
    res += mx_mysql_statement_param_bind(stmt, idx++,  uint8, &(daemon->status));
    res += mx_mysql_statement_param_bind(stmt, idx++, string, &(daemon->hostname));
    res += mx_mysql_statement_param_bind(stmt, idx++, string, &(daemon->mxq_version));
    res += mx_mysql_statement_param_bind(stmt, idx++, string, &(daemon->boot_id));

    res += mx_mysql_statement_param_bind(stmt, idx++, uint64, &(daemon->pid_starttime));
    res += mx_mysql_statement_param_bind(stmt, idx++, uint32, &(daemon->daemon_pid));

    res += mx_mysql_statement_param_bind(stmt, idx++, uint32, &(daemon->daemon_slots));
    res += mx_mysql_statement_param_bind(stmt, idx++, uint64, &(daemon->daemon_memory));
    res += mx_mysql_statement_param_bind(stmt, idx++, uint64, &(daemon->daemon_maxtime));

    res += mx_mysql_statement_param_bind(stmt, idx++, uint64, &(daemon->daemon_memory_limit_slot_soft));
    res += mx_mysql_statement_param_bind(stmt, idx++, uint64, &(daemon->daemon_memory_limit_slot_hard));

    assert(res ==0);

    res = mx_mysql_statement_execute(stmt, &num_rows);
    if (res < 0) {
        mx_log_err("mx_mysql_statement_execute(): %m");
        mx_mysql_statement_close(&stmt);
        return res;
    }

    assert(num_rows == 1);
    mx_mysql_statement_insert_id(stmt, &insert_id);
    assert(insert_id > 0);

    daemon->daemon_id = insert_id;

    res = mx_mysql_statement_close(&stmt);

    return (int)num_rows;
}

int mxq_daemon_shutdown(struct mx_mysql *mysql, struct mxq_daemon *daemon)
{
    assert(daemon);
    assert(daemon->daemon_id);

    struct mx_mysql_bind param = {0};
    char *query;
    int idx;
    int res;

    query = "UPDATE"
                " mxq_daemon"
            " SET"
                " mtime        = NULL,"
                " daemon_stop  = NULL,"
                " status       = " status_str(MXQ_DAEMON_STATUS_EXITED)
            " WHERE daemon_id = ?";

    res = mx_mysql_bind_init_param(&param, 1);
    assert(res == 0);

    idx  = 0;
    res  = 0;
    res += mx_mysql_bind_var(&param, idx++, uint32, &(daemon->daemon_id));
    assert(res == 0);

    res += mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    daemon->status = MXQ_DAEMON_STATUS_EXITED;

    return res;
}

int mxq_daemon_mark_crashed(struct mx_mysql *mysql, struct mxq_daemon *daemon)
{
    assert(daemon);
    assert(daemon->daemon_id);

    struct mx_mysql_bind param = {0};
    char *query;
    int idx;
    int res;

    query = "UPDATE"
                " mxq_daemon"
            " SET"
                " daemon_stop  = NULL,"
                " status       = " status_str(MXQ_DAEMON_STATUS_CRASHED)
            " WHERE status NOT IN ("
                    status_str(MXQ_DAEMON_STATUS_EXITED) ","
                    status_str(MXQ_DAEMON_STATUS_CRASHED) ")"
              " AND daemon_id  != ?"
              " AND hostname    = ?"
              " AND daemon_name = ?";

    res = mx_mysql_bind_init_param(&param, 3);
    assert(res == 0);

    idx  = 0;
    res  = 0;
    res += mx_mysql_bind_var(&param, idx++, uint32, &daemon->daemon_id);
    res += mx_mysql_bind_var(&param, idx++, string, &daemon->hostname);
    res += mx_mysql_bind_var(&param, idx++, string, &daemon->daemon_name);
    assert(res == 0);

    res += mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    return res;
}

int mxq_daemon_set_status(struct mx_mysql *mysql, struct mxq_daemon *daemon, uint8_t status)
{
    assert(daemon);
    assert(daemon->daemon_id);

    struct mx_mysql_bind param = {0};
    char *query;
    int idx;
    int res;

    /* set the same status only once every >= 5 minutes or return 0 */
    if (daemon->status == status)
        mx_within_rate_limit_or_return(5*60, 0);

    query = "UPDATE"
                " mxq_daemon"
            " SET"
                " mtime        = NULL,"
                " status       = ?"
            " WHERE daemon_id = ?";

    res = mx_mysql_bind_init_param(&param, 2);
    assert(res == 0);

    idx  = 0;
    res  = 0;
    res += mx_mysql_bind_var(&param, idx++,  uint8, &(status));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(daemon->daemon_id));
    assert(res == 0);

    res += mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    daemon->status = status;

    return res;
}

int mxq_daemon_update_statistics(struct mx_mysql *mysql, struct mxq_daemon *daemon)
{
    assert(daemon);
    assert(daemon->daemon_id);

    struct mx_mysql_bind param = {0};
    char *query;
    int idx;
    int res;

    query = "UPDATE"
                " mxq_daemon"
            " SET"
                " mtime = NULL,"
                " daemon_jobs_running    = ?,"
                " daemon_slots_running   = ?,"
                " daemon_threads_running = ?,"
                " daemon_memory_used     = ?"
            " WHERE daemon_id = ?";

    res = mx_mysql_bind_init_param(&param, 5);
    assert(res == 0);

    idx  = 0;
    res  = 0;
    res += mx_mysql_bind_var(&param, idx++, uint32, &(daemon->daemon_jobs_running));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(daemon->daemon_slots_running));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(daemon->daemon_threads_running));
    res += mx_mysql_bind_var(&param, idx++, uint64, &(daemon->daemon_memory_used));
    res += mx_mysql_bind_var(&param, idx++, uint32, &(daemon->daemon_id));
    assert(res == 0);

    res = mx_mysql_do_statement_noresult_retry_on_fail(mysql, query, &param);
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    return res;
}

int mxq_load_all_daemons(struct mx_mysql *mysql, struct mxq_daemon **daemons)
{
    struct mxq_daemon *daemons_tmp = NULL;
    struct mxq_daemon daemon_buf = {0};
    struct mx_mysql_bind result = {0};
    int res;

    assert(mysql);
    assert(daemons);
    assert(!(*daemons));

    char *query =
            "SELECT"
                DAEMON_FIELDS
            " FROM"
                " mxq_daemon";

    res = bind_result_daemon_fields(&result, &daemon_buf);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, NULL, &result, &daemon_buf, (void **)&daemons_tmp, sizeof(*daemons_tmp));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *daemons = daemons_tmp;
    return res;
}

int mxq_load_running_daemons(struct mx_mysql *mysql, struct mxq_daemon **daemons)
{
    struct mxq_daemon *daemons_tmp = NULL;
    struct mxq_daemon daemon_buf = {0};
    struct mx_mysql_bind result = {0};
    int res;

    assert(mysql);
    assert(daemons);
    assert(!(*daemons));

    char *query =
            "SELECT"
                DAEMON_FIELDS
            " FROM"
                " mxq_daemon"
            " WHERE"
                " daemon_jobs_runnning > 0"
              " OR"
                " daemon_stop = 0";

    res = bind_result_daemon_fields(&result, &daemon_buf);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, NULL, &result, &daemon_buf, (void **)&daemons_tmp, sizeof(*daemons_tmp));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *daemons = daemons_tmp;
    return res;
}

int mxq_load_running_daemons_by_host_and_name(struct mx_mysql *mysql, struct mxq_daemon **daemons, char *hostname, char *daemon_name)
{
    struct mxq_daemon *daemons_tmp = NULL;
    struct mxq_daemon daemon_buf = {0};
    struct mx_mysql_bind result = {0};
    struct mx_mysql_bind param = {0};
    int res;
    int idx;

    assert(mysql);
    assert(daemons);
    assert(!(*daemons));
    assert(hostname);
    assert(daemon_name);

    char *query =
            "SELECT"
                DAEMON_FIELDS
            " FROM"
                " mxq_daemon"
            " WHERE"
                " hostname = ?"
              " AND"
                " daemon_name = ?"
              " AND"
                " ("
                    " daemon_jobs_runnning > 0"
                " OR"
                    " daemon_stop = 0"
                " )";

    res = mx_mysql_bind_init_param(&param, 2);
    assert(res == 0);
    idx = 0;
    res = mx_mysql_bind_var(&param, idx++, string, &hostname);
    res = mx_mysql_bind_var(&param, idx++, string, &daemon_name);
    assert(res == 0);

    res = bind_result_daemon_fields(&result, &daemon_buf);
    assert(res == 0);

    res = mx_mysql_do_statement(mysql, query, NULL, &result, &daemon_buf, (void **)&daemons_tmp, sizeof(*daemons_tmp));
    if (res < 0) {
        mx_log_err("mx_mysql_do_statement(): %m");
        return res;
    }

    *daemons = daemons_tmp;
    return res;
}
