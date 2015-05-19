
#include <assert.h>
#include <errno.h>

#include "mx_log.h"
#include "mx_mysql.h"

int main(int argc, char *argv[])
{
    struct mx_mysql *mysql = NULL;
    struct mx_mysql_stmt *stmt = NULL;
    uint64_t group_id = 444;
    uint64_t group_id1 = 1234;
    uint64_t group_id2 = 123;
    char *group_name = NULL;

    int res;

    mx_log_level_set(MX_LOG_DEBUG);

    mx_log_debug("group_id = %d", group_id);
    mx_log_debug("group_id2 = %d", group_id2);

    res = mx_mysql_init(&mysql);
    assert(res == 0);

    res = mx_mysql_connect(mysql);
    assert(res == 0);

    //res = mx_mysql_connect(mysql);
    //assert(res == 0);

    res = mx_mysql_statement_init(mysql, &stmt);
    assert(res == 0);

    res = mx_mysql_statement_prepare(stmt, "SELECT group_id, group_id, group_name FROM mxq_group where group_id = ?");
    assert(res == 0);
    mx_log_debug("field_count = %d", mx_mysql_statement_field_count(stmt));
    mx_log_debug("param_count = %d", mx_mysql_statement_param_count(stmt));


    res = mx_mysql_statement_param_bind(stmt, 0, uint64, &group_id);
    assert(res == 0);

    res = mx_mysql_statement_execute(stmt);
    assert(res == 0);

    res = mx_mysql_statement_result_bind(stmt, 0, uint64, &group_id1);
    assert(res == 0);
    res = mx_mysql_statement_result_bind(stmt, 1, uint64, &group_id2);
    assert(res == 0);
    res = mx_mysql_statement_result_bind(stmt, 2, string, &group_name);
    assert(res == 0);



    //mx_mysql_statement_param_bind(stmt, 0, uint64, &group_id);

    res = mx_mysql_statement_fetch(stmt);
    assert(res == 0);

    mx_log_debug("mx_mysql_statement_fetch(): %m");


    mx_log_debug("&group_name = 0x%x", &group_name);
    mx_log_debug("group_name = %s", group_name);

//#define debug_value(fmt, v)  mx_log_debug("debug_value: " #v " = " fmt, v)

    //debug_value("%d", stmt->result.data[0].length);

    mx_debug_value("%d",   stmt->result.data[2].length);
    mx_debug_value("0x%x", stmt->result.data[2].string_ptr);
    mx_debug_value("%s", *(stmt->result.data[2].string_ptr));


    mx_log_debug("group_id = %d", group_id);
    mx_log_debug("group_id2 = %d", group_id2);

//    assert(res == -ERANGE);
//    assert(group_id2 == 188); // truncated
    assert(res == 0);
    assert(group_id2 == group_id);




    res = mx_mysql_statement_fetch(stmt);
    assert(res == 0);

    res = mx_mysql_statement_fetch(stmt);
    assert(res == 0);




    res = mx_mysql_statement_close(&stmt);
    assert(res == 0);

    res = mx_mysql_disconnect(mysql);
    assert(res == 0);

    res = mx_mysql_free(&mysql);
    assert(res == 0);

    res = mx_mysql_end();
    assert(res == 0);

    mx_free_null(group_name);

    return 0;
}
