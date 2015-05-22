
#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdarg.h>

#include <unistd.h>

#include <mysql.h>
#include <mysql/mysqld_error.h>

#include <errno.h>

#include <time.h>

#include "mx_log.h"
#include "mx_util.h"
#include "mxq_mysql.h"
#include "mxq_util.h"


MYSQL *mxq_mysql_connect(struct mxq_mysql *mmysql)
{
    MYSQL *mysql;
    MYSQL *mres;

    my_bool reconnect = 1;
    int try = 1;

    mysql = mysql_init(NULL);
    if (!mysql)
        return NULL;

    while (1) {
        if (mmysql->default_file && *mmysql->default_file)
            if (*mmysql->default_file != '/' || euidaccess(mmysql->default_file, R_OK) == 0)
                mysql_options(mysql, MYSQL_READ_DEFAULT_FILE,  mmysql->default_file);

        if (mmysql->default_group && *mmysql->default_group)
            mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, mmysql->default_group);
        else
            mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "mxq");

        mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

        mres = mysql_real_connect(mysql, NULL, NULL, NULL, NULL, 0, NULL, 0);
        if (mres == mysql)
            return mysql;

        mx_log_err("MAIN: Failed to connect to database (try=%d): Error: %s", try++, mysql_error(mysql));
        sleep(1);
    }
    return NULL;
}

void mxq_mysql_close(MYSQL *mysql) {
    mysql_close(mysql);
    mysql_library_end();
}

MYSQL_STMT *mxq_mysql_stmt_do_query(MYSQL *mysql, char *stmt_str, int field_count, MYSQL_BIND *param, MYSQL_BIND *result)
{
    MYSQL_STMT *stmt;
    int res;
    long tries = 0;
    struct timespec nsleep = {0};

    assert(mysql);
    assert(stmt_str);
    assert(field_count >= 0);

    stmt = mysql_stmt_init(mysql);
    if (!stmt) {
        mx_log_err("mysql_stmt_init(mysql=%p)", mysql);
        mxq_mysql_print_error(mysql);
        return NULL;
    }

    res = mysql_stmt_prepare(stmt, stmt_str, strlen(stmt_str));
    if (res) {
        mx_log_err("mysql_stmt_prepare(stmt=%p, stmt_str=\"%s\", length=%ld)", stmt, stmt_str, strlen(stmt_str));
        mxq_mysql_stmt_print_error(stmt);
        mysql_stmt_close(stmt);
        return NULL;
    }

    if (mysql_stmt_field_count(stmt) != field_count) {
        mx_log_err("mysql_stmt_field_count(stmt=%p) does not match requested field_count (=%d)", stmt, field_count);
        mysql_stmt_close(stmt);
        return NULL;
    }

    if (result) {
        res = mysql_stmt_bind_result(stmt, result);
        if (res) {
            mx_log_err("mysql_stmt_bind_result(stmt=%p)", stmt);
            mxq_mysql_stmt_print_error(stmt);
            mysql_stmt_close(stmt);
            return NULL;
        }
    }

    if (param) {
        res = mysql_stmt_bind_param(stmt, param);
        if (res) {
            mx_log_err("mysql_stmt_bind_param(stmt=%p)", stmt);
            mxq_mysql_stmt_print_error(stmt);
            mysql_stmt_close(stmt);
            return NULL;
        }
    }

    do {
        res = mysql_stmt_execute(stmt);
        if (!res)
            break;

        if (mysql_stmt_errno(stmt) == ER_LOCK_DEADLOCK) {
            mx_log_warning("MySQL recoverable error detected in mysql_stmt_execute(): ER_LOCK_DEADLOCK. (try %ld).", ++tries);
            nsleep.tv_nsec = tries*1000000L;
            nanosleep(&nsleep, NULL);
            continue;
        }

        if (mysql_stmt_errno(stmt) == ER_LOCK_WAIT_TIMEOUT) {
            mx_log_warning("MySQL recoverable error detected in mysql_stmt_execute(): ER_LOCK_WAIT_TIMEOUT. (try %ld).", ++tries);
            nsleep.tv_nsec = tries*1000000L;
            nanosleep(&nsleep, NULL);
            continue;
        }

        mxq_mysql_stmt_print_error(stmt);
        mysql_stmt_close(stmt);
        return NULL;
    } while(1);

    return stmt;
}

int mxq_mysql_stmt_fetch_string(MYSQL_STMT *stmt, MYSQL_BIND *bind, int col, char **buf, unsigned long len)
{
    char *s;
    int res;

    if (len > 0) {
        s = malloc(len+1);
        if (!s) {
            errno = ENOMEM;
            return 0;
        }
        bind[col].buffer = *buf = s;
        bind[col].buffer_length = len;

        res = mysql_stmt_fetch_column(stmt, &bind[col], col, 0);
        s[len] = 0;

        if (res) {
            errno = EIO;
            return 0;
        }
    }
    return 1;
}

int mxq_mysql_stmt_fetch_row(MYSQL_STMT *stmt)
{
    int res;

    res = mysql_stmt_fetch(stmt);

    if (res && res != MYSQL_DATA_TRUNCATED) {
        if (res == MYSQL_NO_DATA) {
            errno = ENOENT;
            return 0;
        }
        mxq_mysql_stmt_print_error(stmt);
        errno = EIO;
        return 0;
    }
    return 1;
}

int mxq_mysql_do_update(MYSQL *mysql, char* query, MYSQL_BIND *param)
{
    MYSQL_STMT *stmt;
    int   res;

    stmt = mxq_mysql_stmt_do_query(mysql, query, 0, param, NULL);
    if (!stmt) {
        mx_log_err("mxq_mysql_do_update: Failed to query database.");
        errno = EIO;
        return -1;
    }

    res = mysql_stmt_affected_rows(stmt);
    mysql_stmt_close(stmt);

    if (res == 0) {
        errno = ENOENT;
        return -1;
    }

    return res;
}
