
#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdarg.h>

#include <unistd.h>

#include <mysql.h>

#include <errno.h>

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

    if (mmysql->default_file)
        mysql_options(mysql, MYSQL_READ_DEFAULT_FILE,  mmysql->default_file);

    mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "mxq_submit");
    mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

    while (1) {
        mres = mysql_real_connect(mysql, NULL, NULL, NULL, NULL, 0, NULL, CLIENT_REMEMBER_OPTIONS);
        if (mres == mysql)
            return mysql;

        log_msg(0, "MAIN: Failed to connect to database (try=%d): Error: %s\n", try++, mysql_error(mysql));
        sleep(1);
    }
    return NULL;
}

void mxq_mysql_close(MYSQL *mysql) {
    mysql_close(mysql);
    mysql_library_end();
}

int mxq_mysql_query(MYSQL *mysql, const char *fmt, ...)
{
    va_list ap;
    _cleanup_free_ char *query = NULL;
    int res;
    size_t len;

    va_start(ap, fmt);
    len = vasprintf(&query, fmt, ap);
    va_end(ap);

    if (len == -1)
        return 0;

    assert(len == strlen(query));

    //printf("QUERY(%d): %s;\n", (int)len, query);

    res = mysql_real_query(mysql, query, len);

    return res;
}

MYSQL_RES *mxq_mysql_query_with_result(MYSQL *mysql, const char *fmt, ...)
{
    va_list ap;
    _cleanup_free_ char *query = NULL;
    MYSQL_RES *mres;
    size_t len;
    int res;

    va_start(ap, fmt);
    len = vasprintf(&query, fmt, ap);
    va_end(ap);

    if (len == -1)
        return 0;

    assert(len == strlen(query));

    //printf("QUERY(%d): %s;\n", (int)len, query);

    res = mysql_real_query(mysql, query, len);
    if (res) {
        MXQ_LOG_ERROR("mysql_real_query() failed. Error: %s\n", mysql_error(mysql));
        MXQ_LOG_INFO("query was: %s\n", query);
        return NULL;
    }

    mres = mysql_store_result(mysql);
    if (!mres) {
        MXQ_LOG_ERROR("mysql_store_result() failed. Error: %s\n", mysql_error(mysql));
        MXQ_LOG_INFO("query was: %s\n", query);
        return NULL;
    }

    return mres;
}

MYSQL_STMT *mxq_mysql_stmt_do_query(MYSQL *mysql, char *stmt_str, int field_count, MYSQL_BIND *param, MYSQL_BIND *result)
{
    MYSQL_STMT *stmt;
    int res;

    assert(mysql);
    assert(stmt_str);
    assert(field_count > 0);
    assert(!param); // not implemented yet

    stmt = mysql_stmt_init(mysql);
    if (!stmt) {
        print_error("mysql_stmt_init(mysql=%p)\n", mysql);
        mxq_mysql_print_error(mysql);
        return NULL;
    }

    res = mysql_stmt_prepare(stmt, stmt_str, strlen(stmt_str));
    if (res) {
        print_error("mysql_stmt_prepare(stmt=%p, stmt_str=\"%s\", length=%ld)\n", stmt, stmt_str, strlen(stmt_str));
        mxq_mysql_stmt_print_error(stmt);
        mysql_stmt_close(stmt);
        return NULL;
    }

    if (mysql_stmt_field_count(stmt) != field_count) {
        print_error("mysql_stmt_field_count(stmt=%p) does not match requested field_count (=%d)\n", stmt, field_count);
        mysql_stmt_close(stmt);
        return NULL;
    }

    if (result) {
        res = mysql_stmt_bind_result(stmt, result);
        if (res) {
            print_error("mysql_stmt_bind_result(stmt=%p)\n", stmt);
            mxq_mysql_stmt_print_error(stmt);
            mysql_stmt_close(stmt);
            return NULL;
        }
    }

    res = mysql_stmt_execute(stmt);
    if (res) {
        print_error("mysql_stmt_execute(stmt=%p)\n", stmt);
        mxq_mysql_stmt_print_error(stmt);
        mysql_stmt_close(stmt);
        return NULL;
    }

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
        errno = EIO;
        return 0;
    }
    return 1;
}


char *mxq_mysql_escape_str(MYSQL *mysql, char *s)
{
    char *quoted = NULL;
    size_t len;

    len    = strlen(s);
    quoted = malloc(len*2 + 1);
    if (!quoted)
       return NULL;

    mysql_real_escape_string(mysql, quoted, s,  len);

    return quoted;
}

char *mxq_mysql_escape_strvec(MYSQL *mysql, char **sv)
{
    char *quoted = NULL;
    _cleanup_free_ char *s = NULL;
    size_t len;

    s = strvec_to_str(sv);
    if (!s)
        return NULL;

    len    = strlen(s);
    quoted = malloc(len*2 + 1);
    if (!quoted)
       return NULL;

    mysql_real_escape_string(mysql, quoted, s,  len);

    return quoted;
}

char *mxq_mysql_escape_string(MYSQL *mysql, char *s)
{
    return mxq_mysql_escape_str(mysql, s);
}
