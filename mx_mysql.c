#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdarg.h>

#include <unistd.h>

#include <mysql.h>
#include <mysqld_error.h>
#include <errmsg.h>

#include <errno.h>

#include <time.h>

#include "mx_mysql.h"
#include "mx_util.h"
#include "mx_log.h"

/**********************************************************************/

static inline int mx__mysql_errno(struct mx_mysql *mysql)
{
    unsigned int error;

    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(mysql->mysql, EBADF);

    error = mysql_errno(mysql->mysql);
    mx_assert_return_minus_errno((unsigned int)(int)error == error, ERANGE);

    return (int)error;
}

inline const char *mx__mysql_error(struct mx_mysql *mysql)
{
    mx_assert_return_NULL(mysql, EINVAL);
    mx_assert_return_NULL(mysql->mysql, EBADF);

    return mysql_error(mysql->mysql);
}

static inline int mx__mysql_stmt_errno(struct mx_mysql_stmt *stmt)
{
    unsigned int error;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    error = mysql_stmt_errno(stmt->stmt);
    mx_assert_return_minus_errno((unsigned int)(int)error == error, ERANGE);

    return (int)error;
}

static inline const char *mx__mysql_stmt_error(struct mx_mysql_stmt *stmt)
{
    mx_assert_return_NULL(stmt, EINVAL);
    mx_assert_return_NULL(stmt->stmt, EBADF);

    return mysql_stmt_error(stmt->stmt);
}

static inline int mx__mysql_init(struct mx_mysql *mysql)
{
    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(!mysql->mysql, EUCLEAN);

    mysql->mysql = mysql_init(NULL);

    mx_assert_return_minus_errno(mysql->mysql, ENOMEM);

    return 0;
}

static inline int mx__mysql_options(struct mx_mysql *mysql, enum mysql_option option, const void *arg)
{
    int res;

    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(mysql->mysql, EBADF);

    res = mysql_options(mysql->mysql, option, arg);
    mx_assert_return_minus_errno(res == 0, EINVAL);

    return 0;
}

static int mx__mysql_real_connect(struct mx_mysql *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long client_flag)
{
    MYSQL *m;

    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(mysql->mysql, EBADF);

    m = mysql_real_connect(mysql->mysql, host, user, passwd, db, port, unix_socket, client_flag);
    if (m == mysql->mysql)
        return 0;

    if (mx__mysql_errno(mysql) == CR_ALREADY_CONNECTED) {
        return -(errno=EALREADY);
    }

    return -(errno=EAGAIN);
}

static inline int mx__mysql_ping(struct mx_mysql *mysql)
{
    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(mysql->mysql, EBADF);

    return mysql_ping(mysql->mysql);
}

static int mx__mysql_real_query(struct mx_mysql *mysql, const char *stmt_str, unsigned long length)
{
    mx_assert_return_minus_errno(mysql,     EINVAL);
    mx_assert_return_minus_errno(stmt_str,  EINVAL);
    mx_assert_return_minus_errno(*stmt_str, EINVAL);

    mx_assert_return_minus_errno(mysql->mysql, EBADF);

    int res;

    if (!length)
        length = strlen(stmt_str);

    res = mysql_real_query(mysql->mysql, stmt_str, length);

    return res;
}

static int mx__mysql_stmt_init(struct mx_mysql_stmt *stmt)
{
    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(!stmt->stmt, EUCLEAN);
    mx_assert_return_minus_errno(stmt->mysql, EBADF);

    stmt->stmt = mysql_stmt_init(stmt->mysql->mysql);
    if (stmt->stmt)
        return 0;

    if (mx__mysql_errno(stmt->mysql) == CR_OUT_OF_MEMORY)
        return -(errno=ENOMEM);

    mx_log_debug("ERROR: mysql_stmt_init() returned undefined error: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_stmt_prepare(struct mx_mysql_stmt *stmt, char *statement)
{
    int res;

    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(statement,  EINVAL);
    mx_assert_return_minus_errno(*statement, EINVAL);

    mx_assert_return_minus_errno(!stmt->statement, EUCLEAN);

    mx_assert_return_minus_errno(stmt->mysql, EBADF);
    mx_assert_return_minus_errno(stmt->stmt,  EBADF);

    res = mysql_stmt_prepare(stmt->stmt, statement, strlen(statement));
    if (res == 0) {
        stmt->statement = statement;
        return 0;
    }
    switch (mx__mysql_stmt_errno(stmt)) {
        case CR_OUT_OF_MEMORY:
            return -(errno=ENOMEM);

        case CR_SERVER_GONE_ERROR:
        case CR_SERVER_LOST:
            return -(errno=EAGAIN);
    }

    mx_log_debug("ERROR: mysql_stmt_prepare() returned undefined error: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_stmt_bind_param(struct mx_mysql_stmt *stmt)
{
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = (int)mysql_stmt_bind_param(stmt->stmt, stmt->param.bind);
    if (res == 0)
        return 0;

    switch (mx__mysql_stmt_errno(stmt)) {
        case CR_OUT_OF_MEMORY:
            return -(errno=ENOMEM);

        case CR_UNSUPPORTED_PARAM_TYPE:
            return -(errno=EINVAL);

        case CR_UNKNOWN_ERROR:
            return -(errno=EIO);
    }

    mx_log_debug("ERROR: mysql_stmt_bind_param() returned undefined error: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_stmt_bind_result(struct mx_mysql_stmt *stmt)
{
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = (int)mysql_stmt_bind_result(stmt->stmt, stmt->result.bind);
    if (res == 0)
        return 0;

    switch (mx__mysql_stmt_errno(stmt)) {
        case CR_OUT_OF_MEMORY:
            return -(errno=ENOMEM);

        case CR_UNSUPPORTED_PARAM_TYPE:
            return -(errno=EINVAL);

        case CR_UNKNOWN_ERROR:
            return -(errno=EIO);
    }

    mx_log_debug("ERROR: mysql_stmt_bind_result() returned undefined error: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_stmt_execute(struct mx_mysql_stmt *stmt)
{
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = mysql_stmt_execute(stmt->stmt);
    if (res == 0)
        return 0;

    switch (mx__mysql_stmt_errno(stmt)) {
        case CR_COMMANDS_OUT_OF_SYNC:
            return -(errno=EPROTO);

        case CR_OUT_OF_MEMORY:
            return -(errno=ENOMEM);

        case CR_SERVER_GONE_ERROR:
        case CR_SERVER_LOST:
            return -(errno=EAGAIN);

        case CR_UNKNOWN_ERROR:
            return -(errno=EIO);
    }

    mx_log_debug("ERROR: mysql_stmt_execute() returned undefined error: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_stmt_store_result(struct mx_mysql_stmt *stmt)
{
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = mysql_stmt_store_result(stmt->stmt);
    if (res == 0)
        return 0;

    switch (mx__mysql_stmt_errno(stmt)) {
        case CR_COMMANDS_OUT_OF_SYNC:
            return -(errno=EPROTO);

        case CR_OUT_OF_MEMORY:
            return -(errno=ENOMEM);

        case CR_SERVER_GONE_ERROR:
        case CR_SERVER_LOST:
            return -(errno=EAGAIN);

        case CR_UNKNOWN_ERROR:
            return -(errno=EIO);
    }

    mx_log_debug("ERROR: mysql_stmt_store_result() returned undefined error: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_stmt_free_result(struct mx_mysql_stmt *stmt)
{
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = (int)mysql_stmt_free_result(stmt->stmt);
    if (res == 0)
        return 0;

    mx_log_debug("ERROR: mysql_stmt_free_result() failed: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_stmt_fetch(struct mx_mysql_stmt *stmt)
{
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = mysql_stmt_fetch(stmt->stmt);
    if (res == 0)
        return 0;

    if (res == 1) {
        switch (mx__mysql_stmt_errno(stmt)) {
            case CR_COMMANDS_OUT_OF_SYNC:
                return -(errno=EPROTO);

            case CR_OUT_OF_MEMORY:
                return -(errno=ENOMEM);

            case CR_SERVER_GONE_ERROR:
            case CR_SERVER_LOST:
                return -(errno=EAGAIN);

            case CR_UNKNOWN_ERROR:
                return -(errno=EIO);
        }
        mx_log_debug("ERROR: mysql_stmt_fetch() returned undefined error: %s", mx__mysql_stmt_error(stmt));
        return -(errno=EBADE);
    }

    switch (res) {
        case MYSQL_NO_DATA:
            return -(errno=ENOENT);

        case MYSQL_DATA_TRUNCATED:
            return -(errno=ERANGE);
    }

    mx_log_debug("ERROR: mysql_stmt_fetch() returned undefined result code: %d", res);
    return -(errno=EBADE);
}

static int mx__mysql_stmt_fetch_column(struct mx_mysql_stmt *stmt, unsigned int column, unsigned long offset)
{
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = mysql_stmt_fetch_column(stmt->stmt, &(stmt->result.bind[column]), column, offset);
    if (res == 0)
        return 0;

    switch (mx__mysql_stmt_errno(stmt)) {
        case CR_INVALID_PARAMETER_NO:
            return -(errno=EPROTO);

        case CR_NO_DATA:
            return -(errno=ENOENT);
    }
    mx_log_debug("ERROR: mysql_stmt_fetch_column() returned undefined error: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_stmt_param_count(struct mx_mysql_stmt *stmt)
{
    unsigned long count;

    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    count = mysql_stmt_param_count(stmt->stmt);
    mx_assert_return_minus_errno((unsigned long)(int)count == count, ERANGE);

    return (int)count;
}

static int mx__mysql_stmt_field_count(struct mx_mysql_stmt *stmt)
{
    unsigned long count;

    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    count = mysql_stmt_field_count(stmt->stmt);
    mx_assert_return_minus_errno((unsigned long)(int)count == count, ERANGE);

    return (int)count;
}

static int mx__mysql_stmt_num_rows(struct mx_mysql_stmt *stmt, unsigned long long *count)
{
    my_ulonglong c;

    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    c = mysql_stmt_num_rows(stmt->stmt);

    *count = (unsigned long long)c;

    return 0;
}

static int mx__mysql_stmt_affected_rows(struct mx_mysql_stmt *stmt, unsigned long long *count)
{
    my_ulonglong c;

    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    c = mysql_stmt_affected_rows(stmt->stmt);

    *count = (unsigned long long)c;

    return 0;
}

static int mx__mysql_stmt_close(struct mx_mysql_stmt *stmt)
{
    my_bool res;

    mx_assert_return_minus_errno(stmt, EINVAL);

    res = mysql_stmt_close(stmt->stmt);
    if (res == 0) {
        stmt->stmt = NULL;
        return 0;
    }

    switch (mx__mysql_stmt_errno(stmt)) {
        case CR_SERVER_GONE_ERROR:
            return -(errno=ECONNABORTED);

        case CR_UNKNOWN_ERROR :
            return -(errno=EIO);
    }

    mx_log_debug("ERROR: mysql_stmt_close() returned undefined error: %s", mx__mysql_stmt_error(stmt));
    return -(errno=EBADE);
}

static int mx__mysql_close(struct mx_mysql *mysql) {
    mx_assert_return_minus_errno(mysql, EINVAL);

    if (mysql->mysql) {
        mysql_close(mysql->mysql);
        mysql->mysql = NULL;
    }

    return 0;
}

static int mx__mysql_library_end(void) {
    mysql_library_end();
    return 0;
}

/**********************************************************************/

static inline int _mx_mysql_bind_integer(struct mx_mysql_bind *b, unsigned int index, void *value, int type, int is_unsigned)
{
    mx_assert_return_minus_errno(b,     EINVAL);
    mx_assert_return_minus_errno(value, EINVAL);

    mx_assert_return_minus_errno(index < b->count, ERANGE);

    mx_assert_return_minus_errno(!(b->data[index].flags), EUCLEAN);

    memset(&(b->bind[index]), 0, sizeof(b->bind[index]));

    b->bind[index].buffer_type = (enum enum_field_types)type;
    b->bind[index].buffer      = value;
    b->bind[index].is_unsigned = (my_bool)is_unsigned;
    b->bind[index].length      = &(b->data[index].length);
    b->bind[index].is_null     = &(b->data[index].is_null);
    b->bind[index].error       = &(b->data[index].is_error);

    b->data[index].flags = 1;

    return 0;
}

static inline int _mx_mysql_bind_string(struct mx_mysql_bind *b, unsigned int index, char **value)
{
    mx_assert_return_minus_errno(b,     EINVAL);
    mx_assert_return_minus_errno(value, EINVAL);

    mx_assert_return_minus_errno(index < b->count, ERANGE);

    mx_assert_return_minus_errno(!(b->data[index].flags), EUCLEAN);

    mx_assert_return_minus_errno((*value && b->type == MX_MYSQL_BIND_TYPE_PARAM) || (!*value && b->type == MX_MYSQL_BIND_TYPE_RESULT), EBADF);

    memset(&(b->bind[index]), 0, sizeof(b->bind[index]));

    if (b->type == MX_MYSQL_BIND_TYPE_PARAM) {
        b->data[index].string_ptr = value;
        b->data[index].length     = strlen(*value);

        b->bind[index].buffer_type   = MYSQL_TYPE_STRING;
        b->bind[index].buffer        = *(b->data[index].string_ptr);
        b->bind[index].buffer_length = b->data[index].length;
        b->bind[index].length        = &(b->data[index].length);
        b->bind[index].is_null       = &(b->data[index].is_null);
        b->bind[index].error         = &(b->data[index].is_error);
    } else {
        b->data[index].string_ptr = value;
        b->data[index].length     = 0;

        b->bind[index].buffer_type   = MYSQL_TYPE_STRING;
        b->bind[index].buffer        = NULL;
        b->bind[index].buffer_length = 0;
        b->bind[index].length        = &(b->data[index].length);
        b->bind[index].is_null       = &(b->data[index].is_null);
        b->bind[index].error         = &(b->data[index].is_error);
    }

    b->data[index].flags = 1;
    return 0;
}

static inline int _mx_mysql_bind_validate(struct mx_mysql_bind *b)
{
    int i;

    mx_assert_return_minus_errno(b, EINVAL);

    for (i=0; i < b->count; i++) {
        if (!(b->data[i].flags)) {
            return -(errno=ENOENT);
        }
    }

    return 0;
}

/**********************************************************************/

int mx_mysql_init(struct mx_mysql **mysql)
{
    struct mx_mysql *m;
    int res;

    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(!(*mysql), EUCLEAN);

    m = mx_calloc_forever(1, sizeof(*m));

    do {
        res = mx__mysql_init(m);
        if (res == 0)
            break;

        if (res != -ENOMEM)
            return res;

        mx_log_debug("mx__mysql_init() failed: %m - retrying (forever) in %d second(s).", MX_CALLOC_FAIL_WAIT_DEFAULT);
        mx_sleep(MX_CALLOC_FAIL_WAIT_DEFAULT);

    } while (1);

    *mysql = m;

    return 0;
}

int mx_mysql_option_set_default_file(struct mx_mysql *mysql, char *fname)
{
    mx_assert_return_minus_errno(mysql, EINVAL);

    if (fname && (*fname == '/') && (euidaccess(fname, R_OK) != 0)) {
        mx_log_debug("ignoring MySQL defaults file: euidaccess(\"%s\", R_OK) failed: %m", fname);
        return -errno;
    }

    if (fname && !(*fname))
        fname = NULL;

    mysql->default_file = fname;

    return 0;
}

char *mx_mysql_option_get_default_file(struct mx_mysql *mysql)
{
    mx_assert_return_NULL(mysql, EINVAL);
    return mysql->default_file;
}

int mx_mysql_option_set_default_group(struct mx_mysql *mysql, char *group)
{
    mx_assert_return_minus_errno(mysql, EINVAL);

    if (group && !(*group))
        group = NULL;

    mysql->default_group = group;

    return 0;
}

char *mx_mysql_option_get_default_group(struct mx_mysql *mysql)
{
    mx_assert_return_NULL(mysql, EINVAL);

    return mysql->default_group;
}

int mx_mysql_option_set_reconnect(struct mx_mysql *mysql, int reconnect)
{
    mx_assert_return_minus_errno(mysql, EINVAL);

    mysql->reconnect = (my_bool)!!reconnect;
    return 0;
}

int mx_mysql_option_get_reconnect(struct mx_mysql *mysql)
{
    mx_assert_return_minus_errno(mysql, EINVAL);

    return (int)mysql->reconnect;
}

static int mx_mysql_real_connect(struct mx_mysql *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long client_flag)
{
    int res;

    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(mysql->mysql, EBADF);

    if (mysql->default_file) {
        res = mx__mysql_options(mysql, MYSQL_READ_DEFAULT_FILE, mysql->default_file);
        mx_mysql_assert_usage_ok(res);
    }

    if (mysql->default_group) {
        res = mx__mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, mysql->default_group);
        mx_mysql_assert_usage_ok(res);
    } else {
        res = mx__mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, program_invocation_short_name);
        mx_mysql_assert_usage_ok(res);
    }
    res = mx__mysql_options(mysql, MYSQL_OPT_RECONNECT, &mysql->reconnect);
    mx_mysql_assert_usage_ok(res);

    res = mx__mysql_real_connect(mysql, host, user, passwd, db, port, unix_socket, client_flag);
    mx_mysql_assert_usage_ok(res);

    if (res == 0)
        return 0;

    if (res == -EALREADY) {
        mx_log_debug("WARNING: %s", mx__mysql_error(mysql));
        return 0;
    }

    mx_log_debug("ERROR: mysql_real_connect(): %s", mx__mysql_error(mysql));
    return res;
}

int mx_mysql_connect(struct mx_mysql **mysql)
{
    int res;

    mx_assert_return_minus_errno(mysql, EINVAL);

    if (!(*mysql)) {
        res = mx_mysql_init(mysql);
        if (res < 0)
            return res;
    }

    res = mx_mysql_real_connect(*mysql, NULL, NULL, NULL, NULL, 0, NULL, 0);
    return res;
}

int mx_mysql_connect_forever(struct mx_mysql **mysql, unsigned int seconds)
{
    int res;

    while ((res = mx_mysql_connect(mysql)) < 0) {
        mx_mysql_assert_usage_ok(res);
        mx_sleep(seconds);
    }

    return 0;
}

int mx_mysql_disconnect(struct mx_mysql *mysql) {
    mx_assert_return_minus_errno(mysql, EINVAL);

    return mx__mysql_close(mysql);
}

int mx_mysql_end(void) {
    return mx__mysql_library_end();
}

int mx_mysql_free(struct mx_mysql **mysql)
{
    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(*mysql, EBADF);
    mx_assert_return_minus_errno(!((*mysql)->mysql), EUCLEAN);

    mx_free_null(*mysql);

    return 0;
}

int mx_mysql_finish(struct mx_mysql **mysql)
{
    int res = 0;
    int res1 = 0, res2 = 0, res3 = 0;

    if (mysql && *mysql) {
        res1 = mx_mysql_disconnect(*mysql);
        if (res1 < 0)
            res = res1;

        res2 = mx_mysql_free(mysql);
        if (!res && res2 < 0)
            res = res2;
    }
    res3 = mx_mysql_end();
    if (!res && res3 < 0)
        res = res3;

    return res;
}


int mx_mysql_ping(struct mx_mysql *mysql)
{
    mx_assert_return_minus_errno(mysql, EINVAL);

    return mx__mysql_ping(mysql);
}

int mx_mysql_queryf(struct mx_mysql *mysql, const char *fmt, ...)
{
    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(fmt,   EINVAL);
    mx_assert_return_minus_errno(*fmt,  EINVAL);

    mx_assert_return_minus_errno(mysql->mysql, EBADF);

    va_list ap;
    _mx_cleanup_free_ char *query = NULL;
    int res;
    size_t len;

    va_start(ap, fmt);
    len = vasprintf(&query, fmt, ap);
    va_end(ap);

    if (len == -1)
        return 0;

    res = mx__mysql_real_query(mysql, query, len);

    return res;
}

int mx_mysql_statement_init(struct mx_mysql *mysql, struct mx_mysql_stmt **stmt)
{
    struct mx_mysql_stmt *s;
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(mysql, EINVAL);
    mx_assert_return_minus_errno(!(*stmt), EUCLEAN);

    s = mx_calloc_forever(1, sizeof(*s));

    s->mysql       = mysql;
    s->param.type  = MX_MYSQL_BIND_TYPE_PARAM;
    s->result.type = MX_MYSQL_BIND_TYPE_RESULT;

    do {
        res = mx__mysql_stmt_init(s);
        if (res == 0)
            break;

        if (res != -ENOMEM)
            return res;

        mx_log_debug("mx__mysql_stmt_init() failed: %m - retrying (forever) in %d second(s).", MX_CALLOC_FAIL_WAIT_DEFAULT);
        mx_sleep(MX_CALLOC_FAIL_WAIT_DEFAULT);
    } while (1);

    *stmt = s;

    return 0;
}

int mx_mysql_statement_execute(struct mx_mysql_stmt *stmt, unsigned long long *count)
{
    struct mx_mysql_stmt *s;
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = _mx_mysql_bind_validate(&stmt->param);
    if (res < 0) {
        mx_log_debug("ERROR: param not initialized completely.");
        return res;
    }

    res = mx__mysql_stmt_bind_param(stmt);
    if (res < 0) {
        mx_log_debug("ERROR: mx__mysql_stmt_bind_param: %m");
        return res;
    }

    res = mx__mysql_stmt_execute(stmt);
    if (res < 0) {
        mx_log_debug("ERROR: mx__mysql_stmt_execute: %m");
        return res;
    }

    res = mx__mysql_stmt_store_result(stmt);
    if (res < 0) {
        mx_log_debug("ERROR: mx__mysql_stmt_store_result: %m");
        return res;
    }

    if (count) {
        res = mx__mysql_stmt_affected_rows(stmt, count);
        if (res < 0) {
            mx_log_debug("ERROR: mx__mysql_stmt_affected_rows(): %m");
            return res;
        }
    }

    return 0;
}

int mx_mysql_statement_affected_rows(struct mx_mysql_stmt *stmt, unsigned long long int *count) {
    return mx__mysql_stmt_affected_rows(stmt, count);
}

int mx_mysql_statement_num_rows(struct mx_mysql_stmt *stmt, unsigned long long int *count) {
    return mx__mysql_stmt_num_rows(stmt, count);
}

int mx_mysql_statement_fetch(struct mx_mysql_stmt *stmt)
{
    struct mx_mysql_stmt *s;
    struct mx_mysql_bind *r;
    int res;
    int col;
    char *str;
    int no_error = 1;

    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    res = _mx_mysql_bind_validate(&stmt->result);
    if (res < 0) {
        mx_log_debug("ERROR: result not initialized completely.");
        return res;
    }

    res = mx__mysql_stmt_bind_result(stmt);
    if (res < 0) {
        mx_log_debug("ERROR: mx__mysql_stmt_bind_result: %m");
        return res;
    }

    res = mx__mysql_stmt_fetch(stmt);
    if (res == -ENOENT || res == 0)
        return 0;

    if (res < 0 && res != -ERANGE) {
        mx_log_debug("ERROR: mx__mysql_stmt_fetch: %m");
        return res;
    }

    r = &stmt->result;
    for (col = 0; col < r->count; col++) {
        if (!(r->data[col].is_error))
            continue;

        if (r->bind[col].buffer_type == MYSQL_TYPE_STRING) {
            str = mx_calloc_forever(r->data[col].length + 1, sizeof(*str));

            *(r->data[col].string_ptr) = str;
            r->bind[col].buffer        = *(r->data[col].string_ptr);
            r->bind[col].buffer_length =   r->data[col].length;

            res = mx__mysql_stmt_fetch_column(stmt, col, 0);
            continue;
        }

        mx_log_debug("WARNING: result data returned in column with index %d was truncated. query was:", col);
        mx_log_debug("       \\ %s", stmt->statement);
        no_error = 0;
    }

    if (!no_error)
        return -(errno=ERANGE);

    return 0;
}

int mx_mysql_statement_param_count(struct mx_mysql_stmt *stmt)
{
    mx_assert_return_minus_errno(stmt, EINVAL);

    return mx__mysql_stmt_param_count(stmt);
}

int mx_mysql_statement_field_count(struct mx_mysql_stmt *stmt)
{
    mx_assert_return_minus_errno(stmt, EINVAL);
    return mx__mysql_stmt_field_count(stmt);
}

inline int mx_mysql_stmt_field_count_set(struct mx_mysql_stmt *stmt)
{
    unsigned long count;

    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    stmt->field_count = mysql_stmt_field_count(stmt->stmt);

    return 0;
}

inline int mx_mysql_stmt_field_count_get(struct mx_mysql_stmt *stmt, unsigned long *count)
{
    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(count,      EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    *count = stmt->field_count;

    return 0;
}

inline int mx_mysql_stmt_param_count_set(struct mx_mysql_stmt *stmt)
{
    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    stmt->param_count = mysql_stmt_param_count(stmt->stmt);

    return 0;
}

inline int mx_mysql_stmt_param_count_get(struct mx_mysql_stmt *stmt, unsigned long *count)
{
    mx_assert_return_minus_errno(stmt,       EINVAL);
    mx_assert_return_minus_errno(count,      EINVAL);
    mx_assert_return_minus_errno(stmt->stmt, EBADF);

    *count = stmt->param_count;

    return 0;
}

int mx_mysql_bind_cleanup(struct mx_mysql_bind *bind)
{
    mx_assert_return_minus_errno(bind, EINVAL);

    mx_assert_return_minus_errno(bind->type != MX_MYSQL_BIND_TYPE_UNKNOWN, EBADF);

    mx_free_null(bind->bind);
    mx_free_null(bind->data);

    bind->count = 0;

    return 0;
}

int mx_mysql_bind_init(struct mx_mysql_bind *bind, unsigned long count)
{
    unsigned long *l;
    char *f;

    mx_assert_return_minus_errno(bind, EINVAL);

    mx_assert_return_minus_errno(bind->type != MX_MYSQL_BIND_TYPE_UNKNOWN, EBADF);

    mx_assert_return_minus_errno(!bind->count,  EUCLEAN);
    mx_assert_return_minus_errno(!bind->bind,   EUCLEAN);
    mx_assert_return_minus_errno(!bind->data,   EUCLEAN);

    if (!count)
        return 0;

    bind->count = count;
    bind->bind  = mx_calloc_forever(bind->count, sizeof(*bind->bind));
    bind->data  = mx_calloc_forever(bind->count, sizeof(*bind->data));

    return 0;
}

int mx_mysql_statement_prepare(struct mx_mysql_stmt *stmt, char *statement)
{
    int res;

    mx_assert_return_minus_errno(stmt, EINVAL);

    res = mx__mysql_stmt_prepare(stmt, statement);
    mx_mysql_assert_usage_ok(res);
    if (res < 0)
        return res;

    res = mx_mysql_stmt_param_count_set(stmt);
    mx_mysql_assert_usage_ok(res);
    if (res < 0)
        return res;

    res = mx_mysql_stmt_field_count_set(stmt);
    mx_mysql_assert_usage_ok(res);
    if (res < 0)
        return res;

    res = mx_mysql_bind_init(&stmt->param, stmt->param_count);
    mx_mysql_assert_usage_ok(res);
    if (res < 0)
        return res;

    res = mx_mysql_bind_init(&stmt->result, stmt->field_count);
    mx_mysql_assert_usage_ok(res);
    if (res < 0)
        return res;

    return 0;
}

int mx_mysql_statement_close(struct mx_mysql_stmt **stmt)
{
    mx_assert_return_minus_errno(stmt, EINVAL);
    mx_assert_return_minus_errno(*stmt, EINVAL);

    mx__mysql_stmt_free_result(*stmt);
    mx__mysql_stmt_close(*stmt);

    mx_mysql_bind_cleanup(&(*stmt)->param);
    mx_mysql_bind_cleanup(&(*stmt)->result);
    mx_free_null(*stmt);

    return 0;
}

inline int mx_mysql_bind_integer(struct mx_mysql_bind *b, unsigned int index, void *value, int type, int is_unsigned)
{
    int res;

    res = _mx_mysql_bind_integer(b, index, value, type, is_unsigned);
    if (res == 0)
        return 0;

    mx_log_debug("Failed to set index %d: %m", index);
    return res;
}

inline int mx_mysql_bind_string(struct mx_mysql_bind *b, unsigned int index, char **value)
{
    int res;

    res = _mx_mysql_bind_string(b, index, value);
    if (res == 0)
        return 0;

    mx_log_debug("Failed to set index %d: %m", index);
    return res;

}

inline int mx_mysql_bind_int8(struct mx_mysql_bind *b, unsigned int index, int8_t *value)
{
    return mx_mysql_bind_integer(b, index, (void *)value, MYSQL_TYPE_TINY, 0);
}

inline int mx_mysql_bind_uint8(struct mx_mysql_bind *b, unsigned int index, uint8_t *value)
{
    return mx_mysql_bind_integer(b, index, (void *)value, MYSQL_TYPE_TINY, 1);
}

inline int mx_mysql_bind_int16(struct mx_mysql_bind *b, unsigned int index, int16_t *value)
{
    return mx_mysql_bind_integer(b, index, (void *)value, MYSQL_TYPE_SHORT, 0);
}

inline int mx_mysql_bind_uint16(struct mx_mysql_bind *b, unsigned int index, uint16_t *value)
{
    return mx_mysql_bind_integer(b, index, (void *)value, MYSQL_TYPE_SHORT, 1);
}

inline int mx_mysql_bind_int32(struct mx_mysql_bind *b, unsigned int index, int32_t *value)
{
    return mx_mysql_bind_integer(b, index, (void *)value, MYSQL_TYPE_LONG, 0);
}

inline int mx_mysql_bind_uint32(struct mx_mysql_bind *b, unsigned int index, uint32_t *value)
{
    return mx_mysql_bind_integer(b, index, (void *)value, MYSQL_TYPE_LONG, 1);
}

inline int mx_mysql_bind_int64(struct mx_mysql_bind *b, unsigned int index, int64_t *value)
{
    return mx_mysql_bind_integer(b, index, (void *)value, MYSQL_TYPE_LONGLONG, 0);
}

inline int mx_mysql_bind_uint64(struct mx_mysql_bind *b, unsigned int index, uint64_t *value)
{
    return mx_mysql_bind_integer(b, index, (void *)value, MYSQL_TYPE_LONGLONG, 1);
}
