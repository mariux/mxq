
#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdarg.h>

#include <unistd.h>

#include <mysql.h>

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

char *mxq_mysql_escape_string(MYSQL *mysql, char *s)
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
