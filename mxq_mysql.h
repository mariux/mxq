
#ifndef __MXQ_MYSQL_H__
#define __MXQ_MYSQL_H__ 1

#include <sys/resource.h>

#include "mx_log.h"
#include "mxq_util.h"

#include <mysql.h>

struct mxq_mysql {
    char *default_file;
    char *default_group;
};

MYSQL *mxq_mysql_connect(struct mxq_mysql *mmysql);
void mxq_mysql_close(MYSQL *mysql);

int mxq_mysql_stmt_fetch_string(MYSQL_STMT *stmt, MYSQL_BIND *bind, int col, char **buf, unsigned long len);
int mxq_mysql_stmt_fetch_row(MYSQL_STMT *stmt);

MYSQL_STMT *mxq_mysql_stmt_do_query(MYSQL *mysql, char *stmt_str, int field_count, MYSQL_BIND *param, MYSQL_BIND *result);

int mxq_mysql_do_update(MYSQL *mysql, char* query, MYSQL_BIND *param);

#define MXQ_MYSQL_BIND_INT(b, c, v, t, s) \
    do { \
        (b)[(c)].buffer_type = (t); \
        (b)[(c)].buffer = (v); \
        (b)[(c)].is_unsigned = (s); \
    } while (0)

#define MXQ_MYSQL_BIND_INT64(b, c, v)  MXQ_MYSQL_BIND_INT((b), (c), (v), MYSQL_TYPE_LONGLONG, 0)
#define MXQ_MYSQL_BIND_UINT64(b, c, v) MXQ_MYSQL_BIND_INT((b), (c), (v), MYSQL_TYPE_LONGLONG, 1)

#define MXQ_MYSQL_BIND_INT32(b, c, v)  MXQ_MYSQL_BIND_INT((b), (c), (v), MYSQL_TYPE_LONG, 0)
#define MXQ_MYSQL_BIND_UINT32(b, c, v) MXQ_MYSQL_BIND_INT((b), (c), (v), MYSQL_TYPE_LONG, 1)

#define MXQ_MYSQL_BIND_INT16(b, c, v)  MXQ_MYSQL_BIND_INT((b), (c), (v), MYSQL_TYPE_SHORT, 0)
#define MXQ_MYSQL_BIND_UINT16(b, c, v) MXQ_MYSQL_BIND_INT((b), (c), (v), MYSQL_TYPE_SHORT, 1)

#define MXQ_MYSQL_BIND_INT8(b, c, v)   MXQ_MYSQL_BIND_INT((b), (c), (v), MYSQL_TYPE_TINY, 0)
#define MXQ_MYSQL_BIND_UINT8(b, c, v)  MXQ_MYSQL_BIND_INT((b), (c), (v), MYSQL_TYPE_TINY, 1)

#define MXQ_MYSQL_BIND_VARSTR(b, c, v) \
    do { \
        (b)[(c)].buffer_type = MYSQL_TYPE_STRING; \
        (b)[(c)].buffer = 0; \
        (b)[(c)].buffer_length = 0; \
        (b)[(c)].length = (v); \
    } while (0)

#define MXQ_MYSQL_BIND_STRING(b, c, v) \
    do { \
        (b)[(c)].buffer_type = MYSQL_TYPE_STRING; \
        (b)[(c)].buffer = (v); \
        (b)[(c)].buffer_length = strlen((v)); \
        (b)[(c)].length = &((b)[(c)].buffer_length); \
    } while (0)


#define mxq_mysql_print_error(mysql) \
    mx_log_err("MySQL: ERROR %u (%s): %s", \
    mysql_errno(mysql), \
    mysql_sqlstate(mysql), \
    mysql_error(mysql))

#define mxq_mysql_stmt_print_error(stmt) \
        mx_log_err("MySQL: ERROR %u (%s): %s", \
            mysql_stmt_errno(stmt), \
            mysql_stmt_sqlstate(stmt), \
            mysql_stmt_error(stmt))

static inline void close_mysqlp(void *p) {
    if (!p)
       return;
    mxq_mysql_close(*(void**) p);
}

#define _cleanup_close_mysql_ _cleanup_(close_mysqlp)

#endif
