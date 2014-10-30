
#ifndef __MXQ_MYSQL_H__
#define __MXQ_MYSQL_H__ 1

#include <sys/resource.h>

#include "mxq_util.h"

#include <mysql.h>

struct mxq_mysql {
    char *default_file;
    char *default_group;
};

MYSQL *mxq_mysql_connect(struct mxq_mysql *mmysql);
void mxq_mysql_close(MYSQL *mysql);

int mxq_mysql_query(MYSQL *mysql, const char *fmt, ...) __attribute__ ((format(printf, 2, 3)));

MYSQL_RES *mxq_mysql_query_with_result(MYSQL *mysql, const char *fmt, ...) __attribute__ ((format(printf, 2, 3)));

char *mxq_mysql_escape_string(MYSQL *mysql, char *s);
char *mxq_mysql_escape_str(MYSQL *mysql, char *s);
char *mxq_mysql_escape_strvec(MYSQL *mysql, char **s);


static inline void close_mysqlp(void *p) {
    if (!p)
       return;
    mxq_mysql_close(*(void**) p);
}

#define _cleanup_close_mysql_ _cleanup_(close_mysqlp)

#endif
