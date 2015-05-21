#ifndef __MX_MYSQL_H__
#define __MX_MYSQL_H__ 1

#include <sys/resource.h>
#include <mysql.h>

//#include "mxq_util.h"

#include "mx_util.h"

#ifdef MX_NDEBUG_MYSQL
#   include <assert.h>
#   define mx_mysql_assert_return_minus_errno(test, eno) \
           assert(test)
#   define mx_mysql_assert_return_NULL(test, eno) \
           assert(test)
#else
#   define mx_mysql_assert_return_minus_errno(test, eno) \
           mx_assert_return_minus_errno(test, eno)
#   define mx_mysql_assert_return_NULL(test, eno) \
           mx_assert_return_NULL(test, eno)
#endif

#define mx_mysql_assert_usage_ok(res)  \
    do { \
        if ((res) < 0) { \
            assert((res) == -errno); \
            assert(errno != EINVAL); \
            assert(errno != EBADF); \
            assert(errno != EUCLEAN); \
        } \
    } while (0)


struct mx_mysql {
    MYSQL *mysql;

    const char *func;

    char *default_file;
    char *default_group;
    my_bool reconnect;

    unsigned int saved_errno;
    const char *error;
    const char *sqlstate;
};

struct mx_mysql_bind_data {
    char flags;

    char **string_ptr;
    unsigned long length;
    my_bool is_null;
    my_bool is_error;
};

enum mx_mysql_bind_type {
    MX_MYSQL_BIND_TYPE_UNKNOWN,
    MX_MYSQL_BIND_TYPE_PARAM,
    MX_MYSQL_BIND_TYPE_RESULT
};

struct mx_mysql_bind {
    enum mx_mysql_bind_type type;
    unsigned long count;

    MYSQL_BIND *bind;

    struct mx_mysql_bind_data *data;
};

struct mx_mysql_stmt {
    struct mx_mysql *mysql;

    char *statement;

    MYSQL_STMT *stmt;

    const char *func;

    unsigned long field_count;
    unsigned long param_count;

    struct mx_mysql_bind result;
    struct mx_mysql_bind param;
};

/*
#include <string.h>
static inline void mx_mysql_bind_string2(struct mx_mysql_bind *b, int index)
{
    assert(index < b->count);
    b->bind[index].buffer_type = MYSQL_TYPE_STRING;
    b->bind[index].buffer = 0;
    b->bind[index].buffer_length = 0;
    b->bind[index].length = &(b->length[index]);
}

static inline void mx_mysql_bind_string3(struct mx_mysql_bind *b, int index, char *value)
{
    assert(index < b->count);
    b->bind[index].buffer_type = MYSQL_TYPE_STRING;
    b->bind[index].buffer = value;
    b->bind[index].buffer_length = strlen(value);
    b->bind[index].length = &(b->bind[index].buffer_length);
}

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
*/
#ifndef MX_MYSQL_FAIL_WAIT_DEFAULT
#   ifdef MX_CALLOC_FAIL_WAIT_DEFAULT
#       define MX_MYSQL_FAIL_WAIT_DEFAULT MX_CALLOC_FAIL_WAIT_DEFAULT
#   else
#       define MX_MYSQL_FAIL_WAIT_DEFAULT 5
#   endif
#endif

#define mx_mysql_statement_param_bind(s, i, t, p)  mx_mysql_bind_##t(&((s)->param), (i), (p))
#define mx_mysql_statement_result_bind(s, i, t, p) mx_mysql_bind_##t(&((s)->result), (i), (p))

int mx_mysql_init(struct mx_mysql **);
int mx_mysql_free(struct mx_mysql **mysql);

int mx_mysql_option_set_default_file(struct mx_mysql *mysql, char *fname);
int mx_mysql_option_set_default_group(struct mx_mysql *mysql, char *group);

char *mx_mysql_option_get_default_file(struct mx_mysql *mysql);
char *mx_mysql_option_get_default_group(struct mx_mysql *mysql);

int mx_mysql_connect(struct mx_mysql **mysql);
int mx_mysql_connect_forever_sec(struct mx_mysql **mysql, unsigned int seconds);
#define mx_mysql_connect_forever(m) mx_mysql_connect_forever_sec((m), MX_MYSQL_FAIL_WAIT_DEFAULT)

int mx_mysql_disconnect(struct mx_mysql *mysql);

int mx_mysql_end(void);

int mx_mysql_finish(struct mx_mysql **mysql);

int mx_mysql_statement_init(struct mx_mysql *mysql, struct mx_mysql_stmt **stmt);
int mx_mysql_statement_prepare(struct mx_mysql_stmt *stmt, char *statement);
int mx_mysql_statement_execute(struct mx_mysql_stmt *stmt, unsigned long long *count);

int mx_mysql_statement_insert_id(struct mx_mysql_stmt *stmt, unsigned long long int *id);
int mx_mysql_statement_affected_rows(struct mx_mysql_stmt *stmt, unsigned long long int *count);
int mx_mysql_statement_num_rows(struct mx_mysql_stmt *stmt, unsigned long long int *count);

int mx_mysql_statement_fetch(struct mx_mysql_stmt *stmt);

int mx_mysql_statement_field_count(struct mx_mysql_stmt *stmt);
int mx_mysql_statement_param_count(struct mx_mysql_stmt *stmt);
int mx_mysql_statement_close(struct mx_mysql_stmt **stmt);

#define mx_mysql_statement_param_bind(s, i, t, p) mx_mysql_bind_##t(&((s)->param), (i), (p))

//int mx_mysql_bind_validate(struct mx_mysql_bind *b);
int mx_mysql_bind_string(struct mx_mysql_bind *b, unsigned int index, char **value);

int mx_mysql_bind_int8(struct mx_mysql_bind *b, unsigned int index, int8_t *value);
int mx_mysql_bind_int16(struct mx_mysql_bind *b, unsigned int index, int16_t *value);
int mx_mysql_bind_int32(struct mx_mysql_bind *b, unsigned int index, int32_t *value);
int mx_mysql_bind_int64(struct mx_mysql_bind *b, unsigned int index, int64_t *value);

int mx_mysql_bind_uint8(struct mx_mysql_bind *b, unsigned int index, uint8_t *value);
int mx_mysql_bind_uint16(struct mx_mysql_bind *b, unsigned int index, uint16_t *value);
int mx_mysql_bind_uint32(struct mx_mysql_bind *b, unsigned int index, uint32_t *value);
int mx_mysql_bind_uint64(struct mx_mysql_bind *b, unsigned int index, uint64_t *value);



#endif
