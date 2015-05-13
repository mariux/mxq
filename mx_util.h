#ifndef __MX_UTIL_H__
#define __MX_UTIL_H__ 1

#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>

#include "mx_log.h"

#ifdef MX_NDEBUG
#   include <assert.h>
#   define mx_assert_return_minus_errno(test, eno) \
        assert(test)
#   define mx_assert_return_NULL(test, eno) \
        assert(test)
#else
#   define mx_assert_return_minus_errno(test, eno) \
            do {\
                if (!(test)) {\
                    errno=(eno);\
                    mx_log_debug("Assertion '" #test "' failed. Returning -(errno=" #eno ") [%d]: %m", -errno);\
                    return -errno;\
                }\
            } while (0)

#   define mx_assert_return_NULL(test, eno) \
            do {\
                if (!(test)) {\
                    errno=(eno);\
                    mx_log_debug("Assertion '" #test "' failed. Setting errno=" #eno " [%d] and returning NULL: %m", errno);\
                    return NULL;\
                }\
            } while (0)
#endif

#undef mx_free_null
#define mx_free_null(a) do { free(a); (a) = NULL; } while(0)

int mx_strbeginswith(char *str, const char *start, char **endptr);
int mx_stribeginswith(char *str, const char *start, char **endptr);
int mx_strbeginswithany(char *str, char **starts, char **endptr);

char *mx_strskipwhitespaces(char *str);

int mx_strtoul(char *str,  unsigned long int *to);
int mx_strtoull(char *str, unsigned long long int *to);

int mx_strtoui(char *str,  unsigned int *to);
int mx_strtou8(char *str,  uint8_t *to);
int mx_strtou16(char *str, uint16_t *to);
int mx_strtou32(char *str, uint32_t *to);
int mx_strtou64(char *str, uint64_t *to);

int mx_strtol(char *str,  signed long int *to);
int mx_strtoll(char *str, signed long long int *to);

int mx_strtoi(char *str,  signed int *to);
int mx_strtoi8(char *str,  int8_t *to);
int mx_strtoi16(char *str, int16_t *to);
int mx_strtoi32(char *str, int32_t *to);
int mx_strtoi64(char *str, int64_t *to);

char *mx_strdup_forever(char *str);
int mx_vasprintf_forever(char **strp, const char *fmt, va_list ap);
int mx_asprintf_forever(char **strp, const char *fmt, ...)  __attribute__ ((format(printf, 2, 3)));

char *mx_dirname(char *path);
char *mx_dirname_forever(char *path);

int mx_dup2_close_new(int oldfd, int newfd);
int mx_dup2_close_both(int oldfd, int newfd);

int mx_setenv_forever(const char *name, const char *value);
int mx_setenvf_forever(const char *name, char *fmt, ...) __attribute__ ((format(printf, 2, 3)));

int mx_open_newfile(char *fname);

int mx_sleep(unsigned int seconds);
int mx_sleep_nofail(unsigned int seconds);

#endif
