#ifndef __MX_UTIL_H__
#define __MX_UTIL_H__ 1

#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <stdio.h>

#include "mx_log.h"

struct proc_pid_stat {
    long long int pid;     /* 1 */
    char *comm;            /* 2 (comm) */
    char state;            /* 3 "RSDZTW" */
    long long int ppid;    /* 4 */
    long long int pgrp;    /* 5 */
    long long int session; /* 6 */
    long long int tty_nr;  /* 7 */
    long long int tpgid;   /* 8 */
    unsigned long long int flags;   /*  9 */
    unsigned long long int minflt;  /* 10 */
    unsigned long long int cminflt; /* 11 */
    unsigned long long int majflt;  /* 12 */
    unsigned long long int cmajflt; /* 13 */
    unsigned long long int utime;   /* 14 */
    unsigned long long int stime;   /* 15 */
    long long int cutime;   /* 16 */
    long long int cstime;   /* 17 */
    long long int priority; /* 18 */
    long long int nice;     /* 19 */
    long long int num_threads;          /* 20 */
    long long int itrealvalue;          /* 21 */
    unsigned long long int starttime;   /* 22 */
    unsigned long long int vsize;       /* 23 */
    long long int rss;                  /* 24 */
    unsigned long long int rsslim;      /* 25 */
    unsigned long long int startcode;   /* 26 */
    unsigned long long int endcode;     /* 27 */
    unsigned long long int startstack;  /* 28 */
    unsigned long long int kstkesp;     /* 29 */
    unsigned long long int kstkeip;     /* 30 */
    unsigned long long int signal;      /* 31 */
    unsigned long long int blocked;     /* 32 */
    unsigned long long int sigignore;   /* 33 */
    unsigned long long int sigcatch;    /* 34 */
    unsigned long long int wchan;       /* 35 */
    unsigned long long int nswap;       /* 36 */
    unsigned long long int cnswap;      /* 37 */
    long long int exit_signal;          /* 38 */
    long long int processor;            /* 39 */
    unsigned long long int rt_priority; /* 40 */
    unsigned long long int policy;      /* 41 */
    unsigned long long int delayacct_blkio_ticks; /* 42 */
    unsigned long long int guest_time;  /* 43 */
    long long int cguest_time;          /* 44 */
};

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
                    mx_log_emerg("%s:%d:%s(): Assertion '" #test "' failed . Returning -(errno=" #eno ") [%d]: %m", __FILE__, __LINE__, __func__, -errno);\
                    return -errno;\
                }\
            } while (0)

#   define mx_assert_return_NULL(test, eno) \
            do {\
                if (!(test)) {\
                    errno=(eno);\
                    mx_log_emerg("%s:%d:%s(): Assertion '" #test "' failed. Setting errno=" #eno " [%d] and returning NULL: %m", __FILE__, __LINE__, __func__, errno);\
                    return NULL;\
                }\
            } while (0)
#endif

#define mx_debug_value(fmt, v)  mx_log_debug("mx_debug_value: " #v " = " fmt, v)

#undef mx_free_null
#define mx_free_null(a) do { free(a); (a) = NULL; } while(0)

#undef _mx_cleanup_
#define _mx_cleanup_(x) __attribute__((cleanup(x)))

static inline void __mx_free(void *ptr) {
    free(*(void **)ptr);
}

static inline void __mx_fclose(FILE **ptr) {
    if (*ptr)
        fclose(*ptr);
}

#undef _mx_cleanup_free_
#define _mx_cleanup_free_ _mx_cleanup_(__mx_free)

#undef _mx_cleanup_fclose_
#define _mx_cleanup_fclose_ _mx_cleanup_(__mx_fclose)

#undef likely
#define likely(x)       __builtin_expect((x),1)

#undef unlikely
#define unlikely(x)     __builtin_expect((x),0)

#undef mx_streq
#define mx_streq(a, b) (strcmp((a), (b)) == 0)

#undef mx_streq_nocase
#define mx_streq_nocase(a, b) (strcasecmp((a), (b)) == 0)

int mx_strbeginswith(char *str, const char *start, char **endptr);
int mx_stribeginswith(char *str, const char *start, char **endptr);
int mx_strbeginswithany(char *str, char **starts, char **endptr);

char *mx_strskipwhitespaces(char *str);

int mx_strtobytes(char *str, unsigned long long int *bytes);

int mx_strtoseconds(char *str, unsigned long long int *seconds);
int mx_strtominutes(char *str, unsigned long long int *minutes);

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

void *mx_malloc_forever(size_t size);
char *mx_strdup_forever(char *str);
int mx_vasprintf_forever(char **strp, const char *fmt, va_list ap);
int mx_asprintf_forever(char **strp, const char *fmt, ...)  __attribute__ ((format(printf, 2, 3)));

char *mx_hostname(void);
char *mx_dirname(char *path);
char *mx_dirname_forever(char *path);

int mx_dup2_close_new(int oldfd, int newfd);
int mx_dup2_close_both(int oldfd, int newfd);

int mx_setenv_forever(const char *name, const char *value);
int mx_setenvf_forever(const char *name, char *fmt, ...) __attribute__ ((format(printf, 2, 3)));

int mx_open_newfile(char *fname);

int mx_read_first_line_from_file(char *fname, char **line);

int mx_strscan_ull(char **str, unsigned long long int *to);
int mx_strscan_ll(char **str, long long int *to);
int mx_strscan_proc_pid_stat(char *str, struct proc_pid_stat *pps);

int mx_proc_pid_stat(struct proc_pid_stat *pps, pid_t pid);
void mx_proc_pid_stat_free(struct proc_pid_stat *pps);

int mx_sleep(unsigned int seconds);
int mx_sleep_nofail(unsigned int seconds);

#ifndef MX_CALLOC_FAIL_WAIT_DEFAULT
#   define MX_CALLOC_FAIL_WAIT_DEFAULT 1
#endif

#define mx_calloc_forever(n, s) mx_calloc_forever_sec((n), (s), MX_CALLOC_FAIL_WAIT_DEFAULT)

void *mx_calloc_forever_sec(size_t nmemb, size_t size, unsigned int time);

char** mx_strvec_new(void);
size_t mx_strvec_length(char **strvec);
int    mx_strvec_push_str(char ***strvecp, char *str);
int    mx_strvec_push_strvec(char*** strvecp, char **strvec);
char*  mx_strvec_to_str(char **strvec);
char** mx_strvec_from_str(char *str);
void   mx_strvec_free(char **strvec);
char*  mx_strvec_join(char *sep,char **strvec);

#endif
