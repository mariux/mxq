
#ifndef __MXQ_UTIL_H__
#define __MXQ_UTIL_H__ 1

#include <stdlib.h>
#include <string.h>
#include <mysql.h>

#include "mxq.h"

char *mxq_hostname(void);

int log_msg(int prio, const char *fmt, ...);

void mxq_free_job(struct mxq_job_full *job);

char **stringtostringvec(int argc, char *s);
char *stringvectostring(int argc, char *argv[]);
int chrcnt(char *s, char c);


int safe_convert_string_to_ull(char *string, unsigned long long int *integer);

int safe_convert_string_to_ui8(char *string, u_int8_t *integer);
int safe_convert_string_to_ui16(char *string, u_int16_t *integer);
int safe_convert_string_to_ui32(char *string, u_int32_t *integer);
int safe_convert_string_to_ui64(char *string, u_int64_t *integer);



#define streq(a, b) (strcmp((a), (b)) == 0)

#define _cleanup_(x) __attribute__((cleanup(x)))

static inline void freep(void *p) {
    free(*(void**) p);
}

#define _cleanup_free_ _cleanup_(freep)

static inline void free_mxq_taskp(void *p) {
    mxq_free_task(*(void**) p);
    free(*(void**) p);
}

#define _cleanup_free_mxq_task_ _cleanup_(free_mxq_taskp)


#endif
