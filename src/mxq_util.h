
#ifndef __MXQ_UTIL_H__
#define __MXQ_UTIL_H__ 1

#include <stdlib.h>
#include <string.h>
#include <mysql.h>

#include "mxq.h"

char *mxq_hostname(void);

int log_msg(int prio, const char *fmt, ...);

void mxq_free_task(struct mxq_task *task);
void mxq_free_job(struct mxq_job *job);

char **stringtostringvec(int argc, char *s);
char *stringvectostring(int argc, char *argv[]);
int chrcnt(char *s, char c);


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
