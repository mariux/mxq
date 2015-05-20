
#ifndef __MXQ_UTIL_H__
#define __MXQ_UTIL_H__ 1

#include <stdlib.h>
#include <string.h>
#include <mysql.h>

#include "mxq.h"

char *mxq_hostname(void);

void mxq_free_job(struct mxq_job_full *job);

char **stringtostringvec(int argc, char *s);
char *stringvectostring(int argc, char *argv[]);
int chrcnt(char *s, char c);

void *realloc_or_free(void *ptr, size_t size);
void *realloc_forever(void *ptr, size_t size);

char** strvec_new(void);
size_t strvec_length(char **strvec);
int    strvec_push_str(char ***strvecp, char *str);
int    strvec_push_strvec(char*** strvecp, char **strvec);
char*  strvec_to_str(char **strvec);
char** str_to_strvec(char *str);
void   strvec_free(char **strvec);

#define streq(a, b) (strcmp((a), (b)) == 0)

#define free_null(a) do { free((a)); (a) = NULL; } while(0)

int mxq_setenv(const char *name, const char *value);
int mxq_setenvf(const char *name, char *fmt, ...);

#endif
