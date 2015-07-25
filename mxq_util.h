
#ifndef __MXQ_UTIL_H__
#define __MXQ_UTIL_H__ 1

#include <stdlib.h>
#include <string.h>
#include <mysql.h>

char *mxq_hostname(void);

char** strvec_new(void);
size_t strvec_length(char **strvec);
int    strvec_push_str(char ***strvecp, char *str);
int    strvec_push_strvec(char*** strvecp, char **strvec);
char*  strvec_to_str(char **strvec);
char** strvec_from_str(char *str);
void   strvec_free(char **strvec);

#endif
