
#define _GNU_SOURCE

#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <errno.h>
#include <assert.h>

#include <stdarg.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "mxq_util.h"


mode_t getumask(void)
{
    mode_t mask = umask( 0 );
    umask(mask);
    return mask;
}


int log_msg(int prio, const char *fmt, ...)
{
    va_list ap;
    char *msg = NULL;
    static char *lastmsg;
    static int cnt = 0;
    int res;
    size_t len;

    if (!fmt) {
        free(lastmsg);
        return 0;
    }

    va_start(ap, fmt);
    len = vasprintf(&msg, fmt, ap);
    va_end(ap);
    
    if (len == -1)
        return 0;

    assert(len == strlen(msg));
    
    if (lastmsg) {
        res = strcmp(msg, lastmsg);
        if (res == 0) {
            cnt++;
            free(msg);
            return 2;
        }
        free(lastmsg);
    }
    lastmsg = msg;
    
    
    if (cnt) {
        printf("%s[%d]: last message repeated %d times\n",program_invocation_short_name, getpid(), cnt);
        cnt = 0;
    }
    printf("%s[%d]: %s",program_invocation_short_name, getpid(), msg);
    fflush(stdout);
    return 1;
}

char *mxq_hostname(void)
{
    static char hostname[1024] = "";
    int res;

    if (*hostname)
        return hostname;

    res = gethostname(hostname, 1024);
    if (res == -1) {
        if (errno != ENAMETOOLONG)
            assert_perror(errno);
        hostname[1024-1] = 0;
    }

    return hostname;
}

int safe_convert_string_to_ull(char *string, unsigned long long int *integer)
{
	unsigned long long int ull;
	char *endptr;

	assert(integer);
	
	if (!string) {
		*integer = 0;
		return 1;
    }
	
	errno = 0;
	
	ull = strtoull(string, &endptr, 0);

	if (errno) {
		return 0;
	}
	if (string == endptr || *endptr) {
		errno = EINVAL;
		return 0;
	}

	*integer = ull;

	return 1;
}


int safe_convert_string_to_ui8(char *string, u_int8_t *integer)
{
	unsigned long long int ull;
	int res;

    res = safe_convert_string_to_ull(string, &ull);
    
    if (res)
	   *integer = (u_int8_t)ull;

	return res;
}

int safe_convert_string_to_ui16(char *string, u_int16_t *integer)
{
	unsigned long long int ull;
	int res;

    res = safe_convert_string_to_ull(string, &ull);
    
    if (res)
	   *integer = (u_int16_t)ull;

	return res;
}


int safe_convert_string_to_ui32(char *string, u_int32_t *integer)
{
	unsigned long long int ull;
	int res;

    res = safe_convert_string_to_ull(string, &ull);
    
    if (res)
	   *integer = (u_int32_t)ull;

	return res;
}

int safe_convert_string_to_ui64(char *string, u_int64_t *integer)
{
	unsigned long long int ull;
	int res;

    res = safe_convert_string_to_ull(string, &ull);
    
    if (res)
	   *integer = (u_int64_t)ull;

	return res;
}

void mxq_free_job(struct mxq_job_full *job)
{
    if (!job)
        return;

    free(job);
}

char *stringvectostring(int argc, char *argv[])
{
    int     i,j,k;
    char   *buf;
    char   *s;
    size_t  len = 1;
    
    for (i=0; i < argc; i++) {
        len += strlen(argv[i]); 
        len += chrcnt(argv[i], '\\');
        len += 2;
    }
    
    buf = malloc(len);
    if (!buf)
        return NULL;
    
    for (i=0, k=0; i < argc; i++) {
        s = argv[i];
        for (j=0; j < strlen(s); j++) {
             buf[k++] = s[j];
             if (s[j] == '\\')
                 buf[k++] = '\\';
        }
        buf[k++] = '\\';
        buf[k++] = '0';
    }
    
    assert(k == len-1);
    buf[k] = 0;

    return buf;
}

char **stringtostringvec(int argc, char *s)
{
    int i;
    char *p;
    char **argv;
    
    argv = calloc(argc+1, sizeof(*argv));
    if (!argv)
        return NULL;    

    for (i=0, p=s; i < argc; i++) {
        argv[i] = p;
        p = strstr(p, "\\0");  /* search "\0" */
        if (!p) {
	    errno = EINVAL; /* "\0" need to be there or string is invalid */
	    return NULL;
        }
        *p = 0;     /* add end of string */
        p += 2;     /* skip "\0" */
    }

    return argv;
}

int chrcnt(char *s, char c) 
{
    int i = 0;
    char *p;
    
    p = s;
    
    while ((p = strchr(p, c))) {
        i++;
        p++;
    }
    
    return i; 
}
