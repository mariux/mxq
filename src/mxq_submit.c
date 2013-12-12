




#define _GNU_SOURCE


#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>

#include <sys/types.h>

#include <pwd.h>

#include <assert.h>

#include <sysexits.h>

#include <string.h>

#include "bee_getopt.h"

       struct passwd *getpwnam(const char *name);

       struct passwd *getpwuid(uid_t uid);


       uid_t getuid(void);
       uid_t geteuid(void);


int getresuid(uid_t *ruid, uid_t *euid, uid_t *suid);


int mxq_submit_job(char *username, char *workdir, char *command, int argc, char *argv[], char *stdout, char *stderr)
{
    int i;
    char *args = NULL;
    char *p;
  
    int totallen;
    
    printf("Starting '%s' as '%s' in directory '%s'\n", command, username, workdir);
    printf("\targuments: ");
    
    for (i=0, totallen=1; i < argc; i++) {
        printf("'%s' ", argv[i]);

        totallen += strlen(argv[i]) + 1;
    }
    
    
    
    printf("\n\tstdout = %s\n", stdout);
    printf("\tstderr = %s\n", stderr);

    args = calloc(sizeof(*args), totallen);
    assert(args);
    
    for (i=0, p=args; i < argc; i++) {
        int len;
        
        len = strlen(argv[i]);
        strcpy(p, argv[i]);
        p += len+1;
    }

    printf("totallen = %d\nargs = %s\n", totallen, args);
    
}

int main(int argc, char *argv[])
{
    int i;
    uid_t ruid;
    uid_t euid;
    uid_t suid;
    int res;
    
    char *arg_stdout = "/dev/null";
    char *arg_stderr = "/dev/null";
    
    int opt;
    
    struct bee_getopt_ctl optctl;
    
    struct bee_option opts[] = {
        BEE_OPTION_NO_ARG("help",          'h'),
        BEE_OPTION_NO_ARG("version",       'V'),
        BEE_OPTION_REQUIRED_ARG("stdout",  'o'),
        BEE_OPTION_REQUIRED_ARG("stderr",  'e'),
        BEE_OPTION_REQUIRED_ARG("workdir", 'p'),
        BEE_OPTION_END
    };

    char *arg_cwd = NULL;

    struct passwd *passwd;

    res = getresuid(&ruid, &euid, &suid);

    assert(res != -1);

    printf("ruid: %5d\neuid: %5d\nsuid: %5d\n", ruid, euid, suid);
    
    for (i=0; i < argc; i++) {
       printf("arg %2d: %s\n", i, argv[i]);
    }


    bee_getopt_init(&optctl, argc, argv, opts);

    optctl.flags = BEE_FLAG_STOPONUNKNOWN;

    while ((opt=bee_getopt(&optctl, &i)) != BEE_GETOPT_END) {
        printf("    i = %2d\n", i);
        if (opt == BEE_GETOPT_ERROR) {
            exit(EX_USAGE);
        }
        
        switch (opt) {
            case 'h':
            case 'V':
                printf("help/version\n");
                exit(EX_USAGE);
                
            case 'o':
                arg_stdout = optctl.optarg;
                break;
                
            case 'e':
                arg_stderr = optctl.optarg;
                break;
                
            case 'p':
                arg_cwd = optctl.optarg;
                break;
                
        }
    }

    BEE_GETOPT_FINISH(optctl, argc, argv);


    printf("stdout = %s\n", arg_stdout);
    printf("stderr = %s\n", arg_stderr);



    for (i=1; i < argc; i++) {
       printf("command %2d: %s\n", i, argv[i]);
    }

    if (!arg_cwd || arg_cwd[0] != '/') {
        arg_cwd = get_current_dir_name();
    }
    
    assert(arg_cwd != NULL);

    printf("cwd = %s\n", arg_cwd);
  

    passwd = getpwuid(ruid);
    
    assert(passwd != NULL);
    
    printf("username = %s\n", passwd->pw_name);

    assert(argc >= 2);
 
    mxq_submit_job(passwd->pw_name, arg_cwd, argv[1], argc-2, &argv[2], arg_stdout, arg_stderr);
}

