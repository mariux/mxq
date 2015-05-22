#ifndef __MXQ_H__
#define __MXQ_H__ 1

#ifndef _GNU_SOURCE
#   define _GNU_SOURCE
#endif

#include <errno.h>
#include <stdio.h>

#ifndef MXQ_VERSION
#   define MXQ_VERSION "0.00"
#endif

#ifndef MXQ_VERSIONFULL
#   define MXQ_VERSIONFULL "MXQ v0.00 super alpha 0"
#endif

#ifndef MXQ_VERSIONDATE
#   define MXQ_VERSIONDATE "2015"
#endif

#ifndef MXQ_MYSQL_DEFAULT_FILE
#   define MXQ_MYSQL_DEFAULT_FILE NULL
#   define MXQ_MYSQL_DEFAULT_FILE_STR "\"MySQL defaults\""
#else
#   define MXQ_MYSQL_DEFAULT_FILE_STR MXQ_MYSQL_DEFAULT_FILE
#endif

#ifndef MXQ_MYSQL_DEFAULT_GROUP
#   define MXQ_MYSQL_DEFAULT_GROUP     program_invocation_short_name
#endif
#define MXQ_MYSQL_DEFAULT_GROUP_STR MXQ_MYSQL_DEFAULT_GROUP

static void mxq_print_generic_version(void)
{
    printf(
    "%s - " MXQ_VERSIONFULL "\n"
    "  by Marius Tolzmann <tolzmann@molgen.mpg.de> " MXQ_VERSIONDATE "\n"
    "  Max Planck Institute for Molecular Genetics - Berlin Dahlem\n",
        program_invocation_short_name
    );
}

#endif
