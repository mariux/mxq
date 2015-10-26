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
#   define MXQ_VERSIONDATE "today"
#endif

#ifndef MXQ_MYSQL_DEFAULT_FILE
#   define MXQ_MYSQL_DEFAULT_FILE NULL
#   define MXQ_MYSQL_DEFAULT_FILE_STR "\"MySQL defaults\""
#else
#   define MXQ_MYSQL_DEFAULT_FILE_STR MXQ_MYSQL_DEFAULT_FILE
#endif

#ifdef MXQ_DEVELOPMENT
#   undef MXQ_MYSQL_DEFAULT_GROUP
#   define MXQ_MYSQL_DEFAULT_GROUP MXQ_MYSQL_DEFAULT_GROUP_DEVELOPMENT
#else
#   ifdef MXQ_TYPE_SERVER
#      ifdef MXQ_MYSQL_DEFAULT_GROUP_SERVER
#          define MXQ_MYSQL_DEFAULT_GROUP MXQ_MYSQL_DEFAULT_GROUP_SERVER
#      endif
#   else
#      ifdef MXQ_MYSQL_DEFAULT_GROUP_CLIENT
#           define MXQ_MYSQL_DEFAULT_GROUP MXQ_MYSQL_DEFAULT_GROUP_CLIENT
#      endif
#   endif
#   ifndef MXQ_MYSQL_DEFAULT_GROUP
#       define MXQ_MYSQL_DEFAULT_GROUP program_invocation_short_name
#   endif
#endif
#define MXQ_MYSQL_DEFAULT_GROUP_STR MXQ_MYSQL_DEFAULT_GROUP

#if defined (LOCALSTATEDIR)
#   define MXQ_LOGDIR LOCALSTATEDIR "/log"
#else
#   define MXQ_LOGDIR "/var/log"
#endif

static void mxq_print_generic_version(void)
{
    printf(
    "%s - " MXQ_VERSIONFULL "\n"
#ifdef MXQ_DEVELOPMENT
    "DEVELOPMENT VERSION: Do not use in production environments.\n"
#endif
    "  by Marius Tolzmann <marius.tolzmann@molgen.mpg.de> 2013-" MXQ_VERSIONDATE "\n"
    "     and Donald Buczek <buczek@molgen.mpg.de> 2015-" MXQ_VERSIONDATE "\n"
    "  Max Planck Institute for Molecular Genetics - Berlin Dahlem\n",
        program_invocation_short_name
    );
}

#endif
