/*
** beegetopt - parse options
**
** Copyright (C) 2009-2012
**       Marius Tolzmann <tolzmann@molgen.mpg.de>
**       and other bee developers
**
** This file is part of bee.
**
** bee is free software; you can redistribute it and/or modify
** it under the terms of the GNU General Public License as published by
** the Free Software Foundation, either version 3 of the License, or
** (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
** GNU General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with this program; if not, see <http://www.gnu.org/licenses/>.
*/

#ifndef MX_GETOPT_H
#define MX_GETOPT_H 1

#define MX_TYPE_STRING     1
#define MX_TYPE_INTEGER    2
#define MX_TYPE_FLOAT      3
#define MX_TYPE_FLAG       4
#define MX_TYPE_NO         5
#define MX_TYPE_ENABLE     6
#define MX_TYPE_WITH       7
#define MX_TYPE_TOGGLE     8
#define MX_TYPE_COUNT      9

#define MX_FLAG_SKIPUNKNOWN   (1<<0)
#define MX_FLAG_STOPONNOOPT   (1<<1)
#define MX_FLAG_STOPONUNKNOWN (1<<2)
#define MX_FLAG_KEEPOPTIONEND (1<<3)

#define MX_OPT_LONG(name)     .long_opt      = (name)
#define MX_OPT_SHORT(short)   .short_opt     = (short)
#define MX_OPT_VALUE(v)       .value         = (v)
#define MX_OPT_FLAG(f)        .flag          = (f)
#define MX_OPT_TYPE(t)        .type          = (t)
#define MX_OPT_OPTIONAL(args) .optional_args = (args)
#define MX_OPT_REQUIRED(args) .required_args = (args)
#define _MX_OPT_LEN(l)        ._long_len     = (l)

#define MX_GETOPT_END        -1
#define MX_GETOPT_ERROR      -2
#define MX_GETOPT_NOVALUE    -3
#define MX_GETOPT_NOOPT      -4
#define MX_GETOPT_AMBIGUOUS  -5
#define MX_GETOPT_OPTUNKNOWN -6
#define MX_GETOPT_NOARG      -7
#define MX_GETOPT_NOARGS     -8

#define MX_OPTION_DEFAULTS \
            MX_OPT_LONG(NULL), \
            MX_OPT_SHORT(0), \
            MX_OPT_VALUE(MX_GETOPT_NOVALUE), \
            MX_OPT_FLAG(NULL), \
            MX_OPT_TYPE(MX_TYPE_FLAG), \
            MX_OPT_OPTIONAL(0), \
            MX_OPT_REQUIRED(0), \
            _MX_OPT_LEN(0)


#define MX_INIT_OPTION_DEFAULTS(opt) \
            (opt)->long_opt  = NULL; \
            (opt)->short_opt = 0; \
            (opt)->value = MX_GETOPT_NOVALUE; \
            (opt)->flag = NULL; \
            (opt)->type = MX_TYPE_FLAG; \
            (opt)->optional_args = 0; \
            (opt)->required_args = 0; \
            (opt)->_long_len = 0


#define MX_INIT_OPTION_END(opt) MX_INIT_OPTION_DEFAULTS((opt))


#define MX_OPTION_END { MX_OPT_LONG(NULL), MX_OPT_SHORT(0) }

#define MX_OPTION(...) { MX_OPTION_DEFAULTS, ## __VA_ARGS__ }

#define MX_OPTION_NO_ARG(name, short, ...) \
            MX_OPTION(MX_OPT_LONG(name), \
                       MX_OPT_SHORT(short), \
                       MX_OPT_VALUE(short), \
                       ## __VA_ARGS__)

#define MX_OPTION_REQUIRED_ARG(name, short, ...) \
            MX_OPTION(MX_OPT_LONG(name), \
                       MX_OPT_SHORT(short), \
                       MX_OPT_VALUE(short), \
                       MX_OPT_TYPE(MX_TYPE_STRING), \
                       MX_OPT_REQUIRED(1), \
                       ## __VA_ARGS__ )

#define MX_OPTION_REQUIRED_ARGS(name, short, n, ...) \
            MX_OPTION(MX_OPT_LONG(name), \
                       MX_OPT_SHORT(short), \
                       MX_OPT_VALUE(short), \
                       MX_OPT_TYPE(MX_TYPE_STRING), \
                       MX_OPT_REQUIRED(n), \
                       ## __VA_ARGS__ )

#define MX_OPTION_OPTIONAL_ARG(name, short, ...) \
            MX_OPTION(MX_OPT_LONG(name), \
                       MX_OPT_SHORT(short), \
                       MX_OPT_VALUE(short), \
                       MX_OPT_TYPE(MX_TYPE_STRING), \
                       MX_OPT_OPTIONAL(1), \
                       ## __VA_ARGS__ )

#define MX_OPTION_OPTIONAL_ARGS(name, short, n, ...) \
            MX_OPTION(MX_OPT_LONG(name), \
                       MX_OPT_SHORT(short), \
                       MX_OPT_VALUE(short), \
                       MX_OPT_TYPE(MX_TYPE_STRING), \
                       MX_OPT_OPTIONAL(n), \
                       ## __VA_ARGS__ )

#define MX_OPTION_ARGS(name, short, opt, req, ...) \
            MX_OPTION(MX_OPT_LONG(name), \
                       MX_OPT_SHORT(short), \
                       MX_OPT_VALUE(short), \
                       MX_OPT_TYPE(MX_TYPE_STRING), \
                       MX_OPT_OPTIONAL(opt), \
                       MX_OPT_REQUIRED(req), \
                       ## __VA_ARGS__ )

#define MX_GETOPT_FINISH(optctl, argc, argv) \
             do { \
                 (argv) = &(optctl).argv[(optctl).optind]; \
                 (argc) = (optctl).argc - (optctl).optind; \
             } while(0)

struct mx_option {
    char *long_opt;
    char  short_opt;

    int  value;
    int *flag;

    int  type;

    int  optional_args;
    int  required_args;

    int  _long_len;
};

struct mx_getopt_ctl {
    int    optind;

    char  *program;

    char  *optarg;

    int    optargc;
    char **optargv;

    int    argc;
    char **argv;

    struct mx_option *options;

    int   _argc;
    int   _optcnt;
    char *_unhandled_shortopts;

    int flags;
};

void mx_getopt_pop_current_argument(struct mx_getopt_ctl *optctl);

int mx_getopt_init(struct mx_getopt_ctl *ctl, int argc, char **argv, struct mx_option *optv);
int mx_getopt_long(struct mx_getopt_ctl *optctl, int *optindex);
int mx_getopt(struct mx_getopt_ctl *optctl, int *optindex);

void mx_getopt_print_quoted(char *s);

#endif
