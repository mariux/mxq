#include <sys/types.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>

#include "mx_util.h"
#include "mx_proc.h"

static int _mx_proc_pid_stat_strscan(char *str, struct mx_proc_pid_stat *pps)
{
    size_t res = 0;
    char *p;
    char *s;

    pps->comm = NULL;

    s = str;

    res += mx_strscan_ll(&s, &(pps->pid));

    p = strrchr(s, ')');
    if (!p)
        return -(errno=EINVAL);

    *p = 0;
    s++;

    pps->comm = mx_strdup_forever(s);
    s = p + 2;

    pps->state = *s;
    res += !(*(s+1) == ' ');
    s += 2;

    res += mx_strscan_ll(&s, &(pps->ppid));
    res += mx_strscan_ll(&s, &(pps->pgrp));
    res += mx_strscan_ll(&s, &(pps->session));
    res += mx_strscan_ll(&s, &(pps->tty_nr));
    res += mx_strscan_ll(&s, &(pps->tpgid));
    res += mx_strscan_ull(&s, &(pps->flags));
    res += mx_strscan_ull(&s, &(pps->minflt));
    res += mx_strscan_ull(&s, &(pps->cminflt));
    res += mx_strscan_ull(&s, &(pps->majflt));
    res += mx_strscan_ull(&s, &(pps->cmajflt));
    res += mx_strscan_ull(&s, &(pps->utime));
    res += mx_strscan_ull(&s, &(pps->stime));
    res += mx_strscan_ll(&s, &(pps->cutime));
    res += mx_strscan_ll(&s, &(pps->cstime));
    res += mx_strscan_ll(&s, &(pps->priority));
    res += mx_strscan_ll(&s, &(pps->nice));
    res += mx_strscan_ll(&s, &(pps->num_threads));
    res += mx_strscan_ll(&s, &(pps->itrealvalue));
    res += mx_strscan_ull(&s, &(pps->starttime));
    res += mx_strscan_ull(&s, &(pps->vsize));
    res += mx_strscan_ll(&s, &(pps->rss));
    res += mx_strscan_ull(&s, &(pps->rsslim));
    res += mx_strscan_ull(&s, &(pps->startcode));
    res += mx_strscan_ull(&s, &(pps->endcode));
    res += mx_strscan_ull(&s, &(pps->startstack));
    res += mx_strscan_ull(&s, &(pps->kstkesp));
    res += mx_strscan_ull(&s, &(pps->kstkeip));
    res += mx_strscan_ull(&s, &(pps->signal));
    res += mx_strscan_ull(&s, &(pps->blocked));
    res += mx_strscan_ull(&s, &(pps->sigignore));
    res += mx_strscan_ull(&s, &(pps->sigcatch));
    res += mx_strscan_ull(&s, &(pps->wchan));
    res += mx_strscan_ull(&s, &(pps->nswap));
    res += mx_strscan_ull(&s, &(pps->cnswap));
    res += mx_strscan_ll(&s, &(pps->exit_signal));
    res += mx_strscan_ll(&s, &(pps->processor));
    res += mx_strscan_ull(&s, &(pps->rt_priority));
    res += mx_strscan_ull(&s, &(pps->policy));
    res += mx_strscan_ull(&s, &(pps->delayacct_blkio_ticks));
    res += mx_strscan_ull(&s, &(pps->guest_time));
    res += mx_strscan_ll(&s, &(pps->cguest_time));

    if (res != 0)
        return -(errno=EINVAL);

    return 0;
}

int mx_proc_pid_stat(struct mx_proc_pid_stat **pps, pid_t pid)
{
    _mx_cleanup_free_ char *fname = NULL;
    _mx_cleanup_free_ char *line = NULL;
    int res;

    mx_asprintf_forever(&fname, "/proc/%d/stat", pid);

    if (!*pps)
        *pps = mx_calloc_forever(1, sizeof(**pps));

    res = mx_read_first_line_from_file(fname, &line);
    if (res < 0)
        return res;

    res = _mx_proc_pid_stat_strscan(line, *pps);
    if (res < 0)
        return res;

    return 0;
}

void mx_proc_pid_stat_free_content(struct mx_proc_pid_stat *pps)
{
    if (!pps)
        return;

    mx_free_null(pps->comm);
}
