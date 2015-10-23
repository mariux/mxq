#include <sys/types.h>
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <dirent.h>
#include <ctype.h>

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
    struct mx_proc_pid_stat *pstat;
    int res;

    pstat = *pps;
    if (!pstat)
        pstat = mx_calloc_forever(1, sizeof(*pstat));

    res = mx_proc_pid_stat_read(pstat, "/proc/%d/stat", pid);
    if (res < 0)
        return res;

    *pps = pstat;
    return 0;
}

int mx_proc_pid_task_tid_stat(struct mx_proc_pid_stat **pps, pid_t pid, pid_t tid)
{
    struct mx_proc_pid_stat *pstat;
    int res;

    pstat = *pps;
    if (!pstat)
        pstat = mx_calloc_forever(1, sizeof(*pstat));

    res = mx_proc_pid_stat_read(pstat, "/proc/%d/task/%d/stat", pid, tid);
    if (res < 0)
        return res;

    *pps = pstat;
    return 0;
}

int mx_proc_pid_stat_read(struct mx_proc_pid_stat *pps, char *fmt, ...)
{
    _mx_cleanup_free_ char *fname = NULL;
    _mx_cleanup_free_ char *line = NULL;
    va_list ap;
    int res;

    assert(pps);

    va_start(ap, fmt);
    mx_vasprintf_forever(&fname, fmt, ap);
    va_end(ap);

    res = mx_read_first_line_from_file(fname, &line);
    if (res < 0)
        return res;

    res = _mx_proc_pid_stat_strscan(line, pps);
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

static void mx_proc_tree_update_parent_pinfo(struct mx_proc_tree_node *this, struct mx_proc_info *pinfo)
{
    if (!this)
        return;

    this->pinfo.sum_rss += pinfo->sum_rss;

    mx_proc_tree_update_parent_pinfo(this->parent, pinfo);
}

static void mx_proc_tree_add_to_list_sorted(struct mx_proc_tree_node **ptn_ptr, struct mx_proc_tree_node *new)
{
    struct mx_proc_tree_node *current;

    assert(new);
    assert(new->pinfo.pstat);
    assert(!new->next);
    assert(new->pinfo.pstat->pid > 0);

    current = *ptn_ptr;

    /* update stats */
    if (new->parent) {
        new->parent->nchilds++;
        mx_proc_tree_update_parent_pinfo(new->parent, &(new->pinfo));
    }

    /* empty list? -> start new list */
    if (!current) {
        *ptn_ptr = new;
        return;
    }

    /* new is first entry */
    if (new->pinfo.pstat->pid < current->pinfo.pstat->pid) {
        new->next = current;
        *ptn_ptr = new;
        return;
    }

    /* find position */
    while (1) {
        assert(new->pinfo.pstat->pid > current->pinfo.pstat->pid);

        /* new is last entry */
        if (!current->next) {
            current->next = new;
            break;
        }

        assert(current->next->pinfo.pstat->pid > current->pinfo.pstat->pid);

        /* add new between current and current->next */
        if (new->pinfo.pstat->pid < current->next->pinfo.pstat->pid) {
            new->next = current->next;
            current->next = new;
            break;
        }

        current = current->next;
    }

    return;
}

static struct mx_proc_tree_node *mx_proc_tree_find_by_pid(struct mx_proc_tree_node *ptn, long long int pid)
{
    assert(ptn);
    assert(pid >= 0);

    struct mx_proc_tree_node *current;
    struct mx_proc_tree_node *node;

    if (pid == 0)
        return NULL;

    current = ptn;

    for (current = ptn; current; current=current->next) {
        if (current->pinfo.pstat->pid == pid)
            return current;

        if (!current->childs)
            continue;

        node = mx_proc_tree_find_by_pid(current->childs, pid);
        if (node)
            return node;
    }

    return NULL;
}

#define ppid_or_pgrp(x) (((x)->ppid != 1 || (x)->pid == (x)->pgrp) ? (x)->ppid : (x)->pgrp)

static struct mx_proc_tree_node *mx_proc_tree_add(struct mx_proc_tree *pt, struct mx_proc_pid_stat *pps)
{
    assert(pps);
    assert(pt);
    struct mx_proc_tree_node *new;
    struct mx_proc_tree_node *current;
    struct mx_proc_tree_node *next;
    struct mx_proc_tree_node *parent;

    new = mx_calloc_forever(1, sizeof(*new));

    pt->nentries++;

    new->pinfo.pstat   = pps;
    new->pinfo.sum_rss = pps->rss;

    if (!(pt->root)) {
        pt->root = new;
        return new;
    }

    assert(pt->root);

    /* new is second to last roots parent? -> collect */
    current = pt->root;
    while (current->next) {
        if (ppid_or_pgrp(current->next->pinfo.pstat) != new->pinfo.pstat->pid) {
            current = current->next;
            continue;
        }
        assert(ppid_or_pgrp(current->next->pinfo.pstat) == new->pinfo.pstat->pid);

        /* disconnect next */
        next          = current->next;
        current->next = current->next->next;
        next->next    = NULL;

        /* add as child of new */
        next->parent = new;
        mx_proc_tree_add_to_list_sorted(&new->childs, next);
    }

    /* new is first roots parent? -> new is new root */
    if (ppid_or_pgrp(pt->root->pinfo.pstat)== new->pinfo.pstat->pid) {
        assert(!new->next);

        current = pt->root;
        pt->root = pt->root->next;

        current->next = NULL;
        current->parent = new;

        mx_proc_tree_add_to_list_sorted(&new->childs, current);

        if (!(pt->root)) {
            pt->root = new;
            return new;
        }
    }

    parent = mx_proc_tree_find_by_pid(pt->root, ppid_or_pgrp(new->pinfo.pstat));
    if (parent) {
        new->parent = parent;
        mx_proc_tree_add_to_list_sorted(&parent->childs, new);
    } else {
        mx_proc_tree_add_to_list_sorted(&pt->root, new);
    }

    return new;
}

static void mx_proc_tree_reorder_roots(struct mx_proc_tree *pt)
{
    struct mx_proc_tree_node *current;
    struct mx_proc_tree_node *pid1;
    struct mx_proc_tree_node *last = NULL;
    struct mx_proc_tree_node *next = NULL;

    for (current = pt->root; current; current = current->next) {
        if (current->pinfo.pstat->pid == 1) {
            pid1 = current;
            break;
        }
    }

    if (!pid1)
        return;

    for (current = pt->root; current; current = next) {
        next = current->next;

        if (current->pinfo.pstat->ppid != 1) {
            last = current;
            continue;
        }

        if (!last) {
            if (!current->next)
                return;
            pt->root = current->next;
        } else {
            last->next = current->next;
        }
        current->next = NULL;
        current->parent = pid1;
        mx_proc_tree_add_to_list_sorted(&pid1->childs, current);
    }
}

static int _mx_filter_numbers(const struct dirent *d)
{
    if (!isdigit(d->d_name[0]))
        return 0;

    return 1;
}

int mx_proc_tree(struct mx_proc_tree **newtree)
{
    struct mx_proc_tree *pt;
    struct dirent **namelist = NULL;
    struct mx_proc_pid_stat *pps;
    int n;
    int i;
    int res;
    unsigned long long int pid;

    assert(*newtree == NULL);

    pt = mx_calloc_forever(1, sizeof(*pt));

    n = scandir("/proc", &namelist, _mx_filter_numbers, NULL);
    if (n < 0)
        return -errno;

    if (n == 0)
        return -(errno=ENOENT);

    for (i=0; i < n; i++) {
        res = mx_strtoull(namelist[i]->d_name, &pid);
        free(namelist[i]);
        if (res < 0)
            continue;

        pps = NULL;
        res = mx_proc_pid_stat(&pps, pid);
        if (res < 0)
            continue;

        mx_proc_tree_add(pt, pps);
    }
    free(namelist);

    mx_proc_tree_reorder_roots(pt);

    *newtree = pt;
    return 0;
}

static void _mx_proc_tree_node_free_recursive(struct mx_proc_tree_node *ptn)
{
    assert(ptn);

    struct mx_proc_tree_node *current;
    struct mx_proc_tree_node *next;

    for (current = ptn; current; current=next) {

        if (current->childs)
            _mx_proc_tree_node_free_recursive(current->childs);

        next = current->next;

        mx_proc_pid_stat_free_content(current->pinfo.pstat);
        mx_free_null(current->pinfo.pstat);
        mx_free_null(current);
    }

    return;
}

int mx_proc_tree_free(struct mx_proc_tree **tree)
{
    struct mx_proc_tree *pt;

    pt = *tree;

    _mx_proc_tree_node_free_recursive(pt->root);

    mx_free_null(*tree);

    return 0;
}

struct mx_proc_info *mx_proc_tree_proc_info(struct mx_proc_tree *tree, pid_t pid)
{
    struct mx_proc_tree_node *ptn;

    assert(tree);

    ptn = mx_proc_tree_find_by_pid(tree->root, pid);

    if (!ptn)
        return NULL;

    return &(ptn->pinfo);
}
