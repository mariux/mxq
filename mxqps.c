
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <ctype.h>
#include <assert.h>
#include <unistd.h>

#include "mx_util.h"
#include "mx_log.h"
#include "mx_proc.h"


int filter(const struct dirent *d)
{
    if (!isdigit(d->d_name[0]))
        return 0;

    return 1;
}

#define MX_PROC_TREE_NODE_IS_KERNEL_THREAD(x)  ((x)->pinfo.pstat->ppid == 0 && (x)->pinfo.sum_rss == 0)

int mx_proc_tree_node_print_debug(struct mx_proc_tree_node *ptn, int lvl)
{
    assert(ptn);

    struct mx_proc_tree_node *current;

    current = ptn;

    long pagesize;

    pagesize = sysconf(_SC_PAGESIZE);
    assert(pagesize);

    for (current = ptn; current; current=current->next) {
        if (MX_PROC_TREE_NODE_IS_KERNEL_THREAD(current))
            continue;

        printf("%7lld %7lld %7lld %7lld %15lld %15lld %7lld",
                current->pinfo.pstat->pid,
                current->pinfo.pstat->ppid,
                current->pinfo.pstat->pgrp,
                current->pinfo.pstat->session,
                current->pinfo.pstat->rss*pagesize/1024,
                current->pinfo.sum_rss*pagesize/1024,
                current->pinfo.pstat->num_threads);

        if (lvl>0)
            printf("%*s", lvl*4, "\\_");
        assert(current->pinfo.pstat);
        printf(" %s\n", current->pinfo.pstat->comm);

        if (!current->childs)
            continue;

        mx_proc_tree_node_print_debug(current->childs, lvl+(current->parent != NULL));
    }

    return 0;
}

int mx_proc_tree_print_debug(struct mx_proc_tree *pt)
{
    assert(pt);
    printf("%7s %7s %7s %7s %15s %15s %7s COMMAND\n",
            "PID",
            "PPID",
            "PGRP",
            "SESSION",
            "RSS",
            "SUMRSS",
            "THREADS");
    mx_proc_tree_node_print_debug(pt->root, 0);
    return 0;
}

int main(void)
{
    int res;
    struct mx_proc_tree *pt = NULL;

    res = mx_proc_tree(&pt);
    assert(res == 0);

    mx_proc_tree_print_debug(pt);

    mx_proc_tree_free(&pt);


    return 0;

}
