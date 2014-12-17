#ifndef __MXQ_FLOCK_H__
#define __MXQ_FLOCK_H__ 1

struct mx_flock {
    int   fd;
    char *fname;
    int   operation;
    int   locked;
};

struct mx_flock *mx_flock(int operation, char *fmt, ...);
int mx_funlock(struct mx_flock *lock);

#endif
