
#define _cleanup_(x) __attribute__((cleanup(x)))

static inline void freep(void *p) {
        free(*(void**) p);
}

#define _cleanup_free_ _cleanup_(freep)

