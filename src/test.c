
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <malloc.h>

#include "mxq_util.h"

static void display_mallinfo(void)
{
    struct mallinfo mi;

    mi = mallinfo();

    printf("\nTotal non-mmapped bytes (arena):       %d\n", mi.arena);
    printf("# of free chunks (ordblks):            %d\n", mi.ordblks);
    printf("# of free fastbin blocks (smblks):     %d\n", mi.smblks);
    printf("# of mapped regions (hblks):           %d\n", mi.hblks);
    printf("Bytes in mapped regions (hblkhd):      %d\n", mi.hblkhd);
    printf("Max. total allocated space (usmblks):  %d\n", mi.usmblks);
    printf("Free bytes held in fastbins (fsmblks): %d\n", mi.fsmblks);
    printf("Total allocated space (uordblks):      %d\n", mi.uordblks);
    printf("Total free space (fordblks):           %d\n", mi.fordblks);
    printf("Topmost releasable block (keepcost):   %d\n", mi.keepcost);
}

int main(int argc, char *argv[])
{
    char** strvec1;
    char** strvec2;
    char** strvec3;
    char*  str1;
    char*  str2;
    char*  str3;
    int i;

    assert(strvec1 = strvec_new());
    assert(strvec_push_str(&strvec1, "test1"));
    assert(strvec_push_str(&strvec1, "test2"));
    assert(strvec_length(strvec1) == 2);

    display_mallinfo();

    assert(strvec2 = strvec_new());
    assert(strvec_push_str(&strvec2, "test3"));
    assert(strvec_push_str(&strvec2, "test4"));
    assert(strvec_push_str(&strvec2, "test5"));
    assert(strvec_push_str(&strvec2, "test6"));
    assert(strvec_push_str(&strvec2, "test7"));
    assert(strvec_length(strvec2) == 5);

    display_mallinfo();

    assert(strvec_push_strvec(&strvec1, strvec2));
    assert(strvec_push_strvec(&strvec2, strvec1));
    assert(strvec_length(strvec1) == 7);

    assert(str1 = strvec_to_str(strvec1));

    assert(str3 = strdup(str1));
    assert(strvec3 = str_to_strvec(str3));
    assert(str2 = strvec_to_str(strvec3));

    display_mallinfo();

    for (i=0; i<1000000; i++) {
        strvec_push_str(&strvec3, "yeah");
    }
    assert(strvec_length(strvec3) == 1000000+7);


    display_mallinfo();
    malloc_stats();

    free(str1);
    free(str2);
    free(str3);
    free(strvec1);
    free(strvec2);
    free(strvec3);

    display_mallinfo();
    malloc_stats();
    return 0;
}

