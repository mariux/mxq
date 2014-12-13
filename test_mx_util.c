
#include <assert.h>
#include <errno.h>

#include "mx_util.h"

static void test_mx_strtoul(void)
{
    unsigned long int l;

    assert(mx_strtoul("123", &l) == 0);
    assert(l == 123);

    assert(mx_strtoul("  123  ", &l) == 0);
    assert(l == 123);

    assert(mx_strtoul("0173", &l) == 0);
    assert(l == 123);

    assert(mx_strtoul("0x007b", &l) == 0);
    assert(l == 123);

    assert(mx_strtoul("0x007B", &l) == 0);
    assert(l == 123);

    assert(mx_strtoul("+123", &l) == 0);
    assert(l == 123);

    assert(mx_strtoul("-1", &l) == -ERANGE);
    assert(mx_strtoul(" -1", &l) == -ERANGE);

    assert(mx_strtoul("0888", &l) == -EINVAL);
    assert(mx_strtoul("1.2", &l)  == -EINVAL);
    assert(mx_strtoul("1,2", &l)  == -EINVAL);
    assert(mx_strtoul("test", &l) == -EINVAL);
}

static void test_mx_strtoull(void)
{
    unsigned long long int l;

    assert(mx_strtoull("123", &l) == 0);
    assert(l == 123);

    assert(mx_strtoull("0173", &l) == 0);
    assert(l == 123);

    assert(mx_strtoull("0x007b", &l) == 0);
    assert(l == 123);

    assert(mx_strtoull("0x00000000000000000000000000000007b", &l) == 0);
    assert(l == 123);

    assert(mx_strtoull("0x007B", &l) == 0);
    assert(l == 123);

    assert(mx_strtoull("-127", &l) == -ERANGE);
    assert(mx_strtoull(" -128", &l) == -ERANGE);

    assert(mx_strtoull("0888", &l) == -EINVAL);
    assert(mx_strtoull("1.2", &l) == -EINVAL);
    assert(mx_strtoull("1,2", &l) == -EINVAL);
    assert(mx_strtoull("test", &l) == -EINVAL);
}

static void test_mx_strtou8(void)
{
    uint8_t u;

    assert(mx_strtou8("255", &u) == 0);
    assert(u == 255);

    assert(mx_strtou8("256", &u) == -ERANGE);
}

static void test_mx_strtoi8(void)
{
    int8_t u;

    assert(mx_strtoi8("127", &u) == 0);
    assert(u == 127);

    assert(mx_strtoi8("-127", &u) == 0);
    assert(u == -127);

    assert(mx_strtoi8(" -128", &u) == 0);
    assert(u == -128);

    assert(mx_strtoi8("128", &u) == -ERANGE);
    assert(mx_strtoi8("-129", &u) == -ERANGE);
}


int main(int argc, char *argv[])
{
    test_mx_strtoul();
    test_mx_strtoull();
    test_mx_strtou8();
    test_mx_strtoi8();

    return 0;
}
