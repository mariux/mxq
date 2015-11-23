
#define _GNU_SOURCE

#include <assert.h>

#include "mxqd.h"

#define MEMORY_TOTAL 20480
#define MEMORY_MAX_PER_SLOT 2048
#define SLOTS 10

void __init_server(struct mxq_server *server)
{
    server->memory_total        = MEMORY_TOTAL;
    server->memory_limit_slot_soft = MEMORY_MAX_PER_SLOT;
    server->slots               = SLOTS;
    server->memory_avg_per_slot = MEMORY_TOTAL / SLOTS;
}


static void test_mxqd_control(void)
{
    struct mxq_server _server = { 0 };
    struct mxq_server *server;

    server = &_server;

    __init_server(server);

    assert(1);
}

int main(int argc, char *argv[])
{
    test_mxqd_control();
    return 0;
}
