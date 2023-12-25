#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <unistd.h>

int main() {
    MIMPI_Init(false);
    if (MIMPI_World_rank() == 0) {
        sleep(1);
    }

    if (MIMPI_World_rank() == 2) {
        int buf[1];
        int ret = chrecv(MIMPI_GROUP_R_READ_OFFSET, buf, sizeof(int));
        printf("%d", ret);
    }
    return 0;
}