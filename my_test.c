#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <unistd.h>

int main() {
    MIMPI_Init(false);

    if (MIMPI_World_rank() == 0) {
        int buf[1];
        buf[0] = 8;
        printf("sending\n");
        ASSERT_SYS_OK(chsend(MIMPI_MAIN_WRITE_OFFSET + 2, buf, sizeof(int)));
        buf[0] = 420;
        ASSERT_SYS_OK(chsend(MIMPI_MAIN_WRITE_OFFSET + 2, buf, sizeof(int)));
        buf[0] = 2137;
    }
    if (MIMPI_World_rank() == 2) {
        int buf1[1];
        buf1[0] = 69;
        printf("%d\n", buf1[0]);
        printf("receiving\n");
        sleep(1);
        ASSERT_SYS_OK(chrecv(MIMPI_MAIN_READ_OFFSET + 2, buf1, sizeof(int)));
        printf("%d\n", buf1[0]);
        ASSERT_SYS_OK(chrecv(MIMPI_MAIN_READ_OFFSET + 2, buf1, sizeof(int)));
        printf("%d\n", buf1[0]);
    }
    return 0;
}