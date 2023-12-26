#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <unistd.h>

int main() {
    MIMPI_Init(false);
    int buf[1];
    buf[0] = MIMPI_World_rank();
    if (MIMPI_World_rank() == 0) {
        MIMPI_Send(buf, sizeof(int), 2, 9);
    }
    if (MIMPI_World_rank() == 1) {
        MIMPI_Send(buf, sizeof(int), 2, 2);
    }
    if (MIMPI_World_rank() == 2) {
        printf("%d\n", buf[0]);
        MIMPI_Recv(buf, sizeof(int), 1, 2);
        printf("%d\n", buf[0]);
    }
    MIMPI_Finalize();
    return 0;
}