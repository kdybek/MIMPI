#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <unistd.h>

int main() {
    MIMPI_Init(false);
    if (MIMPI_World_rank() != 9) {
        assert(MIMPI_Barrier() == MIMPI_ERROR_REMOTE_FINISHED);
    }
    MIMPI_Finalize();
    return 0;
}