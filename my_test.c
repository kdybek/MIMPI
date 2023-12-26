#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <unistd.h>

int main() {
    MIMPI_Init(false);
    MIMPI_Finalize();
    return 0;
}