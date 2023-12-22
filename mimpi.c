/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <unistd.h>

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    int rank = MIMPI_World_rank();

    // Create channel for helper process.
    int channel_dsc_aux[2];
    ASSERT_SYS_OK(channel(channel_dsc_aux));

    pid_t pid = fork();
    ASSERT_SYS_OK(pid);
    if (!pid) {

    } else {
        // Close aux write descriptor.
        ASSERT_SYS_OK(close(channel_dsc_aux[1]));

        // Close main read descriptor.
        ASSERT_SYS_OK(close(rank + MIMPI_MAIN_READ_OFFSET));

        // Close semaphore write.
    }
}

void MIMPI_Finalize() {
    TODO

    channels_finalize();
}

int MIMPI_World_size() {
    return atoi(getenv("MIMPI_size"));
}

int MIMPI_World_rank() {
    return atoi(getenv("MIMPI_rank"));
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}