/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"
#include <stdio.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <unistd.h>

#define READ 0
#define WRITE 1

#define OPEN_A_CHANNEL(i, type, read_offset, write_offset)    \
    ASSERT_SYS_OK(channel(channel_dsc_##type[i]));            \
    if (channel_dsc_##type[i][WRITE] == i + read_offset) {    \
        int new_dsc = dup(channel_dsc_##type[i][READ]);       \
        ASSERT_SYS_OK(new_dsc);                               \
        ASSERT_SYS_OK(close(channel_dsc_##type[i][WRITE]));   \
        channel_dsc_##type[i][WRITE] = new_dsc;               \
    }                                                         \
    ASSERT_SYS_OK(dup2(channel_dsc_##type[i][READ],           \
        i + read_offset));                                    \
    ASSERT_SYS_OK(close(channel_dsc_##type[i][READ]));        \
    ASSERT_SYS_OK(dup2(channel_dsc_##type[i][WRITE],          \
        i + write_offset));                                   \
    ASSERT_SYS_OK(close(channel_dsc_##type[i][WRITE]));

int main(int argc, char** argv) {
    if (argc < 3) {
        fatal("Usage: %s program_name number_of_processes [...]\n", argv[0]);
    }

    const int n = atoi(argv[2]);
    char* prog = argv[1];

    // A roundabout way to get the arguments for exec.
    // argv[2] gets overridden, but we already saved that as n.
    char** args = &argv[2];
    args[0] = prog;

    int channel_dsc_main[n][2];
    for (int i = 0; i < n; i++) {
        OPEN_A_CHANNEL(i, main, MIMPI_MAIN_READ_OFFSET, MIMPI_MAIN_WRITE_OFFSET)
    }

    int channel_dsc_sem[n][2];
    for (int i = 0; i < n; i++) {
        OPEN_A_CHANNEL(i, sem, MIMPI_SEM_READ_OFFSET, MIMPI_SEM_WRITE_OFFSET)
    }

    int channel_dsc_queue[n][2];
    for (int i = 0; i < n; i++) {
        OPEN_A_CHANNEL(i, queue, MIMPI_QUEUE_READ_OFFSET, MIMPI_QUEUE_WRITE_OFFSET)
    }

    for (int i = 0; i < n; i++) {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);
        if (!pid) {
            // Close the main write descriptor.
            // Can a process send a message to himself though?
            ASSERT_SYS_OK(close(i + MIMPI_MAIN_WRITE_OFFSET));

            // Close the semaphore write descriptors.
            ASSERT_SYS_OK(close(i + MIMPI_SEM_WRITE_OFFSET));

            for (int j = 0; j < n; j++) {
                if (j != i) {
                    // Close the main read descriptors.
                    ASSERT_SYS_OK(close(j + MIMPI_MAIN_READ_OFFSET));

                    // Close the semaphore read descriptor.
                    ASSERT_SYS_OK(close(j + MIMPI_SEM_READ_OFFSET));

                    // Close the queue read descriptors.
                    ASSERT_SYS_OK(close(j + MIMPI_QUEUE_READ_OFFSET));
                }
            }

            // Add additional info to environment.
            char i_str[4];
            int ret = snprintf(i_str, sizeof(i_str), "%d", i);
            if (ret < 0 || ret >= (int) sizeof(i_str)) {
                fatal("Error in snprintf.");
            }

            char n_str[4];
            ret = snprintf(n_str, sizeof(n_str), "%d", n);
            if (ret < 0 || ret >= (int) sizeof(n_str)) {
                fatal("Error in snprintf.");
            }

            ASSERT_SYS_OK(setenv("MIMPI_rank", i_str, true));
            ASSERT_SYS_OK(setenv("MIMPI_size", n_str, true));

            ASSERT_SYS_OK(execvp(prog, args));
        }
    }

    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(close(i + MIMPI_MAIN_READ_OFFSET));
        ASSERT_SYS_OK(close(i + MIMPI_MAIN_WRITE_OFFSET));
        ASSERT_SYS_OK(close(i + MIMPI_SEM_READ_OFFSET));
        ASSERT_SYS_OK(close(i + MIMPI_SEM_WRITE_OFFSET));
        ASSERT_SYS_OK(close(i + MIMPI_QUEUE_READ_OFFSET));
        ASSERT_SYS_OK(close(i + MIMPI_QUEUE_WRITE_OFFSET));
    }

    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(wait(NULL));
    }
    return 0;
}