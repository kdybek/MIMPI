/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"
#include <stdio.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <unistd.h>

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

    int channel_dsc[n][2];
    for (int i = 0; i < n; i++) { ASSERT_SYS_OK(channel(channel_dsc[i])); }

    int channel_dsc_sem[n][2];
    for (int i = 0; i < n; i++) { ASSERT_SYS_OK(channel(channel_dsc_sem[i])); }

    for (int i = 0; i < n; i++) {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);
        if (!pid) {
            // Close the main write descriptor.
            ASSERT_SYS_OK(close(channel_dsc[i][1]));

            // Replace the main read descriptor.
            ASSERT_SYS_OK(dup2(channel_dsc[i][0], i + MIMPI_MAIN_READ_OFFSET));
            ASSERT_SYS_OK(close(channel_dsc[i][0]));

            // Close the semaphore read descriptor.
            ASSERT_SYS_OK(close(channel_dsc_sem[i][0]));

            // Replace the semaphore write descriptor.
            ASSERT_SYS_OK(
                    dup2(channel_dsc_sem[i][1], i + MIMPI_SEM_WRITE_OFFSET));
            ASSERT_SYS_OK(close(channel_dsc[i][1]));

            for (int j = 0; j < n; j++) {
                if (j != i) {
                    // Close the main read descriptors.
                    ASSERT_SYS_OK(close(channel_dsc[j][0]));

                    // Replace the main write descriptors.
                    ASSERT_SYS_OK(dup2(channel_dsc[j][1],
                                       j + MIMPI_MAIN_WRITE_OFFSET));
                    ASSERT_SYS_OK(close(channel_dsc[j][1]));

                    // Close the semaphore write descriptors.
                    ASSERT_SYS_OK(close(channel_dsc_sem[j][1]));

                    // Replace the semaphore read descriptors.
                    ASSERT_SYS_OK(dup2(channel_dsc_sem[j][0],
                                       j + MIMPI_SEM_READ_OFFSET));
                    ASSERT_SYS_OK(close(channel_dsc[j][0]));
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
        ASSERT_SYS_OK(close(channel_dsc[i][0]));
        ASSERT_SYS_OK(close(channel_dsc[i][1]));
        ASSERT_SYS_OK(close(channel_dsc_sem[i][0]));
        ASSERT_SYS_OK(close(channel_dsc_sem[i][1]));
    }

    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(wait(NULL));
    }
    return 0;
}