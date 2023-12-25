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

 void open_channels(int read_offset, int write_offset, int n) {
     int channel_dsc[2];
     int new_dsc;

     for (int i = 0; i < n; i++) {
         ASSERT_SYS_OK(channel(channel_dsc));

         // Check if current write descriptor and future read descriptor collide.
         if (channel_dsc[WRITE] == i + read_offset) {
            new_dsc = dup(channel_dsc[WRITE]);
            ASSERT_SYS_OK(new_dsc);
            ASSERT_SYS_OK(close(channel_dsc[WRITE]));
            channel_dsc[WRITE] = new_dsc;
         }

         if (channel_dsc[READ] != i + read_offset) {
             ASSERT_SYS_OK(dup2(channel_dsc[READ], i + read_offset));
             ASSERT_SYS_OK(close(channel_dsc[READ]));
         }

         if (channel_dsc[WRITE] != i + write_offset) {
             ASSERT_SYS_OK(dup2(channel_dsc[WRITE], i + write_offset));
             ASSERT_SYS_OK(close(channel_dsc[WRITE]));
         }
     }
}

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

    open_channels(MIMPI_MAIN_READ_OFFSET,
                  MIMPI_MAIN_WRITE_OFFSET, n);
    open_channels(MIMPI_SEM_READ_OFFSET,
                  MIMPI_SEM_WRITE_OFFSET, n);
    open_channels(MIMPI_QUEUE_READ_OFFSET,
                  MIMPI_QUEUE_WRITE_OFFSET, n);
    open_channels(MIMPI_GROUP_R_READ_OFFSET,
                  MIMPI_GROUP_R_WRITE_OFFSET, n);
    open_channels(MIMPI_GROUP_L_READ_OFFSET,
                  MIMPI_GROUP_L_WRITE_OFFSET, n);

    for (int i = 0; i < n; i++) {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);
        if (!pid) {
            // Close the main write descriptor.
            ASSERT_SYS_OK(close(i + MIMPI_MAIN_WRITE_OFFSET));

            // Close the semaphore write descriptors.
            ASSERT_SYS_OK(close(i + MIMPI_SEM_WRITE_OFFSET));

            // Close the group p read descriptor.
            ASSERT_SYS_OK(close(i + MIMPI_GROUP_R_READ_OFFSET));

            // Close the group l read descriptor.
            ASSERT_SYS_OK(close(i + MIMPI_GROUP_L_READ_OFFSET));

            for (int j = 0; j < n; j++) {
                if (j != i) {
                    // Close the main read descriptors.
                    ASSERT_SYS_OK(close(j + MIMPI_MAIN_READ_OFFSET));

                    // Close the semaphore read descriptor.
                    ASSERT_SYS_OK(close(j + MIMPI_SEM_READ_OFFSET));

                    // Close the queue read descriptors.
                    ASSERT_SYS_OK(close(j + MIMPI_QUEUE_READ_OFFSET));

                    // Close the group p write descriptor.
                    ASSERT_SYS_OK(close(j + MIMPI_GROUP_R_WRITE_OFFSET));

                    // Close the group l write descriptor.
                    ASSERT_SYS_OK(close(j + MIMPI_GROUP_L_WRITE_OFFSET));
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
        ASSERT_SYS_OK(close(i + MIMPI_GROUP_R_READ_OFFSET));
        ASSERT_SYS_OK(close(i + MIMPI_GROUP_R_WRITE_OFFSET));
    }

    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(wait(NULL));
    }
    return 0;
}