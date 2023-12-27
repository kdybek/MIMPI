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

 void open_channel(int read_dsc, int write_dsc) {
     int channel_dsc[2];
     int new_dsc;

     ASSERT_SYS_OK(channel(channel_dsc));

     // Check if current write descriptor and future read descriptor collide.
     if (channel_dsc[WRITE] == read_dsc) {
         new_dsc = dup(channel_dsc[WRITE]);
         ASSERT_SYS_OK(new_dsc);
         ASSERT_SYS_OK(close(channel_dsc[WRITE]));
         channel_dsc[WRITE] = new_dsc;
     }

     if (channel_dsc[READ] != read_dsc) {
         ASSERT_SYS_OK(dup2(channel_dsc[READ], read_dsc));
         ASSERT_SYS_OK(close(channel_dsc[READ]));
     }

     if (channel_dsc[WRITE] != write_dsc) {
         ASSERT_SYS_OK(dup2(channel_dsc[WRITE], write_dsc));
         ASSERT_SYS_OK(close(channel_dsc[WRITE]));
     }
}

int main(int argc, char** argv) {
    if (argc < 3) {
        fatal("Usage: %s program_name number_of_processes [...]\n", argv[0]);
    }

    const int n = atoi(argv[1]);
    char* prog = argv[2];

    // A roundabout way to get the arguments for exec.
    // argv[2] gets overridden, but we already saved that as n.
    char** args = &argv[2];

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            if (i != j) {
                open_channel(MIMPI_READ_OFFSET + MIMPI_MAX_N * i + j,
                             MIMPI_WRITE_OFFSET + MIMPI_MAX_N * i + j);
            }
        }
    }

    for (int k = 0; k < n; k++) {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);

        if (!pid) {

            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    if (i != j) {
                        if (j != k) { ASSERT_SYS_OK(close(MIMPI_READ_OFFSET + MIMPI_MAX_N * i + j)); }
                        if (i != k) { ASSERT_SYS_OK(close(MIMPI_WRITE_OFFSET + MIMPI_MAX_N * i + j)); }
                    }
                }
            }

            // Add additional info to environment.
            char k_str[4];
            int ret = snprintf(k_str, sizeof(k_str), "%d", k);
            if (ret < 0 || ret >= (int) sizeof(k_str)) {
                fatal("Error in snprintf.");
            }

            char n_str[4];
            ret = snprintf(n_str, sizeof(n_str), "%d", n);
            if (ret < 0 || ret >= (int) sizeof(n_str)) {
                fatal("Error in snprintf.");
            }

            ASSERT_SYS_OK(setenv("MIMPI_rank", k_str, true));
            ASSERT_SYS_OK(setenv("MIMPI_size", n_str, true));

            ASSERT_SYS_OK(execvp(prog, args));
        }
    }

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            if (i != j) {
                ASSERT_SYS_OK(close(MIMPI_READ_OFFSET + MIMPI_MAX_N * i + j));
                ASSERT_SYS_OK(close(MIMPI_WRITE_OFFSET + MIMPI_MAX_N * i + j));
            }
        }
    }

    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(wait(NULL));
    }
    return 0;
}