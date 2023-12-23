#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>

#define MAX_PATH_LENGTH 1024

void print_open_descriptors(void)
{
    const char* path = "/proc/self/fd";

    // Iterate over all symlinks in `path`.
    // They represent open file descriptors of our process.
    DIR* dr = opendir(path);
    if (dr == NULL)
        fatal("Could not open dir: %s", path);

    struct dirent* entry;
    while ((entry = readdir(dr)) != NULL) {
        if (entry->d_type != DT_LNK)
            continue;

        // Make a c-string with the full path of the entry.
        char subpath[MAX_PATH_LENGTH];
        int ret = snprintf(subpath, sizeof(subpath), "%s/%s", path, entry->d_name);
        if (ret < 0 || ret >= (int)sizeof(subpath))
            fatal("Error in snprintf");

        // Read what the symlink points to.
        char symlink_target[MAX_PATH_LENGTH];
        ssize_t ret2 = readlink(subpath, symlink_target, sizeof(symlink_target) - 1);
        ASSERT_SYS_OK(ret2);
        symlink_target[ret2] = '\0';

        // Skip an additional open descriptor to `path` that we have until closedir().
        if (strncmp(symlink_target, "/proc", 5) == 0)
            continue;

        fprintf(stderr, "Pid %d file descriptor %3s -> %s\n",
                getpid(), entry->d_name, symlink_target);
    }

    closedir(dr);
}

int main() {
    MIMPI_Init(false);

    if (MIMPI_World_rank() == 0) {
        int buf[1];
        buf[0] = 8;
        printf("sending\n");
        ASSERT_SYS_OK(chsend(MIMPI_MAIN_WRITE_OFFSET + 2, buf, sizeof(int)));
        buf[0] = 420;
        ASSERT_SYS_OK(chsend(MIMPI_MAIN_WRITE_OFFSET + 2, buf, sizeof(int)));
    }
    if (MIMPI_World_rank() == 2) {
        int buf1[1];
        buf1[0] = 69;
        printf("%d\n", buf1[0]);
        printf("receiving\n");
        ASSERT_SYS_OK(chrecv(MIMPI_MAIN_READ_OFFSET + 2, buf1, sizeof(int)));
        printf("%d\n", buf1[0]);
        ASSERT_SYS_OK(chrecv(MIMPI_MAIN_READ_OFFSET + 2, buf1, sizeof(int)));
        printf("%d\n", buf1[0]);
    }
    return 0;
}