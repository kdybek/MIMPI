/**
 * This file is for declarations of  common interfaces used in both
 * MIMPI library (mimpi.c) and mimpirun program (mimpirun.c).
 * */

#ifndef MIMPI_COMMON_H
#define MIMPI_COMMON_H

#include <assert.h>
#include <stdbool.h>
#include <stdnoreturn.h>

/*
    Assert that expression doesn't evaluate to -1 (as almost every system function does in case of error).

    Use as a function, with a semicolon, like: ASSERT_SYS_OK(close(fd));
    (This is implemented with a 'do { ... } while(0)' block so that it can be used between if () and else.)
*/
#define ASSERT_SYS_OK(expr)                                                                \
    do {                                                                                   \
        if ((expr) == -1)                                                                  \
            syserr(                                                                        \
                "system command failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ", \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Assert that expression evaluates to zero (otherwise use result as error number, as in pthreads). */
#define ASSERT_ZERO(expr)                                                                  \
    do {                                                                                   \
        int const _errno = (expr);                                                         \
        if (_errno != 0)                                                                   \
            syserr(                                                                        \
                "Failed: %s\n\tIn function %s() in %s line %d.\n\tErrno: ",                \
                #expr, __func__, __FILE__, __LINE__                                        \
            );                                                                             \
    } while(0)

/* Prints with information about system error (errno) and quits. */
_Noreturn extern void syserr(const char* fmt, ...);

/* Prints (like printf) and quits. */
_Noreturn extern void fatal(const char* fmt, ...);

#define TODO fatal("UNIMPLEMENTED function %s", __PRETTY_FUNCTION__);


/////////////////////////////////////////////
// Put your declarations here

#define CHECK_IF_REMOTE_FINISHED(rank, ret)                                                \
    do {                                                                                   \
        if (ret == 0) {                                                                    \
            ASSERT_SYS_OK(close(rank + MIMPI_GROUP_R_WRITE_OFFSET));                       \
            ASSERT_SYS_OK(close(rank + MIMPI_GROUP_R_WRITE_OFFSET));                       \
            return MIMPI_ERROR_REMOTE_FINISHED;                                            \
        }                                                                                  \
    } while(0)

// Offsets:
#define MIMPI_MAIN_READ_OFFSET 20
#define MIMPI_MAIN_WRITE_OFFSET 36
#define MIMPI_SEM_READ_OFFSET 52
#define MIMPI_SEM_WRITE_OFFSET 68
#define MIMPI_QUEUE_READ_OFFSET 84
#define MIMPI_QUEUE_WRITE_OFFSET 100
#define MIMPI_GROUP_R_READ_OFFSET 116
#define MIMPI_GROUP_R_WRITE_OFFSET 132
#define MIMPI_GROUP_L_READ_OFFSET 148
#define MIMPI_GROUP_L_WRITE_OFFSET 164

// Signals:
#define MIMPI_END 0
#define MIMPI_SEND 1

// Misc:
#define MIMPI_MAX_NUMBER_OF_PROCESSES 16

#endif // MIMPI_COMMON_H