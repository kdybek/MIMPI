/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <pthread.h>

typedef struct signal {
    int sender;
    int type;
    int tag;
    int count;
} signal_t;

typedef struct buffer_node {
    int tag;
    int sender;
    int count;
    void* data;
    struct buffer_node* next;
} buffer_node_t;

typedef struct buffer_data {
    int rank;
    pthread_mutex_t mutex;
    pthread_mutex_t main_prog;
    buffer_node_t* first_node;
    buffer_node_t* last_node;
    bool main_waiting;
    int w_tag;
    int w_sender;
    int w_count;
} buffer_data_t;

static buffer_node_t* New_node(int tag, int sender, int count) {
    buffer_node_t* new_n = malloc(sizeof(buffer_node_t));
    new_n->tag = tag;
    new_n->sender = sender;
    new_n->count = count;
    new_n->data = NULL;
    new_n->next = NULL;
    return new_n;
}

static void* Helper_main(void* data) {
    buffer_data_t* bf = data;
    signal_t signal;
    int rank = MIMPI_World_rank();
    bool end = false;

    while (!end) {
        chrecv(rank + MIMPI_QUEUE_READ_OFFSET,
               &signal,
               sizeof(signal));

        switch(signal.type) {
            case MIMPI_END:
                if (signal.sender == rank) { end = true; }
                else {

                }
                break;
        }
    }
    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    buffer_data_t bf;
    ASSERT_ZERO(pthread_mutex_init(&bf.mutex, NULL));
    ASSERT_ZERO(pthread_mutex_init(&bf.main_prog, NULL));
    bf.main_waiting = false;
    bf.first_node = New_node(0, -1, 0);
    bf.last_node = bf.first_node;

    // Create thread attributes.
    pthread_attr_t attr;
    ASSERT_ZERO(pthread_attr_init(&attr));
    ASSERT_ZERO(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED));

    pthread_t thread;
    ASSERT_ZERO(pthread_create(&thread, &attr, Helper_main, &bf));

    ASSERT_ZERO(pthread_attr_destroy(&attr));
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