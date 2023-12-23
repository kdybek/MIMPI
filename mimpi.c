/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <pthread.h>

typedef struct signal
{
    int sender;
    int type;
    int tag;
    int count;

} signal_t;

typedef struct buffer_node
{
    int tag;
    int sender;
    int count;
    void* data;
    struct buffer_node* next;

} buffer_node_t;

typedef struct buffer_data
{
    pthread_mutex_t mutex;
    pthread_mutex_t main_prog;
    buffer_node_t* first_node;
    buffer_node_t* last_node;
    bool alive[MAX_NUMBER_OF_PROCESSES];
    bool main_waiting;
    int w_tag;
    int w_sender;
    int w_count;

} buffer_data_t;

static bool tag_compare(int t1, int t2) {
    return t1 == MIMPI_ANY_TAG || t1 == t2;
}

static buffer_node_t* new_node(int tag, int sender, int count) {
    buffer_node_t* new_n = malloc(sizeof(buffer_node_t));
    new_n->tag = tag;
    new_n->sender = sender;
    new_n->count = count;
    new_n->data = NULL;
    new_n->next = NULL;
    return new_n;
}

static void cleanup(buffer_data_t* bf) {
    ASSERT_SYS_OK(pthread_mutex_destroy(&bf->mutex));
    ASSERT_SYS_OK(pthread_mutex_destroy(&bf->main_prog));
    buffer_node_t* itr = bf->first_node;
    buffer_node_t* aux;

    while (itr != NULL) {
        aux = itr->next;
        free(itr->data);
        free(itr);
        itr = aux;
    }
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

        switch (signal.type) {
            case MIMPI_END:
                ASSERT_SYS_OK(pthread_mutex_lock(&bf->mutex));
                bf->alive[signal.sender] = false;
                if (signal.sender == rank) {
                    end = true;
                    if (bf->main_waiting && bf->w_sender == signal.sender) {
                        ASSERT_SYS_OK(pthread_mutex_unlock(&bf->main_prog));
                    } else {
                        ASSERT_SYS_OK(pthread_mutex_unlock(&bf->mutex));
                    }
                } else {
                    ASSERT_SYS_OK(pthread_mutex_unlock(&bf->mutex));
                }

                // Hard to optimize as I don't have a guarantee that the process
                // on the receiving end will still be alive when the message reaches it.
                for (int i = 0; i < MIMPI_World_size(); i++) {
                    if (bf->alive[i]) {
                        chsend(i + MIMPI_QUEUE_WRITE_OFFSET,
                               &signal,
                               sizeof(signal));
                    }
                }
                break;
            case MIMPI_SEND:
                // Let the sender write his message to the main channel.
                chsend(signal.sender + MIMPI_SEM_WRITE_OFFSET, NULL, sizeof(NULL));

                void* buff = malloc(signal.count);
                chrecv(rank + MIMPI_MAIN_READ_OFFSET, buff, signal.count);

                buffer_node_t* node = new_node(signal.tag, signal.sender, signal.count);
                node->data = buff;

                ASSERT_SYS_OK(pthread_mutex_lock(&bf->mutex));
                bf->last_node->next = node;
                bf->last_node = node;
                if (bf->main_waiting &&
                    tag_compare(bf->w_tag, signal.tag) &&
                    bf->w_sender == signal.sender &&
                    bf->w_count == signal.count) {
                    ASSERT_SYS_OK(pthread_mutex_unlock(&bf->main_prog));
                } else {
                    ASSERT_SYS_OK(pthread_mutex_unlock(&bf->mutex));
                }
                break;
        }
    }

    cleanup(bf);
    channels_finalize();
    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    buffer_data_t bf;
    ASSERT_ZERO(pthread_mutex_init(&bf.mutex, NULL));
    ASSERT_ZERO(pthread_mutex_init(&bf.main_prog, NULL));
    bf.main_waiting = false;
    bf.first_node = new_node(0, -1, 0);
    bf.last_node = bf.first_node;
    for (int i = 0; i < MIMPI_World_size(); i++) {
        bf.alive[i] = true;
    }

    // Create thread attributes.
    pthread_attr_t attr;
    ASSERT_ZERO(pthread_attr_init(&attr));
    ASSERT_ZERO(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED));

    pthread_t thread;
    ASSERT_ZERO(pthread_create(&thread, &attr, Helper_main, &bf));

    ASSERT_ZERO(pthread_attr_destroy(&attr));
}

void MIMPI_Finalize() {
    int rank = MIMPI_World_rank();
    signal_t sig;
    sig.sender = MIMPI_World_rank();
    sig.type = MIMPI_END;
    chsend(rank + MIMPI_QUEUE_WRITE_OFFSET, &sig, sizeof(sig));
    // Helper will take care of the cleanup.
}

int MIMPI_World_size() {
    return atoi(getenv("MIMPI_size"));
}

int MIMPI_World_rank() {
    return atoi(getenv("MIMPI_rank"));
}

MIMPI_Retcode MIMPI_Send(
        void const* data,
        int count,
        int destination,
        int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Recv(
        void* data,
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
        void* data,
        int count,
        int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
        void const* send_data,
        void* recv_data,
        int count,
        MIMPI_Op op,
        int root
) {
    TODO
}