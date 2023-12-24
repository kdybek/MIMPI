/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <pthread.h>

/* Structs */
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
    struct buffer_node* prev;

} buffer_node_t;

/* Global Data */
static pthread_mutex_t g_mutex;
static pthread_mutex_t g_main_prog;
static buffer_node_t* g_first_node;
static buffer_node_t* g_last_node;
static volatile bool g_alive[MIMPI_MAX_NUMBER_OF_PROCESSES];
static volatile bool g_main_waiting;
static volatile int g_tag;
static volatile int g_sender;
static volatile int g_count;

/* Auxiliary Functions */
static bool tag_compare(int t1, int t2) {
    return t1 == MIMPI_ANY_TAG || t1 == t2;
}

static buffer_node_t* new_node(int tag, int sender, int count, void* data) {
    buffer_node_t* new_n = malloc(sizeof(buffer_node_t));
    new_n->tag = tag;
    new_n->sender = sender;
    new_n->count = count;
    new_n->data = data;
    new_n->next = NULL;
    new_n->prev = NULL;
    return new_n;
}

static void cleanup() {
    ASSERT_SYS_OK(pthread_mutex_destroy(&g_mutex));
    ASSERT_SYS_OK(pthread_mutex_destroy(&g_main_prog));
    buffer_node_t* itr = g_first_node;
    buffer_node_t* aux;

    while (itr != NULL) {
        aux = itr->next;
        free(itr->data);
        free(itr);
        itr = aux;
    }
}

// Never to be performed on g_first_node or g_last_node!!!
static void del_node(buffer_node_t* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
    free(node);
}

// Never to be performed outside a mutex!!!
static bool find_and_delete(
        void* data,
        int count,
        int source,
        int tag
) {
    buffer_node_t* itr = g_first_node;
    itr = itr->next;
    while (itr != g_last_node) {
        if (tag_compare(tag, itr->tag) &&
        count == itr->count &&
        source == itr->sender) {
            data = itr->data;
            del_node(itr);
            return true;
        }
        itr = itr->next;
    }
    return false;
}

static void* Helper_main() {
    signal_t signal;
    int rank = MIMPI_World_rank();
    bool end = false;
    char* dummy = NULL;

    while (!end) {
        chrecv(rank + MIMPI_QUEUE_READ_OFFSET,
               &signal,
               sizeof(signal));

        switch (signal.type) {
            case MIMPI_END:
                ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));
                g_alive[signal.sender] = false;
                if (signal.sender == rank) {
                    end = true;
                    if (g_main_waiting && g_sender == signal.sender) {
                        ASSERT_SYS_OK(pthread_mutex_unlock(&g_main_prog));
                    } else {
                        ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                    }
                } else {
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                }

                // Hard to optimize as I don't have a guarantee that the process
                // on the receiving end will still be g_alive when the message reaches it.
                for (int i = 0; i < MIMPI_World_size(); i++) {
                    if (g_alive[i]) {
                        chsend(i + MIMPI_QUEUE_WRITE_OFFSET,
                               &signal,
                               sizeof(signal));
                    }
                }
                break;
            case MIMPI_SEND:
                // Let the sender write his message to the main channel.
                chsend(signal.sender + MIMPI_SEM_WRITE_OFFSET, dummy, sizeof(char));

                void* buff = malloc(signal.count);
                chrecv(rank + MIMPI_MAIN_READ_OFFSET, buff, signal.count);

                buffer_node_t* node = new_node(signal.tag, signal.sender, signal.count, buff);

                ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));
                g_last_node->prev->next = node;
                node->prev = g_last_node->prev;
                g_last_node->prev = node;
                node->next = g_last_node;

                if (g_main_waiting &&
                    tag_compare(g_tag, signal.tag) &&
                    g_sender == signal.sender &&
                    g_count == signal.count) {
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_main_prog));
                } else {
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                }
                break;
        }
    }

    cleanup();
    channels_finalize();
    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    ASSERT_ZERO(pthread_mutex_init(&g_mutex, NULL));
    ASSERT_ZERO(pthread_mutex_init(&g_main_prog, NULL));
    g_main_waiting = false;
    g_first_node = new_node(0, -1, 0, NULL);
    g_last_node = new_node(0, -1, 0, NULL);
    g_first_node->next = g_last_node;
    g_last_node->prev = g_first_node;
    for (int i = 0; i < MIMPI_World_size(); i++) {
        g_alive[i] = true;
    }

    // Create thread attributes.
    pthread_attr_t attr;
    ASSERT_ZERO(pthread_attr_init(&attr));
    ASSERT_ZERO(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED));

    pthread_t thread;
    ASSERT_ZERO(pthread_create(&thread, &attr, Helper_main, NULL));

    ASSERT_ZERO(pthread_attr_destroy(&attr));
}

void MIMPI_Finalize() {
    int rank = MIMPI_World_rank();
    signal_t sig;
    sig.sender = rank;
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
    int rank = MIMPI_World_rank();
    if (destination == rank) return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    if (destination < 0 || destination >= MIMPI_World_size()) return MIMPI_ERROR_NO_SUCH_RANK;
    if (!g_alive[destination]) return MIMPI_ERROR_REMOTE_FINISHED;
    char* dummy = NULL;
    signal_t sig;
    sig.sender = rank;
    sig.tag = tag;
    sig.count = count;
    sig.type = MIMPI_SEND;

    ASSERT_SYS_OK(chsend(destination + MIMPI_QUEUE_WRITE_OFFSET, &sig, sizeof(sig)));
    ASSERT_SYS_OK(chrecv(rank + MIMPI_SEM_READ_OFFSET, dummy, sizeof(char)));
    ASSERT_SYS_OK(chsend(destination + MIMPI_MAIN_WRITE_OFFSET, data, count));
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
        void* data,
        int count,
        int source,
        int tag
) {
    int rank = MIMPI_World_rank();
    if (source == rank) return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    if (source < 0 || source >= MIMPI_World_size()) return MIMPI_ERROR_NO_SUCH_RANK;

    ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));
    if (!find_and_delete(data, count, source, tag)) {
        if (!g_alive[source]) return MIMPI_ERROR_REMOTE_FINISHED;

        // Give the helper info about what it's looking for.
        g_main_waiting = true;
        g_sender = source;
        g_tag = tag;
        g_count = count;

        ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
        ASSERT_SYS_OK(pthread_mutex_lock(&g_main_prog));
        // Critical section inheritance

        g_main_waiting = false;
        buffer_node_t* candidate = g_last_node->prev;

        if (tag_compare(tag, candidate->tag) &&
        source == candidate->sender &&
        count == candidate->count) {
            data = candidate->data;
            del_node(candidate);
            ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
            return MIMPI_SUCCESS;
        } else { // main was woken up, because source left the MIMPI block.
            ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    } else {
        ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
        return MIMPI_SUCCESS;
    }
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