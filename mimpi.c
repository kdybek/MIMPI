/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

/* Structs */
typedef enum {
    SEND = 0,
    WAITING = 1,
    RECEIVED = 2

} send_signal_t;

typedef enum {
    MESSAGE_ARRIVED = 0,
    PROCESS_ENDED = 1,
    DEADLOCK_DETECTED = 2

} recv_signal_t;

typedef struct metadata
{
    send_signal_t signal;
    int tag;
    int count;

} metadata_t;

typedef struct buffer_node
{
    int tag;
    int sender;
    int count;
    void* data;
    struct buffer_node* volatile next;
    struct buffer_node* volatile prev;

} buffer_node_t;

/* Global Data */
static pthread_mutex_t g_mutex;
static pthread_mutex_t g_on_recv;
static pthread_mutex_t g_write;
static buffer_node_t* g_first_node;
static buffer_node_t* g_last_node;
static pthread_t thread[MIMPI_MAX_N];
static volatile bool g_alive[MIMPI_MAX_N];
static volatile int g_source; // If g_source != -1, main program is waiting for a message from g_source.
static volatile int g_tag; // If g_source != -1, main program is waiting for a message with g_tag.
static volatile int g_count; // If g_source != -1, main program is waiting for a message of g_count bytes.
static volatile recv_signal_t g_recv_sig;
static bool g_deadlock_detection;
static volatile int g_num_of_unreceived_messges[MIMPI_MAX_N]; // Used during deadlock detection only.
static volatile bool g_is_waiting_on_recv[MIMPI_MAX_N];

/* Auxiliary Functions */
static bool tag_compare(int t1, int t2) {
    return (t1 == MIMPI_ANY_TAG && t2 > 0) || t1 == t2;
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
    ASSERT_SYS_OK(pthread_mutex_destroy(&g_on_recv));
    ASSERT_SYS_OK(pthread_mutex_destroy(&g_write));
    buffer_node_t* itr = g_first_node;
    buffer_node_t* aux;

    while (itr != NULL) {
        aux = itr->next;
        free(itr->data);
        free(itr);
        itr = aux;
    }
}

// Tries to read the specified amount of bytes and no less.
// Returns false in case no write descriptor for the channel is open.
static bool thorough_read(void* buffer, size_t count, int fd) {
    int bytes_read = 0;
    int ret;

    while (bytes_read < count) {
        ret = chrecv(fd, buffer + bytes_read, count - bytes_read);
        ASSERT_SYS_OK(ret);
        if (ret == 0) return false;
        bytes_read += ret;
    }
    return true;
}

// Tries to write the specified amount of bytes and no less.
// Returns false in case no read descriptor for the channel is open.
static bool thorough_write(void* buffer, size_t count, int fd, int dest) {
    int bytes_read = 0;
    int ret;

    while (bytes_read < count) {
        ret = chsend(fd, buffer + bytes_read, count - bytes_read);
        if (ret == -1 && !g_alive[dest]) return false;
        ASSERT_SYS_OK(ret);
        bytes_read += ret;
    }
    return true;
}

// Never to be performed on g_first_node or g_last_node!!!
static void del_node(buffer_node_t* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
    free(node->data);
    free(node);
}

static inline int left_child(int rank) { return rank * 2 + 1; }

static inline int right_child(int rank) { return left_child(rank) + 1; }

static inline bool has_left_child(int rank) {
    int size = MIMPI_World_size();

    return left_child(rank) < size;
}

static inline bool has_right_child(int rank) {
    int size = MIMPI_World_size();

    return right_child(rank) < size;
}

static inline int parent(int rank) {
    return (rank - 1) / 2;
}

static inline bool is_root(int rank) {
    return rank == 0;
}

static int rank_adjust(int og_rank, int root) {
    if (og_rank == root) return 0;
    else if (og_rank == 0) return root;
    else return og_rank;
}

static void reduction(u_int8_t* first, const u_int8_t* second, int size, MIMPI_Op op) {
    for (int i = 0; i < size; i++) {
        switch (op) {
            case MIMPI_MAX:
                if(first[i] < second[i]) first[i] = second[i];
                break;
            case MIMPI_MIN:
                if(first[i] > second[i]) first[i] = second[i];
                break;
            case MIMPI_SUM:
                first[i] = first[i] + second[i];
                break;
            case MIMPI_PROD:
                first[i] = first[i] * second[i];
                break;
        }
    }
}

static void send_waiting(int dest, int tag, int count) {
    if (g_deadlock_detection) {
        metadata_t mt;
        mt.signal = WAITING;
        mt.tag = tag;
        mt.count = count;

        ASSERT_SYS_OK(pthread_mutex_lock(&g_write));

        thorough_write(&mt,
                       sizeof(metadata_t),
                       MIMPI_WRITE_OFFSET + MIMPI_MAX_N * MIMPI_World_rank() + dest,
                       dest);
        ASSERT_SYS_OK(pthread_mutex_unlock(&g_write));
    }
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
            memcpy(data, itr->data, count);
            del_node(itr);
            return true;
        }
        itr = itr->next;
    }
    return false;
}

static void* helper_main(void* data) {
    int* dummy = data;
    const int src = *dummy;
    free(dummy);
    const int rank = MIMPI_World_rank();
    metadata_t mt;
    int on_recv_tag;
    int on_recv_count;

    while (true) {
        if (!thorough_read(&mt,
                           sizeof(metadata_t),
                           MIMPI_READ_OFFSET + MIMPI_MAX_N * src + rank)) {

            ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

            g_alive[src] = false;
            ASSERT_SYS_OK(close(MIMPI_READ_OFFSET + MIMPI_MAX_N * src + rank));
            if (g_alive[rank]) { ASSERT_SYS_OK(close(MIMPI_WRITE_OFFSET + MIMPI_MAX_N * rank + src)); }
            if (g_source == src) {
                g_recv_sig = PROCESS_ENDED;
                ASSERT_SYS_OK(pthread_mutex_unlock(&g_on_recv));
            } else { ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex)); }
            break;
        }

        void* buf;

        switch(mt.signal) {
            case SEND:
                buf = malloc(mt.count);
                thorough_read(buf,
                              mt.count,
                              MIMPI_READ_OFFSET + MIMPI_MAX_N * src + rank);

                if (g_deadlock_detection) {
                    mt.signal = RECEIVED;
                    ASSERT_SYS_OK(pthread_mutex_lock(&g_write));

                    thorough_write(&mt,
                                   sizeof(metadata_t),
                                   MIMPI_WRITE_OFFSET + MIMPI_MAX_N * rank + src,
                                   src);
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_write));
                }

                buffer_node_t* node = new_node(mt.tag, src, mt.count, buf);

                ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

                g_last_node->prev->next = node;
                node->prev = g_last_node->prev;
                g_last_node->prev = node;
                node->next = g_last_node;

                if (g_source == src &&
                    tag_compare(g_tag, mt.tag) &&
                    g_count == mt.count) {
                    g_recv_sig = MESSAGE_ARRIVED;
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_on_recv));
                } else { ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex)); }
                break;
            case WAITING: // For deadlock detection.
                ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

                if (g_source == src &&
                    g_num_of_unreceived_messges[src] == 0) { // There is a deadlock.

                    g_recv_sig = DEADLOCK_DETECTED;
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_on_recv));
                } else {
                    g_is_waiting_on_recv[src] = true;
                    on_recv_tag = mt.tag;
                    on_recv_count = mt.count;
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                }
                break;
            case RECEIVED: // For deadlock detection.
                ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

                g_num_of_unreceived_messges[src]--;

                if (g_is_waiting_on_recv[src] &&
                    tag_compare(on_recv_tag, mt.tag) &&
                    on_recv_count == mt.count) {
                    g_is_waiting_on_recv[src] = false;
                }

                if (g_is_waiting_on_recv[src] &&
                    g_source == src &&
                    g_num_of_unreceived_messges[src] == 0) { // There is a deadlock.

                    g_is_waiting_on_recv[src] = false;
                    g_recv_sig = DEADLOCK_DETECTED;
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_on_recv));
                } else { ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex)); }
                break;
        }
    }
    return NULL;
}

/* Library Function */
void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    ASSERT_ZERO(pthread_mutex_init(&g_mutex, NULL));
    ASSERT_ZERO(pthread_mutex_init(&g_on_recv, NULL));
    ASSERT_ZERO(pthread_mutex_init(&g_write, NULL));
    ASSERT_SYS_OK(pthread_mutex_lock(&g_on_recv)); // This mutex is initialized with 0.
    g_source = -1;
    g_deadlock_detection = enable_deadlock_detection;
    g_first_node = new_node(0, -1, 0, NULL);
    g_last_node = new_node(0, -1, 0, NULL);
    g_first_node->next = g_last_node;
    g_last_node->prev = g_first_node;
    for (int i = 0; i < MIMPI_World_size(); i++) {
        g_alive[i] = true;
        g_num_of_unreceived_messges[i] = 0;
    }

    for (int i = 0; i < MIMPI_World_size(); i++) {
        if (i != MIMPI_World_rank()) {
            int* thread_data = malloc(sizeof(int));
            *thread_data = i;
            ASSERT_ZERO(pthread_create(&thread[i], NULL, helper_main, thread_data));
        }
    }
}

void MIMPI_Finalize() {
    ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

    g_alive[MIMPI_World_rank()] = false;
    for (int i = 0; i < MIMPI_World_size(); i++) {
        if (g_alive[i]) {
            ASSERT_SYS_OK(close(MIMPI_WRITE_OFFSET + MIMPI_MAX_N * MIMPI_World_rank() + i));
        }
    }
    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));

    for (int i = 0; i < MIMPI_World_size(); i++) {
        if (i != MIMPI_World_rank()) {
            ASSERT_ZERO(pthread_join(thread[i], NULL));
        }
    }

    cleanup();
    channels_finalize();
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

    metadata_t mt;
    mt.signal = SEND;
    mt.tag = tag;
    mt.count = count;

    void* buf = malloc(count + sizeof(metadata_t));
    memcpy(buf, &mt, sizeof(metadata_t));
    memcpy(buf + sizeof(metadata_t), data, count);

    if (g_deadlock_detection) {
        ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

        g_num_of_unreceived_messges[destination]++;
        ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
    }

    ASSERT_SYS_OK(pthread_mutex_lock(&g_write));

    bool ret = thorough_write(buf,
                             count + sizeof(metadata_t),
                             MIMPI_WRITE_OFFSET + MIMPI_MAX_N * rank + destination,
                             destination);
    ASSERT_SYS_OK(pthread_mutex_unlock(&g_write));

    if (!ret) {
        free(buf);
        return MIMPI_ERROR_REMOTE_FINISHED;
    } else {
        free(buf);
        return MIMPI_SUCCESS;
    }
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
        if (!g_alive[source]) {
            ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        if (g_is_waiting_on_recv[source] &&
            g_num_of_unreceived_messges[source] == 0) { // There is a deadlock.

            g_is_waiting_on_recv[source] = false;
            ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
            send_waiting(source, tag, count);
            return MIMPI_ERROR_DEADLOCK_DETECTED;
        }

        // Give the helpers info about what to look for.
        g_source = source;
        g_tag = tag;
        g_count = count;

        ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));

        send_waiting(source, tag, count);
        ASSERT_SYS_OK(pthread_mutex_lock(&g_on_recv));
        // Critical section inheritance

        g_source = -1;

        switch(g_recv_sig) {
            case MESSAGE_ARRIVED:
                memcpy(data, g_last_node->prev->data, count);
                del_node(g_last_node->prev);
                ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                return MIMPI_SUCCESS;
            case PROCESS_ENDED:
                ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                return MIMPI_ERROR_REMOTE_FINISHED;
            case DEADLOCK_DETECTED:
                ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                return MIMPI_ERROR_DEADLOCK_DETECTED;
        }
        // Unreachable code, but there is a return to avoid warnings.
        assert(1 == 0);
    } else {
        ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
        return MIMPI_SUCCESS;
    }
}

MIMPI_Retcode MIMPI_Barrier() {
    MIMPI_Retcode ret;
    int rank = MIMPI_World_rank();
    int l = left_child(rank);
    int r = right_child(rank);
    // If rank is the root of the tree, there will be garbage in p, but it won't be used.
    int p = parent(rank);
    char* dummy = malloc(sizeof(char));

    if (has_left_child(rank)) {
        ret = MIMPI_Recv(dummy, sizeof(char), l, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    if (has_right_child(rank)) {
        ret = MIMPI_Recv(dummy, sizeof(char), r, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    if (!is_root(rank)) {
        ret = MIMPI_Send(dummy, sizeof(char), p, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);

        ret = MIMPI_Recv(dummy, sizeof(char), p, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    if (has_left_child(rank)) {
        ret = MIMPI_Send(dummy, sizeof(char), l, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    if (has_right_child(rank)) {
        ret = MIMPI_Send(dummy, sizeof(char), r, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    free(dummy);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
        void* data,
        int count,
        int root
) {
    int ret;
    int rank = rank_adjust(MIMPI_World_rank(), root);
    int l = rank_adjust(left_child(rank), root);
    int r = rank_adjust(right_child(rank), root);
    // If rank is the root of the tree, there will be garbage in p, but it won't be used.
    int p = rank_adjust(parent(rank), root);
    char* dummy = malloc(sizeof(char));

    if (has_left_child(rank)) {
        ret = MIMPI_Recv(dummy, sizeof(char), l, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    if (has_right_child(rank)) {
        ret = MIMPI_Recv(dummy, sizeof(char), r, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    if (!is_root(rank)) {
        ret = MIMPI_Send(dummy, sizeof(char), p, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);

        ret = MIMPI_Recv(data, count, p, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    if (has_left_child(rank)) {
        ret = MIMPI_Send(data, count, l, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    if (has_right_child(rank)) {
        ret = MIMPI_Send(data, count, r, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, NULL, NULL);
    }

    free(dummy);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Reduce(
        void const* send_data,
        void* recv_data,
        int count,
        MIMPI_Op op,
        int root
) {
    int ret;
    int rank = rank_adjust(MIMPI_World_rank(), root);
    int l = rank_adjust(left_child(rank), root);
    int r = rank_adjust(right_child(rank), root);
    // If rank is the root of the tree, there will be garbage in p, but it won't be used.
    int p = rank_adjust(parent(rank), root);
    char* dummy = malloc(sizeof(char));
    u_int8_t* res = malloc(count);
    u_int8_t* buf = malloc(count);
    memcpy(res, send_data, count);

    if (has_left_child(rank)) {
        ret = MIMPI_Recv(buf, count, l, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, res, buf);
        reduction(res, buf, count, op);
    }

    if (has_right_child(rank)) {
        ret = MIMPI_Recv(buf, count, r, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, res, buf);
        reduction(res, buf, count, op);
    }

    if (!is_root(rank)) {
        ret = MIMPI_Send(res, count, p, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, res, buf);

        ret = MIMPI_Recv(dummy, sizeof(char), p, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, res, buf);
    } else {
        memcpy(recv_data, res, count);
    }

    if (has_left_child(rank)) {
        ret = MIMPI_Send(dummy, sizeof(char), l, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, res, buf);
    }

    if (has_right_child(rank)) {
        ret = MIMPI_Send(dummy, sizeof(char), r, -1);
        CHECK_IF_REMOTE_FINISHED(ret, dummy, res, buf);
    }

    free(dummy);
    free(res);
    free(buf);
    return MIMPI_SUCCESS;
}
