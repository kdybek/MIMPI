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
    WAITING = 1

} send_signal_t;

typedef enum {
    MESSAGE_ARRIVED = 0,
    PROCESS_ENDED = 1,
    DEADLOCK_DETECTED_BY_ONE_SIDE = 2,
    DEADLOCK_DETECTED_BY_BOTH_SIDES = 3,
    RETRY_SENDING_WAITING = 4

} recv_signal_t;

typedef struct metadata
{
    send_signal_t signal;
    int tag;
    int count;
    int num_recv;
    int num_sent;

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
static buffer_node_t* g_first_node;
static buffer_node_t* g_last_node;
static pthread_t thread[MIMPI_MAX_N];
static u_int8_t g_write_buf[MIMPI_WRITE_BUFFER_SIZE];
static volatile bool g_alive[MIMPI_MAX_N];
static volatile int g_source; // If g_source != -1, main program is waiting for a message from g_source.
static volatile int g_tag; // If g_source != -1, main program is waiting for a message with g_tag.
static volatile int g_count; // If g_source != -1, main program is waiting for a message of g_count bytes.
static volatile recv_signal_t g_recv_sig;

// Deadlock detection stuff:
static bool g_deadlock_detection;
static volatile bool g_is_waiting_on_recv[MIMPI_MAX_N];
static volatile int g_num_sent[MIMPI_MAX_N];
static volatile int g_num_recv[MIMPI_MAX_N];
static volatile int g_num_sent_to_me[MIMPI_MAX_N];

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
    buffer_node_t* itr = g_first_node;
    buffer_node_t* aux;

    while (itr != NULL) {
        aux = itr->next;
        free(itr->data);
        free(itr);
        itr = aux;
    }
}

static int minimum(int a, int b) {
    return (a < b) ? a : b;
}

// Tries to read the specified amount of bytes and no less.
// Returns false in case no write descriptor for the channel is open.
static bool thorough_read(void* read_buf, int* offset, int* fillup, void* res_buf, int count, int fd) {
    int left_in_buf = *fillup - *offset;
    int to_read = count;
    int bytes_read = 0;
    int min;
    int ret;

    while (to_read > 0) {
        if (left_in_buf == 0) {
            ret = chrecv(fd, read_buf, MIMPI_READ_BUFFER_SIZE);
            ASSERT_SYS_OK(ret);
            if (ret == 0) return false;
            *offset = 0;
            *fillup = ret;
        }

        left_in_buf = *fillup - *offset;
        min = minimum(left_in_buf, to_read);

        memcpy(res_buf + bytes_read,
               read_buf + *offset,
               min);

        bytes_read += min;
        to_read -= min;
        *offset += min;
        left_in_buf -= min;

        assert(left_in_buf >= 0);
    }
    return true;
}

static bool send_aux(size_t bytes_to_send, int fd, int dest) {
    int ret;
    size_t bytes_written = 0;

    while (bytes_written < bytes_to_send) {
        ret = chsend(fd, g_write_buf + bytes_written, bytes_to_send - bytes_written);
        if (ret == -1 && !g_alive[dest]) return false;
        ASSERT_SYS_OK(ret);
        bytes_written += ret;
    }
    return true;
}

// Tries to write the metadata and data of count bytes and no less.
// Returns false in case no read descriptor for the channel is open.
static bool thorough_write(metadata_t mt, const void* data, int count, int fd, int dest) {
    int bytes_to_copy = minimum(count, MIMPI_WRITE_BUFFER_SIZE - sizeof(metadata_t));
    int offset = 0;

    memcpy(g_write_buf, &mt, sizeof(metadata_t)); // I assume that metadata_t is small enough to fit.

    if (count > 0) { memcpy(g_write_buf + sizeof(metadata_t), data, bytes_to_copy); }

    if (!send_aux(bytes_to_copy + sizeof(metadata_t), fd, dest)) { return false; }

    count -= bytes_to_copy;
    offset += bytes_to_copy;

    while (count > 0) {
        bytes_to_copy = minimum(count, MIMPI_WRITE_BUFFER_SIZE);

        memcpy(g_write_buf, data + offset, bytes_to_copy);

        if (!send_aux(bytes_to_copy, fd, dest)) { return false; }

        count -= bytes_to_copy;
        offset += bytes_to_copy;
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

static void send_waiting(int dest, int recv, int sent) {
    if (g_deadlock_detection) {
        metadata_t mt;
        mt.signal = WAITING;
        mt.tag = 0;
        mt.count = 0;
        mt.num_recv = recv;
        mt.num_sent = sent;

        thorough_write(mt, NULL,0,
                       MIMPI_WRITE_OFFSET + MIMPI_MAX_N * MIMPI_World_rank() + dest,
                       dest);
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
    int8_t read_buff[MIMPI_READ_BUFFER_SIZE];
    int offset = 0;
    int fillup = 0;

    while (true) {
        if (!thorough_read(read_buff, &offset, &fillup,
                           &mt, sizeof(metadata_t),
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

        void* buff;

        switch(mt.signal) {
            case SEND:
                buff = malloc(mt.count);
                thorough_read(read_buff, &offset,
                              &fillup, buff, mt.count,
                              MIMPI_READ_OFFSET + MIMPI_MAX_N * src + rank);

                buffer_node_t* node = new_node(mt.tag, src, mt.count, buff);

                ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

                g_num_recv[src]++;

                g_last_node->prev->next = node;
                node->prev = g_last_node->prev;
                g_last_node->prev = node;
                node->next = g_last_node;

                if (g_source == src &&
                    tag_compare(g_tag, mt.tag) &&
                    g_count == mt.count) {
                    g_recv_sig = MESSAGE_ARRIVED;
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_on_recv));
                }
                else if (g_deadlock_detection && g_source == src) {
                    if (g_is_waiting_on_recv[src] && g_num_sent_to_me[src] == g_num_recv[src]) { // Deadlock.
                        g_recv_sig = DEADLOCK_DETECTED_BY_ONE_SIDE;
                        ASSERT_SYS_OK(pthread_mutex_unlock(&g_on_recv));
                    }
                    else if (!(g_is_waiting_on_recv[src] && g_num_sent_to_me[src] != g_num_recv[src])) {
                        g_recv_sig = RETRY_SENDING_WAITING;
                        ASSERT_SYS_OK(pthread_mutex_unlock(&g_on_recv));
                    }
                    else { ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex)); }
                }
                else { ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex)); }
                break;
            case WAITING: // For deadlock detection.
                ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

                if (g_source == src &&
                    g_num_sent[src] == mt.num_recv &&
                    g_num_recv[src] == mt.num_sent) { // There is a deadlock.

                    g_recv_sig = DEADLOCK_DETECTED_BY_BOTH_SIDES;

                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_on_recv));
                }
                else if (g_num_sent[src] == mt.num_recv) { // src got all my messages.
                    g_is_waiting_on_recv[src] = true;
                    g_num_sent_to_me[src] = mt.num_sent;

                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                }
                else { ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex)); }
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
    ASSERT_SYS_OK(pthread_mutex_lock(&g_on_recv)); // This mutex is initialized with 0.
    g_source = -1;
    g_deadlock_detection = enable_deadlock_detection;
    g_first_node = new_node(0, -1, 0, NULL);
    g_last_node = new_node(0, -1, 0, NULL);
    g_first_node->next = g_last_node;
    g_last_node->prev = g_first_node;
    for (int i = 0; i < MIMPI_World_size(); i++) {
        g_alive[i] = true;
        g_is_waiting_on_recv[i] = false;
        g_num_sent[i] = 0;
        g_num_recv[i] = 0;
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
    mt.num_recv = 0;
    mt.num_sent = 0;

    ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

    g_num_sent[destination]++;
    g_is_waiting_on_recv[destination] = false;

    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));

    bool ret = thorough_write(mt, data, count,
                             MIMPI_WRITE_OFFSET + MIMPI_MAX_N * rank + destination,
                             destination);

    if (!ret) { return MIMPI_ERROR_REMOTE_FINISHED; }
    else { return MIMPI_SUCCESS; }
}

MIMPI_Retcode MIMPI_Recv(
        void* data,
        int count,
        int source,
        int tag
) {
    int rank = MIMPI_World_rank();
    int recv;
    int sent;

    if (source == rank) return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    if (source < 0 || source >= MIMPI_World_size()) return MIMPI_ERROR_NO_SUCH_RANK;

    ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));

    if (!find_and_delete(data, count, source, tag)) {
        if (!g_alive[source]) {
            ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
            return MIMPI_ERROR_REMOTE_FINISHED;
        }

        if (g_is_waiting_on_recv[source] &&
            g_num_sent_to_me[source] == g_num_recv[source]) { // There is a deadlock.

            g_is_waiting_on_recv[source] = false; // The other process will also detect a deadlock.
            recv = g_num_recv[source];
            sent = g_num_sent[source];

            ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));

            send_waiting(source, recv, sent);
            return MIMPI_ERROR_DEADLOCK_DETECTED;
        }

        // Give the helpers info about what to look for.
        g_source = source;
        g_tag = tag;
        g_count = count;

        recv = g_num_recv[source];
        sent = g_num_sent[source];

        ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));

        send_waiting(source, recv, sent);

        while (true) {
            ASSERT_SYS_OK(pthread_mutex_lock(&g_on_recv));
            // Critical section inheritance

            switch(g_recv_sig) {
                case MESSAGE_ARRIVED:
                    g_source = -1;
                    memcpy(data, g_last_node->prev->data, count);
                    del_node(g_last_node->prev);
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                    return MIMPI_SUCCESS;
                case PROCESS_ENDED:
                    g_source = -1;
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                    return MIMPI_ERROR_REMOTE_FINISHED;
                case DEADLOCK_DETECTED_BY_BOTH_SIDES:
                    g_source = -1;
                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
                    return MIMPI_ERROR_DEADLOCK_DETECTED;
                case DEADLOCK_DETECTED_BY_ONE_SIDE:
                    recv = g_num_recv[source];
                    sent = g_num_sent[source];

                    g_source = -1;
                    g_is_waiting_on_recv[source] = false;

                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));

                    send_waiting(source, recv, sent);
                    return MIMPI_ERROR_DEADLOCK_DETECTED;
                case RETRY_SENDING_WAITING:
                    recv = g_num_recv[source];
                    sent = g_num_sent[source];

                    ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));

                    send_waiting(source, recv, sent);
                    break;
            }
        }
    }
    else {
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
