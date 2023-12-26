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
typedef struct metadata
{
    int sender;
    int tag;
    int count;

} metadata_t;

typedef struct signal
{
    metadata_t metadata;
    const void* data;

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

typedef struct send_info
{
    void* data;
    int n_bytes;
    int destination;

} send_info_t;

/* Global Data */
static pthread_mutex_t g_mutex;
static pthread_mutex_t g_main_prog;
static buffer_node_t* g_first_node;
static buffer_node_t* g_last_node;
static pthread_t thread[MIMPI_MAX_N];
static volatile bool g_alive[MIMPI_MAX_N];
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

static inline bool is_left_child(int rank) {
    return rank % 2 == 1;
}

static inline bool is_right_child(int rank) {
    return !is_left_child(rank);
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
                first[i] = first[i] > second[i] ? first[i] : second[i];
                break;
            case MIMPI_MIN:
                first[i] = first[i] < second[i] ? first[i] : second[i];
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
    int ret;
    metadata_t mt;

    while (true) {
        ret = chrecv(MIMPI_READ_OFFSET + MIMPI_MAX_N * src + rank, &mt, sizeof(metadata_t));
        ASSERT_SYS_OK(ret);
        if (ret == 0) {
            ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));
            g_alive[src] = false;
            ASSERT_SYS_OK(close(MIMPI_READ_OFFSET + MIMPI_MAX_N * src + rank));
            if (g_alive[rank]) { ASSERT_SYS_OK(close(MIMPI_WRITE_OFFSET + MIMPI_MAX_N * rank + src)); }
            if (g_main_waiting) { ASSERT_SYS_OK(pthread_mutex_unlock(&g_main_prog)); }
            else { ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex)); }
            break;
        }

        void* buf = malloc(mt.count);
        ASSERT_SYS_OK(chrecv(MIMPI_READ_OFFSET + MIMPI_MAX_N * src + rank, buf, mt.count));

        buffer_node_t* node = new_node(mt.tag, mt.sender, mt.count, buf);

        ASSERT_SYS_OK(pthread_mutex_lock(&g_mutex));
        g_last_node->prev->next = node;
        node->prev = g_last_node->prev;
        g_last_node->prev = node;
        node->next = g_last_node;

        if (g_main_waiting &&
            tag_compare(g_tag, mt.tag) &&
            g_sender == mt.sender &&
            g_count == mt.count) {
            ASSERT_SYS_OK(pthread_mutex_unlock(&g_main_prog));
        } else {
            ASSERT_SYS_OK(pthread_mutex_unlock(&g_mutex));
        }
    }
    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    ASSERT_ZERO(pthread_mutex_init(&g_mutex, NULL));
    ASSERT_ZERO(pthread_mutex_init(&g_main_prog, NULL));
    ASSERT_SYS_OK(pthread_mutex_lock(&g_main_prog)); // I want this mutex initialized with 0.
    g_main_waiting = false;
    g_first_node = new_node(0, -1, 0, NULL);
    g_last_node = new_node(0, -1, 0, NULL);
    g_first_node->next = g_last_node;
    g_last_node->prev = g_first_node;
    for (int i = 0; i < MIMPI_World_size(); i++) {
        g_alive[i] = true;
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
    mt.sender = rank;
    mt.tag = tag;
    mt.count = count;
    void* buf = malloc(count + sizeof(metadata_t));
    memcpy(buf, &mt, sizeof(metadata_t));
    memcpy(buf + sizeof(metadata_t), data, count);

    ASSERT_SYS_OK(chsend(MIMPI_WRITE_OFFSET + MIMPI_MAX_N * rank + destination, buf, count + sizeof(metadata_t)));
    free(buf);
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

        // Give the helper info about what to look for.
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
            memcpy(data, candidate->data, count);
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

/*
MIMPI_Retcode MIMPI_Barrier() {
    int ret;
    int rank = MIMPI_World_rank();
    int l = left_child(rank);
    int r = right_child(rank);
    // If rank is the root of the tree, there will be garbage in p, but it won't be used.
    int p = parent(rank);
    char* dummy = malloc(sizeof(char));

    if (has_left_child(rank)) {
        // Parent reads from group r channel.
        ret = chrecv(l + MIMPI_GROUP_R_READ_OFFSET, dummy, sizeof(char));
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
    }

    if (has_right_child(rank)) {
        ret = chrecv(r + MIMPI_GROUP_R_READ_OFFSET, dummy, sizeof(char));
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
    }

    if (!is_root(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_R_WRITE_OFFSET, dummy, sizeof(char)));

        int offset;
        if (is_left_child(rank)) offset = MIMPI_GROUP_L_READ_OFFSET;
        else offset = MIMPI_GROUP_R_READ_OFFSET;

        ret = chrecv(p + offset, dummy, sizeof(char));
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
    }

    if (has_left_child(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_L_WRITE_OFFSET, dummy, sizeof(char)));
    }

    if (has_right_child(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_R_WRITE_OFFSET, dummy, sizeof(char)));
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
        ret = chrecv(l + MIMPI_GROUP_R_READ_OFFSET, dummy, sizeof(char));
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
    }

    if (has_right_child(rank)) {
        ret = chrecv(r + MIMPI_GROUP_R_READ_OFFSET, dummy, sizeof(char));
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
    }

    if (!is_root(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_R_WRITE_OFFSET, dummy, sizeof(char)));

        int offset;
        if (is_left_child(rank)) offset = MIMPI_GROUP_L_READ_OFFSET;
        else offset = MIMPI_GROUP_R_READ_OFFSET;

        ret = chrecv(p + offset, data, count);
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
    }

    if (has_left_child(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_L_WRITE_OFFSET, data, count));
    }

    if (has_right_child(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_R_WRITE_OFFSET, data, count));
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
    u_int8_t* buf = malloc(count);
    memcpy(buf, send_data, count);

    if (has_left_child(rank)) {
        ret = chrecv(l + MIMPI_GROUP_R_READ_OFFSET, buf, count);
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
        reduction(buf, send_data, count, op);
    }

    if (has_right_child(rank)) {
        ret = chrecv(r + MIMPI_GROUP_R_READ_OFFSET, buf, count);
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
        reduction(buf, send_data, count, op);
    }

    if (!is_root(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_R_WRITE_OFFSET, buf, count));

        int offset;
        if (is_left_child(rank)) offset = MIMPI_GROUP_L_READ_OFFSET;
        else offset = MIMPI_GROUP_R_READ_OFFSET;

        ret = chrecv(p + offset, dummy, sizeof(char));
        ASSERT_SYS_OK(ret);
        CHECK_IF_REMOTE_FINISHED(rank, ret);
    } else {
        memcpy(recv_data, buf, count);
    }

    if (has_left_child(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_L_WRITE_OFFSET, dummy, sizeof(char)));
    }

    if (has_right_child(rank)) {
        ASSERT_SYS_OK(chsend(rank + MIMPI_GROUP_R_WRITE_OFFSET, dummy, sizeof(char)));
    }

    free(dummy);
    return MIMPI_SUCCESS;
}*/
