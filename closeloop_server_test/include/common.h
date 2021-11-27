#define _LARGEFILE64_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/queue.h>
#include <assert.h>
#include <limits.h>

#include <mtcp_api.h>
#include <mtcp_epoll.h>
#include "cpu.h"
#include "rss.h"
#include "http_parsing.h"
#include "netlib.h"
#include "debug.h"

#include "sizes.h"

#define LL long long

#define NUM_ITER 1000000000

#define PUT_PERCENT 50			// Percentage of PUT operations

#define REPLY_SIZE 50

#define NUM_KEYS M_1			// 51 * M_4 ~ 200 M keys
#define NUM_KEYS_ M_1_

#define SWAP(a,b) do{a^=b;b^=a;a^=b;}while(0)

#define VALUE_SIZE 256
#define KEY_SIZE 64

#define BATCHED_KEY

#ifdef BATCHED_KEY
#define NUM_BATCH 1
#endif

//#define __TEST_KV__

//#define ZERO_COPY

// The key-value struct in network connection
struct __attribute__((__packed__)) kv_trans_item {
	uint8_t key[KEY_SIZE];
	uint8_t value[VALUE_SIZE];
};

#define KV_ITEM_SIZE sizeof(struct kv_trans_item)

#ifndef MAX_CPUS
#define MAX_CPUS		16
#endif

struct server_arg {
    int core;
    int thread_id;
};

//Client args
struct client_arg {
    int thread_id;
    char * ip_addr;
    int port;
};

typedef enum {
    CLOSELOOP,
    OPENLOOP,
} test_t;

test_t benchmark;

static pthread_t cl_thread[110];
static struct client_arg cl_thread_arg[110];
