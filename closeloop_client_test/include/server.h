#include "common.h"

//mTCP library
#include <mtcp_api.h>
#include <mtcp_epoll.h>

#include "cpu.h"
#include "http_parsing.h"
#include "netlib.h"
#include "debug.h"

#include "mtcp.h"
#include "mtcp_api.h"
#include "tcp_in.h"
#include "tcp_stream.h"
#include "tcp_out.h"
#include "ip_out.h"
#include "eventpoll.h"
#include "pipe.h"
#include "fhash.h"
#include "addr_pool.h"
#include "rss.h"
#include "debug.h"

#define BUF_SIZE 2048
int buff_size = 1024;

#define MAX_FLOW_NUM  (10000)

#define MAX_EVENTS (MAX_FLOW_NUM * 3)

#ifndef TRUE
#define TRUE (1)
#endif

#ifndef FALSE
#define FALSE (0)
#endif

#ifndef ERROR
#define ERROR (-1)
#endif

#define HT_SUPPORT FALSE

#ifndef MAX_CPUS
#define MAX_CPUS		16
#endif

#ifndef MAX
#define MAX(a, b) ((a)>(b)?(a):(b))
#endif

#ifndef MIN
#define MIN(a, b) ((a)<(b)?(a):(b))
#endif

struct thread_context {
	mctx_t mctx;
	int ep;
	struct server_vars *svars;
};

struct server_vars {
	int recv_len;
	int request_len;
	long int total_read, total_sent;
	uint8_t done;
	uint8_t rspheader_sent;
	uint8_t keep_alive;

	int total_time;
	int scan_range;
//    struct ring_buf * recv_buf;
};

static int total_cores;
static int num_cores;
static pthread_t app_thread[MAX_CPUS];
static int done[MAX_CPUS];
static int task_start[MAX_CPUS];
static char *conf_file = NULL;
static int backlog = -1;

//static pthread_t sv_thread[MAX_CPUS];
static struct server_arg sv_thread_arg[MAX_CPUS];

__thread long long recv_bytes, send_bytes;

__thread long long request, reply;

__thread struct timeval start, log_start;
__thread int established_flag = 0;

__thread struct timeval end;

void * server_thread(void * arg);
