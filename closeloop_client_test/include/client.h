#include "common.h"

static struct thread_context *g_ctx[MAX_CPUS] = {0};

#define MAX_URL_LEN 128
#define FILE_LEN    128
#define FILE_IDX     10
#define MAX_FILE_LEN (FILE_LEN + FILE_IDX)
#define HTTP_HEADER_LEN 1024

#define IP_RANGE 1
#define MAX_IP_STR_LEN 16

#define BUF_SIZE (8*1024)

#define CALC_MD5SUM FALSE

#define TIMEVAL_TO_MSEC(t)		((t.tv_sec * 1000) + (t.tv_usec / 1000))
#define TIMEVAL_TO_USEC(t)		((t.tv_sec * 1000000) + (t.tv_usec))
#define TS_GT(a,b)				((int64_t)((a)-(b)) > 0)

static pthread_t app_thread[MAX_CPUS];
static mctx_t g_mctx[MAX_CPUS];
static int done[MAX_CPUS];
/*----------------------------------------------------------------------------*/
static int num_cores;
static int core_limit;
/*----------------------------------------------------------------------------*/
static int fio = FALSE;
static char outfile[FILE_LEN + 1];
/*----------------------------------------------------------------------------*/
static char host[MAX_IP_STR_LEN + 1] = {'\0'};
static char url[MAX_URL_LEN + 1] = {'\0'};
static in_addr_t daddr;
static in_port_t dport;
static in_addr_t saddr;
/*----------------------------------------------------------------------------*/
static int total_flows;
static int flows[MAX_CPUS];
static int flowcnt = 0;
static int concurrency = 10000;
static int max_fds = 10000;
static uint64_t response_size = 0;

//pthread_mutex_t work_done_lock;
int work_done_flag = 0;

//pthread_mutex_t fin_client_thread_lock;
int fin_client_thread = 0;


struct send_info {
    int * sockfd;
//    pthread_mutex_t * send_lock;
    int * send_byte;
//    pthread_mutex_t * recv_lock;
    int * recv_byte;
    int thread_id;
};

struct sock_info {
    char * file_ptr;
    int total_send;
    int total_recv;
#ifdef EVAL_RTT
    struct timeval start;
#endif
};

#define SOCK_INFO_SIZE  sizeof(struct sock_info)

struct conn_stat {
    int sockfd;
    int complete;
    struct sock_info * info;
};

#define CONN_STAT_SIZE sizeof(struct conn_stat)

struct thread_context {
	int core;

	mctx_t mctx;
	int ep;

	int target;
	int started;
	int errors;
	int incompletes;
	int done;
	int pending;

	struct conn_stat * stats;
};

char input_file[M_512];

typedef struct thread_context* thread_context_t;

#define SEND_INFO_SIZE sizeof(struct send_info)

int connect_server(char * server_ip, int port);

void send_request_thread(struct send_info * info);

void * send_request(void * arg);

void receive_response_thread(struct send_info * info);

void response_process(int sock, short event, void * arg);

uint8_t * value_corpus;

void gen_key_corpus(LL * key_corpus, int num_put, int thread_id);
void gen_value_corpus(uint8_t * value_corpus, int num_put);

int bufcmp(char * a, char * b, int buf_len);
