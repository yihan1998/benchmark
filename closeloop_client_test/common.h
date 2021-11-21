#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>

#include <sys/sysinfo.h>
#include <sys/timerfd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <fcntl.h>
#include <dirent.h>
#include <string.h>

#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <limits.h>

#define __USE_GNU
#include <sched.h>
#include <pthread.h>

#include <netinet/in.h>
#include <sys/epoll.h>

#include "sizes.h"

/* Client thread args */
struct client_arg {
    int core_id;
    char * ip_addr;
    int port;
};

typedef enum {
    SOCK_VAR,
    TIMER_VAR,
} type_t;

struct sock_info {
    char * file_ptr;
    int complete;
    int total_send;
    int total_recv;
#ifdef EVAL_RTT
    struct timeval start;
#endif
};

#define SOCK_INFO_SIZE  sizeof(struct sock_info)

#define SOCK_TIMER_SIZE sizeof(struct sock_timer)

struct param {
    int sockfd;
    int epfd;
    type_t type;
    void * data;
};

#define PARAM_SIZE sizeof(struct param)

//#define BUFF_SIZE   1024
int buff_size = 1024;

#define MAX_CLIENT_NUM  10000

