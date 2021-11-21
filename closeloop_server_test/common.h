#ifndef APP_H
#define APP_H

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <fcntl.h>
#include <dirent.h>
#include <string.h>

#include <sys/time.h>
#include <sys/timerfd.h>
#include <pthread.h>
#include <signal.h>
#include <limits.h>

#include <sched.h>
#include <pthread.h>

#include <sys/sysinfo.h>

#include <sys/epoll.h>

#define MAX_CPUS    16

/* thread info for application thread */

struct thread_arg {
    int core_id;
    int thread_id;
};

//#define BUFF_SIZE   1024
int buff_size = 1024;

int port;

#ifndef MAX_FLOW
#define MAX_FLOW    10000
#define MAX_EVENTS  (MAX_FLOW * 3)
#endif

pthread_t thread[MAX_CPUS];
struct thread_arg thread_arg[MAX_CPUS];

#endif