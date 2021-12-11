#include "common.h"

#define NUM_TEST    M_1

#define TEST_FUNCTION   0
#define TEST_FILE       1

#define CPU_NUM     16

#define BIND_CORE

pthread_t thread[CPU_NUM];
struct client_arg thread_arg[CPU_NUM];

int num_cores;

int num_flow;

__thread int core_id;

int num_server_core;

int num_client_fp;

__thread char * input_file;

int concurrency = 10000;

__thread int num_connection = 0;

int eval_time;

#define MAX_FD  (concurrency * 3)

__thread int done = 0;

__thread int num_complete = 0;

__thread struct timeval start, end;

__thread struct param * vars;

__thread int timerfd;

#ifdef EVAL_RTT
__thread long long * rtt_buff;
__thread int rtt_buff_len;
__thread FILE * rtt_file;
#endif

int handle_signal(int signal) {
    if (signal == SIGINT) {
        printf(" [%s] received interrupt signal", __func__);
        exit(1);
    }
}

int connect_server(int epfd, char * server_ip, int port) {
    int sockfd;

    struct sockaddr_in server_addr;

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(server_ip);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(sockfd == -1){
        perror(" create socket failed");
        return -1;
    }

    int err = -1;
    int snd_size = 0; 
    int rcv_size = 0;  
    socklen_t optlen; 

    // fprintf(stdout, " [%s] connecting to %s(%x)\n", __func__, server_ip, port);

    if(connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0){
        perror("[CLIENT] connect server failed");
        return -1;
    }
    // fprintf(stdout, " [%s] %s(%x) connected\n", __func__, server_ip, port);

    fcntl(sockfd, F_SETFL, O_NONBLOCK);

    vars[num_connection].sockfd = sockfd;
    vars[num_connection].epfd = epfd;
    vars[num_connection].type = SOCK_VAR;
    
    struct sock_info * info = (struct sock_info *)calloc(1, SOCK_INFO_SIZE);
    info->file_ptr = input_file;
    info->complete = 0;
    info->total_send = 0;
    info->total_recv = 0;

    vars[num_connection].data = (void *)info;

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT;
    ev.data.ptr = &vars[num_connection];
    epoll_ctl(epfd, EPOLL_CTL_ADD, sockfd, &ev);

    gettimeofday(&start, NULL);

    num_connection++;

    return sockfd;
}

int close_connection(int epfd, int sockfd, struct param * vars) {
    //epoll_ctl(epfd, EPOLL_CTL_DEL, sockfd, NULL);
    //printf("\t >> closing sock %d\n", sockfd);
    close(sockfd);

    struct sock_info * info = (struct sock_info *)vars->data;
    info->complete = 1;

    num_complete++;

    return 1;
}

int handle_read_event(int epfd, int sockfd, struct param * vars) {
    struct sock_info * info = (struct sock_info *)vars->data;

    char recv_buff[buff_size];
    
    int len = read(sockfd, recv_buff, buff_size);
    // printf(" [%s] recv %d bytes, %.*s\n", __func__, len, len, recv_buff);
    
    if(len <= 0) {
        return len;
    }

    info->total_recv += len;

    if (info->total_recv == info->total_send) {
#ifdef EVAL_RTT
        struct timeval current;
        gettimeofday(&current, NULL);

        long long elapsed;
        if (current.tv_usec < info->start.tv_usec) {
            elapsed = 1000000 + current.tv_usec - info->start.tv_usec;
        } else {
            elapsed = current.tv_usec - info->start.tv_usec;
        }

        if (rtt_buff_len < M_1) {
            rtt_buff[rtt_buff_len++] = elapsed;
        }
#endif
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT;
        ev.data.ptr = vars;

        epoll_ctl(epfd, EPOLL_CTL_MOD, sockfd, &ev);
    }

}

int
handle_write_event(int epfd, int sockfd, struct param * vars) {
    struct sock_info * info = (struct sock_info *)vars->data;

    if (info->file_ptr + buff_size >= input_file + M_512) {
        info->file_ptr = input_file;
    }

    int send_len = write(sockfd, info->file_ptr, buff_size);
    // printf(" [%s] send %d bytes, %.*s\n", __func__, send_len, send_len, info->file_ptr);

    if(send_len < 0) {
        return send_len;
    }

    info->total_send += send_len;
    info->file_ptr = input_file + ((info->file_ptr - input_file) + send_len) % M_512;

#ifdef EVAL_RTT
    gettimeofday(&info->start, NULL);
#endif
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.ptr = vars;

    epoll_ctl(epfd, EPOLL_CTL_MOD, sockfd, &ev);
}

int handle_timeup_event(int epfd, int timerfd) {
    printf(" ========== [%s] ==========\n", __func__);

    for (int i = 0; i < num_connection; i++) {
        struct param * var = &vars[i];
        shutdown(var->sockfd, SHUT_WR);
    }

    sleep(2);
/*
    for (int i = 0; i < num_connection; i++) {
        struct param * var = &vars[i];
        shutdown(var->sockfd, SHUT_RD);
    }

    sleep(2);
*/    
    while (num_complete < concurrency && num_complete < num_connection) {
        for (int i = 0; i < num_connection; i++) {
            struct param * var = &vars[i];
            struct sock_info * info = (struct sock_info *)var->data;
            if (!info->complete) {
                int ret = close_connection(epfd, var->sockfd, var);
                if (ret < 0) {
                    continue;
                }
                //printf(" >> Connection(sock: %d) closed, total: %d\n", var->sockfd, num_complete);
                gettimeofday(&end, NULL);
            }
        }
    }
#ifdef EVAL_RTT
    for (int i = 0; i < rtt_buff_len; i++) {
        fprintf(rtt_file, "%llu\n", rtt_buff[i]);
        fflush(rtt_file);
    }

    fclose(rtt_file);
#endif
}

void * RunClientThread(void * argv) {
    //printf(" ========== Running Client Thread ==========\n");

    struct client_arg * thread_arg = (struct client_arg *)argv;
    
    int core_id = thread_arg->core_id;
    char * server_ip = thread_arg->ip_addr;

    input_file = (char *)malloc(M_128);
    
    FILE * fp = fopen("input.dat", "rb");

    fread(input_file, 1, M_128, fp);

    fclose(fp);

#if defined(EVAL_TAS_BIND) || defined(EVAL_TAS_SEP)
    int server_port = (core_id % num_server_core + 1) << 12;
    printf(" [%s] test tas: connecting to port %x\n", __func__, server_port);
#else
    int server_port = thread_arg->port;
    printf(" [%s] test linux: connecting to port %x\n", __func__, server_port);
#endif

    int bind_core = core_id + num_client_fp + 1;
    printf(" [%s] bind to core %d\n", __func__, bind_core);

    cpu_set_t core_set;

    CPU_ZERO(&core_set);

    CPU_SET(bind_core, &core_set);

    if (pthread_setaffinity_np(pthread_self(), sizeof(core_set), &core_set) == -1){
        printf("warning: could not set CPU affinity, continuing...\n");
    }

#ifdef EVAL_RTT
    char name[20];
    sprintf(name, "rtt_core_%d.txt", core_id);
    rtt_file = fopen(name, "wb");
    fseek(rtt_file, 0, SEEK_END);

    rtt_buff = (long long *)calloc(M_1, sizeof(long long));
    rtt_buff_len = 0;
#endif

    vars = (struct param *)calloc(MAX_FD, PARAM_SIZE);

    int epfd;
    struct epoll_event * events;
    int nevents;

    int maxevent = MAX_FD * 3;

    epfd = epoll_create1(0);

    events = (struct epoll_event *)calloc(maxevent, sizeof(struct epoll_event));

    timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (timerfd < 0) {
        printf(" >> timerfd_create error!\n");
        exit(1);
    }

    //printf("\t >> Create timerfd: %d\n", timerfd);

    struct itimerspec ts;
    
    struct param * timer_param = (struct param *)calloc(1, PARAM_SIZE);
    timer_param->sockfd = timerfd;
    timer_param->epfd = epfd;
    timer_param->type = TIMER_VAR;
    timer_param->data = NULL;

    ts.it_interval.tv_sec = 0;
	ts.it_interval.tv_nsec = 0;
	ts.it_value.tv_sec = eval_time;
	ts.it_value.tv_nsec = 0;

	if (timerfd_settime(timerfd, 0, &ts, NULL) < 0) {
		perror("timerfd_settime() failed");
	}

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.ptr = timer_param;
    epoll_ctl(epfd, EPOLL_CTL_ADD, timerfd, &ev);

    while(!done) {
        while(num_connection < concurrency && num_connection < num_flow) {
            if(connect_server(epfd, server_ip, server_port) < 0) {
                done = 1;
                break;
            }
        }

        //printf(" [%s] Waiting for events...\n", __func__);

        nevents = epoll_wait(epfd, events, maxevent, -1);

        //printf(" [%s] receive %d events\n", __func__, nevents);

        for (int i = 0; i < nevents; i++) {
            struct param * var = (struct param *)events[i].data.ptr;
            if ((events[i].events & EPOLLIN) && var->type == TIMER_VAR && var->sockfd == timerfd) {   
                printf(" >> Time's up\n");
                handle_timeup_event(epfd, timerfd);
                if (num_complete == num_flow) {
                    printf(" >> All connections are closed\n");
                    done = 1;
                    break;
                }
            }else if ((events[i].events & EPOLLERR) && var->type == SOCK_VAR) {
                printf(" >> EPOLLERR on socket %d!\n", var->sockfd);
                close_connection(epfd, var->sockfd, var);
            } else if ((events[i].events & EPOLLIN) && var->type == SOCK_VAR) {
                handle_read_event(epfd, var->sockfd, var);
            } else if ((events[i].events & EPOLLOUT) && var->type == SOCK_VAR) {
                handle_write_event(epfd, var->sockfd, var);
            } else {
                printf(" >> unknown event!\n");
            }
            
        }
    }

    pthread_exit(NULL);

    return NULL;
}

int main(int argc, char * argv[]) {
    int i;
    char server_ip[20];
    int server_port;

    for (i = 0; i < argc; i++){
        double d;
        uint64_t n;
        char junk;
        char s[20];
        if(sscanf(argv[i], "--num_flow=%llu%c", &n, &junk) == 1) {
            num_flow = n;
            printf(" >> flow num per core: %d\n", num_flow);
        }else if(sscanf(argv[i], "--num_cores=%llu", &n) == 1) {
            num_cores = n;
            printf(" >> number of cores: %d\n", num_cores);
        }else if(sscanf(argv[i], "--size=%llu%c", &n, &junk) == 1) {
            buff_size = n;
            printf(" >> buff size: %d\n", buff_size);
        }else if(sscanf(argv[i], "--time=%llu%c", &n, &junk) == 1) {
            eval_time = n;
            printf(" >> evaluation time: %d\n", eval_time);
        }else if(sscanf(argv[i], "--server_ip=%s%c", server_ip, &junk) == 1) {
            printf(" >> server ip: %s\n", server_ip);
        }else if(sscanf(argv[i], "--server_port=%d%c", &server_port, &junk) == 1) {
            printf(" >> server port: %d\n", server_port);
        }else if(sscanf(argv[i], "--num_server_core=%d%c", &num_server_core, &junk) == 1) {
            printf(" >> number of server core: %d\n", num_server_core);
        }else if(sscanf(argv[i], "--num_client_fp=%d%c", &num_client_fp, &junk) == 1) {
            printf(" >> number of client fast path core: %d\n", num_client_fp);
        }else if(i > 0) {
            printf("error (%s)!\n", argv[i]);
        }
    }

    signal(SIGINT, handle_signal);

    for (int i = 0; i < num_cores; i++) {
        thread_arg[i].core_id = i;
        thread_arg[i].ip_addr = server_ip;
        thread_arg[i].port = server_port + i % num_server_core;
        if (pthread_create(&thread[i], NULL, RunClientThread, (void *)&thread_arg[i]) != 0) {
            printf("pthread_create of server thread failed!\n");
            return 0;
        }
    }
    
    for (int i = 0; i < num_cores; i++) {
		pthread_join(thread[i], NULL);
	}

}
