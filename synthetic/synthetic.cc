#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/core_workload.h"

#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <getopt.h>

#include <fcntl.h>

#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <sys/time.h>

#define TIMEVAL_TO_USEC(t)  ((t.tv_sec * 1000000) + (t.tv_usec))

#define MAX_CPUS    8

#define MAX_CONNECT 4100
#define MAX_EVENTS  8192

int rx = 0;
int buff_size = 1024;
void * (*func)(void *); 

__thread int num_accept = 0;
__thread int num_finished = 0;
__thread int num_conn = 0;

utils::Properties props;

struct conn_info {
    int     sockfd;
    int     epfd;

    long long   total_ops;
    long long   actual_ops;
};

__thread struct conn_info * info;

using namespace std;

int ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int AcceptConnection(int sock) {
    int c;
    
    struct sockaddr_in client_addr;
    socklen_t len = sizeof(client_addr);

    if ((c = accept(sock, (struct sockaddr *)&client_addr, &len)) < 0) {
        printf("\n accept connection error \n");
        return -1;
    }

    fcntl(c, F_SETFL, O_NONBLOCK);

    return c;
}

void * DelegateServer(void * arg) {
    int core_id = *((int *)arg);

    cpu_set_t core_set;
    CPU_ZERO(&core_set);
    CPU_SET(core_id, &core_set);

    if (pthread_setaffinity_np(pthread_self(), sizeof(core_set), &core_set) == -1){
        printf("warning: could not set CPU affinity, continuing...\n");
    }

    int oks = 0;

    int epfd;
    struct epoll_event * events;
    int nevents;

    int num_complete = 0;

    /* Create epoll fd */
    epfd = epoll_create1(0);

    /* Initialize epoll event array */
    events = (struct epoll_event *)calloc(MAX_EVENTS, sizeof(struct epoll_event));

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if(!sock) {
        printf(" [%s] allocate socket failed !\n", __func__);
        exit(1);
    }
    
    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    fcntl(sock, F_SETFL, O_NONBLOCK);

    /* Bind to port */
	struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;

    int port = stoi(props["port"]);
    addr.sin_port = htons(port);

    int ret;
    ret = bind(sock, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));
    if(ret < 0) {
        perror("bind failed");
        exit(1);
    }
    
    ret = listen(sock, 1024);
    if(ret < 0) {
        perror("listen failed");
        exit(1);
    }

    struct epoll_event ev;

    ev.events = EPOLLIN;
    ev.data.fd = sock;

    int ret;
    if ((ret = epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev)) == -1) {
        fprintf(stderr, " cetus_epoll_ctl() error on sock\n");
        exit(EXIT_FAILURE);
    }

    int wait_timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (wait_timerfd == -1) {
        fprintf(stderr, " timerfd_create() wait_timerfd failed!\n");
        exit(1);
    }

    struct itimerspec ts;
    ts.it_interval.tv_sec = 0;
	ts.it_interval.tv_nsec = 0;
	ts.it_value.tv_sec = 30;
	ts.it_value.tv_nsec = 0;

	if (timerfd_settime(wait_timerfd, 0, &ts, NULL) < 0) {
		fprintf(stderr, "cetus_timerfd_settime() failed");
        exit(EXIT_FAILURE);
	}

    ev.events = EPOLLIN;
    ev.data.fd = wait_timerfd;
    if ((ret = epoll_ctl(epfd, EPOLL_CTL_ADD, wait_timerfd, &ev)) == -1) {
        fprintf(stderr, " cetus_epoll_ctl() error on wait_timerfd sock\n");
        exit(EXIT_FAILURE);
    }

    // Peforms transactions
    utils::Timer<double> timer;
    timer.Start();

    int done = 0;
    while(!done) {
        nevents = epoll_wait(epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            if (events[i].data.fd == sock) {
                /* Accept connection */
                int c;
                if ((c = AcceptConnection(sock)) > 0) {
                    // std::cout <<  " accept connection through sock " << c << std::endl;
                    struct epoll_event ev;
                    ev.events = EPOLLIN;
                    ev.data.fd = c;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, c, &ev);

                    num_accept++;
                }
            }  else if (events[i].data.fd == wait_timerfd) {
                if (!num_accept) {
                    return NULL;
                } else {
                    continue;
                }
            } else if ((events[i].events & EPOLLERR)) {
                shutdown(events[i].data.fd, SHUT_WR);
                shutdown(events[i].data.fd, SHUT_RD);
                close(events[i].data.fd);
                if (++num_complete == num_accept) {
                    done = 1;
                }
            } else if ((events[i].events & EPOLLIN)) {
                char recv_buff[buff_size];
                int len = read(info->sockfd, recv_buff, buff_size);

                if (len <= 0) {
                    close(events[i].data.fd);
                    if (++num_complete == num_accept) {
                        done = 1;
                        continue;
                    }
                }

                int service_time = 1;
                if (sscanf(recv_buff, "SERVICE_TIME = %d\n", &service_time) == 1) {
                    struct timeval start;
                    gettimeofday(&start, NULL);
                    do {
                        struct timeval curr;
                        gettimeofday(&curr, NULL);
                    } while((TIMEVAL_TO_USEC(curr) - TIMEVAL_TO_USEC(start) < service_time) || (TIMEVAL_TO_USEC(curr) - TIMEVAL_TO_USEC(start) > 120));
                }

                char reply_buff[buff_size];
                sprintf(reply_buff, "+OK\n");
                
                write(info->sockfd, reply_buff, buff_size);

                oks++;
            }
        }
    }

    double duration = timer.End();

    fprintf(stdout, " [core %d] # Transaction throughput : %.2f (KTPS)\n", \
                    core_id, oks / duration / 1000);
    fflush(stdout);
    
    return NULL;
}

static int ConnectServer(const std::string &ip, int port) {
    int sock = 0;
    struct sockaddr_in server_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }
   
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());
   
    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        printf("\nConnection Failed \n");
        return -1;
    }

    fcntl(sock, F_SETFL, O_NONBLOCK);

    return sock;
}

void * DelegateClient(void * arg) {
    int core_id = *((int *)arg);

    cpu_set_t core_set;
    CPU_ZERO(&core_set);
    CPU_SET(core_id, &core_set);

    if (pthread_setaffinity_np(pthread_self(), sizeof(core_set), &core_set) == -1){
        printf("warning: could not set CPU affinity, continuing...\n");
    }

    CoreWorkload wl;
    wl.Init(props);

    int done = 0;

    int num_load_complete = 0;

    int epfd;
    struct epoll_event * events;

    /* Create epoll fd */
    epfd = epoll_create1(0);

    /* Initialize epoll event array */
    events = (struct epoll_event *)calloc(MAX_EVENTS, sizeof(struct epoll_event));

    int nevents;

    // Peforms transactions
    utils::Timer<double> timer;
    timer.Start();

    int num_flows = stoi(props.GetProperty("num_flows", "1"));

    int port = stoi(props["port"]);

    int num_complete = 0;

    int oks = 0;

    while(!done) {
        while(num_conn < num_flows) {
            /* Connect server */
            int sock;
            if ((sock = ConnectServer("10.0.1.1", port)) > 0) {
                // fprintf(stdout, " [%s] connect server through sock %d\n", __func__, sock);
                struct conn_info * conn_info = &info[num_conn];
                conn_info->sockfd = sock;
                conn_info->epfd = epfd;

                conn_info->total_ops = stoi(props.GetProperty("count", "1")) / num_flows;
                conn_info->actual_ops = 0;

                num_conn++;

                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.ptr = conn_info;
                epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev);
            } else {
                fprintf(stderr, " [%s] connect server failed!", __func__);
                exit(1);
            }
        }

        nevents = epoll_wait(epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & EPOLLERR)) {
                continue;
            }
            
            if ((events[i].events & EPOLLIN)) {
                char buff[buff_size];
                int len = read(info->sockfd, buff, buff_size);

                if (len > 0) {
                    if (strcmp(buff, "+OK\n")) {
                        fprintf(stderr, " Weird reply...\n");
                    }

                    oks++;

                    /* Increase actual ops */
                    if(++info->actual_ops == info->total_ops) {
                        // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                        // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                        if (++num_complete == num_conn) {
                            done = 1;
                        }
                    }
                    
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLOUT;
                    ev.data.ptr = info;

                    epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                }
            } else if ((events[i].events & EPOLLOUT)) {
                char buff[buff_size];
                sprintf(buff, "SERVICE_TIME = %d\n", (int)(wl.NextServiceTime()));

                int len = write(info->sockfd, buff, buff_size);
            
                if(len > 0) {
                    struct epoll_event ev;
                    ev.events = EPOLLIN;
                    ev.data.ptr = info;

                    epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                }
            } else {
                printf(" >> unknown event!\n");
            }
        }
    }

    double duration = timer.End();

    fprintf(stdout, " [core %d] # Transaction throughput : %.2f (KTPS) \t %s\n", \
                    core_id, oks / duration / 1000, props["dbname"].c_str());
    fflush(stdout);
    
    return NULL;
}

int main(const int argc, const char *argv[]) {
    ParseCommandLine(argc, argv, props);

    /* Initialize connection info array */
    info = (struct conn_info *)calloc(MAX_CONNECT, sizeof(struct conn_info));

    if (rx) {
        func = DelegateServer;
    } else {
        func = DelegateClient;
    }

    pthread_t tid[MAX_CPUS];
    int args[MAX_CPUS];

    int num_cores = stoi(props.GetProperty("num_cores", "1"));

    for (int i = 0; i < num_cores; i++) {
        args[i] = i;
        if (pthread_create(&tid[i], NULL, DelegateServer, (void *)&args[i]) != 0) {
            printf("pthread_create of server thread failed!\n");
            return 0;
        }
    }

    for (int i = 0; i < num_cores; i++) {
		pthread_join(tid[i], NULL);
	}

    return 0;
}

enum cfg_params {
    HOST,
    NUM_CORES,
    BUFF_SIZE,
    PORT,
    NUM_FLOWS,
};

const struct option options[] = {
    {   .name = "h", 
        .has_arg = no_argument,
        .flag = NULL, 
        .val = HOST},
    {   .name = "c", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = NUM_CORES},
    {   .name = "b", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = BUFF_SIZE},
    {   .name = "p", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = PORT},
    {   .name = "n", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = NUM_FLOWS},
    {0, 0, 0, 0}
};

int ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
    int ret, done = 0;
    char * end;
    optind = 1;
    string strarg;

    while (!done) {
        ret = getopt_long(argc, (char * const *)argv, "", options, NULL);
        switch (ret) {
            case HOST:
                rx = 1;
                break;
            
            case NUM_CORES:
                props.SetProperty("num_cores", optarg);
                break;

            case BUFF_SIZE:
                strarg.assign(optarg);
                buff_size = stoi(strarg);
                break;
            
            case PORT:
                props.SetProperty("port", optarg);
                break;

            case NUM_FLOWS:
                props.SetProperty("num_flows", optarg);
                break;

            case -1:
                done = 1;
                break;
        }
    }

    return 0;
}