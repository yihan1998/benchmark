#include "common.h"

__thread struct timeval start, log_start, end;
__thread int established_flag = 0;

__thread long long sec_recv_bytes, sec_send_bytes;
__thread long long recv_bytes, send_bytes;
__thread long long request, reply;

__thread int num_accept = 0;
__thread int num_finished = 0;

__thread int done = 0;

int eval_time;

__thread int timerfd;
__thread int wait_timerfd;

int num_cores;
__thread int core_id;

int num_server_fp;

void handle_signal(int signal) {
    if (signal == SIGINT) {
        printf(" [%s] received interrupt signal", __func__);
        exit(1);
    }
}

int setnonblocking(int sockfd) {
    if (fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFD, 0)|O_NONBLOCK) == -1) {
        return -1;
    }
    return 0;
}

int accept_connection(int epfd, int sock) {
    int c;
    
    struct sockaddr_in client;
    socklen_t len = sizeof(client);

    c = accept(sock, (struct sockaddr *)&client, &len);

    if(c < 0) {
        perror(" accept error ");
        return -1;
    }

    if (setnonblocking(c) < 0) {
        perror("setnonblocking error");
        return -1;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = c;
    epoll_ctl(epfd, EPOLL_CTL_ADD, c, &ev);

    num_accept++;

    if(!established_flag) {
        gettimeofday(&start, NULL);
        gettimeofday(&log_start, NULL);
        established_flag = 1;

        /* Set timer */
        timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
        if (timerfd < 0) {
            printf(" >> timerfd_create error!\n");
            exit(1);
        } else {
            printf(" [%s] create timerfd: %d\n", __func__, timerfd);
        }

        struct itimerspec ts;

        ts.it_interval.tv_sec = 0;
	    ts.it_interval.tv_nsec = 0;
    	ts.it_value.tv_sec = eval_time + 2;
	    ts.it_value.tv_nsec = 0;

    	if (timerfd_settime(timerfd, 0, &ts, NULL) < 0) {
	    	perror("timerfd_settime() failed");
	    }

        struct epoll_event timer_ev;
        timer_ev.events = EPOLLIN;
        timer_ev.data.fd = timerfd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, timerfd, &timer_ev);
    }

    return c;
}

int handle_read_event(int sockid) {
    char buff[buff_size];

    int len = read(sockid, buff, buff_size);
    //printf(" [%s] read %d bytes\n", __func__, len);

    if(len <= 0) {
        return len;
    }

	recv_bytes += len;
	request++;

    int send_len = write(sockid, buff, len);
    //printf(" [%s] write %d bytes\n", __func__, send_len);

    if(send_len < 0) {
        return send_len;
    }

    send_bytes += send_len;
    reply++;
}

int close_connection(int epfd, int sockid) {
    //printf(" ========== [%s] ==========\n", __func__);
    //printf(" [%s] close sock %d\n", __func__, sockid);
    close(sockid);

    gettimeofday(&end, NULL);
}

void * server_thread(void * arg) {
	struct thread_arg * args = (struct thread_arg *)arg;
	
	int thread_id = args->thread_id;
	int core_id = args->core_id;

#if defined(EVAL_TAS_BIND) || defined(EVAL_TAS_SEP)
    int server_port = core_id << 12;
    printf(" [%s] test tas: listening to port %x\n", __func__, server_port);
#else
    int server_port = port;
    printf(" [%s] test linux: listening to port %x\n", __func__, server_port);
#endif

#if defined(EVAL_TAS_BIND)
    int bind_core = core_id;
    printf(" [%s] test tas bind: bind to core %d\n", __func__, bind_core);
#elif defined(EVAL_TAS_SEP)
    int bind_core = core_id + num_server_fp;
    printf(" [%s] test tas seperate: bind to core %d\n", __func__, bind_core);
#else
    int bind_core = core_id;
    printf(" [%s] test linux: bind to core %d\n", __func__, bind_core);
#endif

    cpu_set_t core_set;

    CPU_ZERO(&core_set);

    CPU_SET(bind_core, &core_set);

    printf(" [%s] server core %d listening to port %x\n", __func__, bind_core, server_port);

    if (pthread_setaffinity_np(pthread_self(), sizeof(core_set), &core_set) == -1){
        printf("warning: could not set CPU affinity, continuing...\n");
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if(!sock) {
        printf(" [%s] allocate socket failed !\n", __func__);
        exit(1);
    }

    //printf(" [%s] create sock %d\n", __func__, sock);

    int opt = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
    }

    if (setnonblocking(sock) < 0) {
        perror("setnonblock error");
    }

    /* Bind to port */
	struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(server_port);

    int ret;
    ret = bind(sock, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));
    if(ret < 0) {
        perror(" bind failed!");
    }
    
    ret = listen(sock, 1024);
    if(ret < 0) {
        perror(" listen failed!");
    }

    /* Create epoll fd */
    int epfd = epoll_create(MAX_EVENTS);
    struct epoll_event * events = (struct epoll_event *)calloc(MAX_EVENTS, sizeof(struct epoll_event));

    struct epoll_event ev;

    ev.events = EPOLLIN;
    ev.data.fd = sock;

    epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev);

    /* Set wait connection timer */
    wait_timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (wait_timerfd < 0) {
        printf(" >> timerfd_create error!\n");
        exit(1);
    } else {
        printf(" [%s] create wait_timerfd: %d\n", __func__, wait_timerfd);
    }

    struct itimerspec ts;

    ts.it_interval.tv_sec = 0;
    ts.it_interval.tv_nsec = 0;
    ts.it_value.tv_sec = eval_time;
    ts.it_value.tv_nsec = 0;

    if (timerfd_settime(wait_timerfd, 0, &ts, NULL) < 0) {
    	perror("timerfd_settime() failed");
	}

    struct epoll_event timer_ev;
    timer_ev.events = EPOLLIN;
    timer_ev.data.fd = wait_timerfd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, wait_timerfd, &timer_ev);

    printf("========== Start to wait events ==========\n");
    
    int num_events;
    while(!done) {
        num_events = epoll_wait(epfd, events, MAX_EVENTS, -1);

        //printf(" [%s] Waiting for events...\n", __func__);

        if(num_events < 0) {
            printf(" [%s] epoll wait return %d\n", __func__, num_events);
            break;
        }

        //printf(" [%s] receive %d events\n", __func__, num_events);

        for (int i = 0; i < num_events; i++) {
            /* Handle received events */
            //printf(" [%s] event sock: %d\n", __func__, events[i].data.fd);
            if (events[i].data.fd == sock) {
                ret = accept_connection(epfd, sock);
            } else if (events[i].data.fd == timerfd) {
                printf(" [%s] Time's up\n", __func__);
                close_connection(epfd, sock);
                done = 1;
            }else if (events[i].data.fd == wait_timerfd) {
                printf(" [%s] Wait connection time's up\n", __func__);
                if (!established_flag) {
                    printf(" >> close idle thread\n");
                    done = 1;
                }
                epoll_ctl(epfd, EPOLL_CTL_DEL, wait_timerfd, NULL);
            } else if (events[i].events & EPOLLIN) {
                ret = handle_read_event(events[i].data.fd);
				if (ret == 0) {
                    close_connection(epfd, events[i].data.fd);
	                num_finished++;
    	            if (num_finished == num_accept) {
            	        done = 1;
                	}
				}
            } else if (events[i].events & EPOLLERR) {
                close_connection(epfd, events[i].data.fd);
                num_finished++;
                if (num_finished == num_accept) {
                    done = 1;
                }
            }
        }
        
    }

	printf("========== Clean up ==========\n");
    
    double start_time = (double)start.tv_sec * 1000000 + (double)start.tv_usec;
    double end_time = (double)end.tv_sec * 1000000 + (double)end.tv_usec;
    double total_time = (end_time - start_time)/1000000.00;

    char result_buff[512];

	char throughput_file_name[32];
	sprintf(throughput_file_name, "throughput_core_%d.txt", core_id);

	FILE * throughput_file = fopen(throughput_file_name, "a+");

    sprintf(result_buff, " [%d] recv payload rate: %.2f(Mbps), recv request rate: %.2f, send payload rate: %.2f(Mbps), send reply rate: %.2f\n", 
    	                num_accept, (recv_bytes * 8.0) / (total_time * 1000 * 1000), request / (total_time * 1000), 
        	            (send_bytes * 8.0) / (total_time * 1000 * 1000), reply / (total_time * 1000));
	
	
	printf("%s", result_buff);

	fprintf(throughput_file, result_buff);
	
	fclose(throughput_file);

    pthread_exit(NULL);

    return NULL;
}

int main(int argc, char ** argv) {
    int virtual_flag = 0;

    for (int i = 0; i < argc; i++){
        long long unsigned n;
        char junk;
        char s[20];
        if(strcmp(argv[i], "--virtual") == 0){
            virtual_flag = 1;
        }else if(sscanf(argv[i], "--size=%llu%c", &n, &junk) == 1){
            buff_size = n;
			printf(" >> buff size: %d\n", buff_size);
        }else if(sscanf(argv[i], "--port=%llu%c", &n, &junk) == 1){
            port = n;
			printf(" >> port: %d\n", port);
        }else if(sscanf(argv[i], "--time=%llu%c", &n, &junk) == 1) {
            eval_time = n;
            printf(" >> evaluation time: %d\n", eval_time);
        }else if(sscanf(argv[i], "--num_cores=%llu%c", &n, &junk) == 1) {
            num_cores = n;
            printf(" >> number of cores: %d\n", core_id);
        }else if(sscanf(argv[i], "--num_server_fp=%d%c", &num_server_fp, &junk) == 1) {
            printf(" >> number of server fast path core: %d\n", num_server_fp);
        }else if(i > 0){
            printf("error (%s)!\n", argv[i]);
        }
    }

	signal(SIGINT, handle_signal);
    
    for (int i = 0; i < num_cores; i++) {
        thread_arg[i].thread_id = 0;
    	thread_arg[i].core_id = core_id;
        if (pthread_create(&thread[i], NULL, server_thread, (void *)&thread_arg[i]) != 0) {
            printf("pthread_create of server thread failed!\n");
            return 0;
        }
    }
    
    for (int i = 0; i < num_cores; i++) {
		pthread_join(thread[i], NULL);
	}
    
    printf(" [%s] Test finished!\n", __func__);
	
	return 0;
}
