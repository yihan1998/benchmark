#include "client.h"

int buff_size = 1024;

int num_flow;

static char *conf_file = NULL;
char server_ip[20];
int server_port;

int execution_time;

int start_flag = 0;
struct timeval start, current;

__thread int num_connection;

#ifdef EVAL_RTT
__thread long long * rtt_buff;
__thread int rtt_buff_len;
__thread FILE * rtt_file;
#endif

__thread int core;

__thread thread_context_t ctx;

thread_context_t 
CreateContext(int core)
{
	ctx = (thread_context_t)calloc(1, sizeof(struct thread_context));
	if (!ctx) {
		perror("malloc");
		TRACE_ERROR("Failed to allocate memory for thread context.\n");
		return NULL;
	}
	ctx->core = core;

	ctx->mctx = mtcp_create_context(core);
	if (!ctx->mctx) {
		TRACE_ERROR("Failed to create mtcp context.\n");
		free(ctx);
		return NULL;
	}
	g_mctx[core] = ctx->mctx;

	return ctx;
}

void 
DestroyContext(thread_context_t ctx) 
{
	mtcp_destroy_context(ctx->mctx);
	for (int i = 0; i < num_connection; i++) {
		free(ctx->stats[i].info);
	}
	
	free(ctx->stats);
}

int CreateConnection(thread_context_t ctx){
    mctx_t mctx = ctx->mctx;
	struct mtcp_epoll_event ev;
	struct sockaddr_in addr;
	int sockid;
	int ret;

	sockid = mtcp_socket(mctx, AF_INET, SOCK_STREAM, 0);
	if (sockid < 0) {
		TRACE_INFO("Failed to create socket!\n");
		return -1;
	}

	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = inet_addr(server_ip);
	addr.sin_port = htons(server_port);
	
	ret = mtcp_connect(mctx, sockid, 
			(struct sockaddr *)&addr, sizeof(struct sockaddr_in));
	if (ret < 0) {
		if (errno != EINPROGRESS) {
			perror("mtcp_connect");
			mtcp_close(mctx, sockid);
			return -1;
		}
	}

	//printf(" [%s on core %d] create sock %d\n", __func__, core, ret);

	ret = mtcp_setsock_nonblock(mctx, sockid);
	if (ret < 0) {
		TRACE_ERROR("Failed to set socket in nonblocking mode.\n");
		exit(-1);
	}

	ctx->started++;
	ctx->pending++;

	ctx->stats[num_connection].sockfd = sockid;
	ctx->stats[num_connection].complete = 0;

	struct sock_info * info = (struct sock_info *)calloc(1, SOCK_INFO_SIZE);
    info->file_ptr = input_file;
    info->total_send = 0;
    info->total_recv = 0;

    ctx->stats[num_connection].info = info;

	ev.events = MTCP_EPOLLOUT;
	ev.data.ptr = &ctx->stats[num_connection];
	mtcp_epoll_ctl(mctx, ctx->ep, MTCP_EPOLL_CTL_ADD, sockid, &ev);

	num_connection++;

	return sockid;
}

static inline void 
CloseConnection(thread_context_t ctx, int sockid)
{
	//printf(" [%s on core %d] close sock %d\n", __func__, core, sockid);
	//mtcp_epoll_ctl(ctx->mctx, ctx->ep, MTCP_EPOLL_CTL_DEL, sockid, NULL);
	mtcp_close(ctx->mctx, sockid);
	ctx->pending--;
	ctx->done++;
/*
	assert(ctx->pending >= 0);
	while (ctx->pending < concurrency && ctx->started < ctx->target) {
		if (CreateConnection(ctx) < 0) {
			done[ctx->core] = TRUE;
			break;
		}
	}
*/
}

static inline int
HandleReadEvent(thread_context_t ctx, int sockid, struct conn_stat * var)
{
    struct sock_info * info = var->info;
 
    mctx_t mctx = ctx->mctx;

    char recv_buff[buff_size];
    
	int len = mtcp_read(mctx, sockid, recv_buff, buff_size);
    
	if(len <= 0) {
        return len;
    }

	info->total_recv += len;

    if (benchmark == CLOSELOOP && info->total_recv == info->total_send) {
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
        /* Close loop test */
        struct mtcp_epoll_event ev;
        ev.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
        ev.data.ptr = var;

		mtcp_epoll_ctl(mctx, ctx->ep, MTCP_EPOLL_CTL_MOD, sockid, &ev);
    }

	return len;
}

static inline int
HandleWriteEvent(thread_context_t ctx, int sockid, struct conn_stat * var)
{
    struct sock_info * info = var->info;

    mctx_t mctx = ctx->mctx;

    if (info->file_ptr + buff_size >= input_file + M_512) {
        info->file_ptr = input_file;
    }

#ifdef EVAL_RTT
    gettimeofday(&info->start, NULL);
#endif

    int send_len = mtcp_write(mctx, sockid, info->file_ptr, buff_size);

	if(send_len < 0) {
        return send_len;
    }
    
	info->total_send += send_len;
    info->file_ptr = input_file + ((info->file_ptr - input_file) + send_len) % M_512;

	if (benchmark == CLOSELOOP) {
		struct mtcp_epoll_event ev;
		ev.events = MTCP_EPOLLIN;
		ev.data.ptr = var;
		mtcp_epoll_ctl(mctx, ctx->ep, MTCP_EPOLL_CTL_MOD, sockid, &ev);
	}
}

void
ClientSignalHandler(int signum) {
	if (signum == SIGQUIT) {
		printf(" [%s] receive SIGQUIT signal\n", __func__);
		for (int i = 0; i < num_cores; i++) {
			if (app_thread[i] == pthread_self()) {
				printf(" [%s] exit current thread on core %d\n", __func__, i);
				for (int i = 0; i < num_connection; i++) {
					CloseConnection(ctx, ctx->stats[i].sockfd);
				}
				done[i] = TRUE;
			}
		}
/*
		for (int i = 0; i < num_cores; i++) {
			if (app_thread[i] != pthread_self()) {
				int kill_rc = pthread_kill(app_thread[i], 0);

				if (kill_rc == ESRCH) {
					printf("the specified thread did not exists or already quit\n");
				}else if(kill_rc == EINVAL) {
					printf("signal is invalid\n");
				}else{
					printf("the specified thread is alive\n");
					int ret = pthread_kill(app_thread[i], SIGQUIT);
					if (ret == EINVAL) {
						printf("Invalid signal\n");
					} else if (ret == ESRCH) {
						printf("No thread os found\n");
					} else {
						printf("succeed!\n");
						//pthread_kill(app_thread[i], SIGTERM);
					}
				}
			}
		}

		printf(" [%s] destroy context on core %d!\n", __func__, core);
		mtcp_destroy_context(ctx->mctx);

		printf(" [%s] thread on core %d exit!\n", __func__, core);
		//pthread_exit(NULL);
*/
	}
}

void * RunClientThread(void * arg){
	signal(SIGQUIT, ClientSignalHandler);

	core = *(int *)arg;
	struct in_addr daddr_in;
	int n, maxevents;
	int ep;
	struct mtcp_epoll_event *events;
	int nevents;
	int i;
	mctx_t mctx;

	struct timeval cur_tv, prev_tv;
	//uint64_t cur_ts, prev_ts;

	printf(" [%s] binding to core %d\n", __func__, core);
	mtcp_core_affinitize(core);

	ctx = CreateContext(core);
	if (!ctx) {
		return NULL;
	}

	mctx = ctx->mctx;
	g_ctx[core] = ctx;
	srand(time(NULL));

	mtcp_init_rss(mctx, INADDR_ANY, IP_RANGE, inet_addr(server_ip), server_port);

	ctx->target = num_flow;

	daddr_in.s_addr = daddr;
	fprintf(stderr, "Thread %d handles %d flows. connecting to %s:%u\n", 
			core, num_flow, server_ip, server_port);

	/* Initialization */
	maxevents = max_fds * 3;
	ep = mtcp_epoll_create(mctx, maxevents);
	if (ep < 0) {
		fprintf(stderr,"Failed to create epoll struct!n");
		exit(EXIT_FAILURE);
	}
	events = (struct mtcp_epoll_event *)
			calloc(maxevents, sizeof(struct mtcp_epoll_event));
	if (!events) {
		fprintf(stderr,"Failed to allocate events!\n");
		exit(EXIT_FAILURE);
	}
	ctx->ep = ep;

	ctx->stats = (struct conn_stat *)calloc(max_fds, CONN_STAT_SIZE);

	ctx->started = ctx->done = ctx->pending = 0;
	ctx->errors = ctx->incompletes = 0;

	gettimeofday(&cur_tv, NULL);
	//prev_ts = TIMEVAL_TO_USEC(cur_tv);
	prev_tv = cur_tv;

    FILE * fp = fopen("input.dat", "rb");

    fread(input_file, 1, M_512, fp);

    fclose(fp);

#ifdef EVAL_RTT
    char name[20];
    sprintf(name, "rtt_core_%d.txt", core);
    rtt_file = fopen(name, "wb");
    fseek(rtt_file, 0, SEEK_END);

    rtt_buff = (long long *)calloc(M_1, sizeof(long long));
    rtt_buff_len = 0;
#endif

	int * connect_socket = (int *)calloc(concurrency, sizeof(int));

	while (!done[core]) {
		gettimeofday(&cur_tv, NULL);

		while(num_connection < concurrency && num_connection < num_flow) {
            int ret;
			if ((ret = CreateConnection(ctx)) < 0) {
				done[core] = TRUE;
				break;
			}
        }

		if(!start_flag) {
			gettimeofday(&start, NULL);
			start_flag = 1;
		}

		nevents = mtcp_epoll_wait(mctx, ep, events, maxevents, -1);
	
		if (nevents < 0) {
			if (errno != EINTR) {
				fprintf(stderr,"mtcp_epoll_wait failed! ret: %d\n", nevents);
			}
			done[core] = TRUE;
			break;
		}

		for (i = 0; i < nevents; i++) {
            struct conn_stat * var = (struct conn_stat *)events[i].data.ptr;
			
			if (events[i].events & MTCP_EPOLLIN) {
				HandleReadEvent(ctx, var->sockfd, var);

			} else if (events[i].events == MTCP_EPOLLOUT) {
				HandleWriteEvent(ctx, var->sockfd, var);
			} else if (events[i].events & MTCP_EPOLLERR) {
#if 0			
				int err;
				socklen_t len = sizeof(err);

				fprintf(stdout,"[CPU %d] Error on socket %d\n", 
						core, var->sockfd);
				ctx->errors++;
				CloseConnection(ctx, var->sockfd);
#endif
			} else {
				fprintf(stdout,"Socket %d: event: %s\n", 
						var->sockfd, EventToString(events[i].events));
				assert(0);
			}
		}

		gettimeofday(&current, NULL);
		if(current.tv_sec - start.tv_sec >= execution_time) {
			fprintf(stdout, " [%s on core %d] Time's up! End %d connections\n", __func__, core, num_connection);
            for (int i = 0; i < num_connection; i++) {
				struct conn_stat * var = &ctx->stats[i];
	            if (!var->complete) {
    	            CloseConnection(ctx, var->sockfd);
					var->complete = 1;
            	}
			}
			done[core] = TRUE;
		}
	}

#ifdef EVAL_RTT
	printf(" >> Output Round Trip Time\n");
    for (int i = 0; i < rtt_buff_len; i++) {
        fprintf(rtt_file, "%llu\n", rtt_buff[i]);
        fflush(rtt_file);
    }

    fclose(rtt_file);
	printf(" >> Output finished\n");
	free(rtt_buff);
#endif
/*
	for (i = 0; i < num_cores; i++) {
		if (app_thread[i] != pthread_self()) {
			int kill_rc = pthread_kill(app_thread[i], 0);

			if (kill_rc == ESRCH) {
				printf(" [%s] the specified thread did not exists or already quit\n", __func__);
			}else if(kill_rc == EINVAL) {
				printf(" [%s] signal is invalid\n", __func__);
			}else{
				printf(" [%s] the specified thread is alive\n", __func__);
				int ret = pthread_kill(app_thread[i], SIGQUIT);
				if (ret == EINVAL) {
					printf(" [%s] Invalid signal\n", __func__);
				} else if (ret == ESRCH) {
					printf(" [%s] No thread os found\n", __func__);
				} else {
					printf(" [%s] succeed!\n", __func__);
					//pthread_kill(app_thread[i], SIGTERM);
				}
			}
		}
	}
*/

	//fprintf(stdout, " [%s on core %d] Exit client thread\n", __func__, core);
	mtcp_destroy_context(mctx);
	pthread_exit(NULL);

	return NULL;
}

void 
SignalHandler(int signum)
{
	int i;

	for (i = 0; i < core_limit; i++) {
		done[i] = TRUE;
	}
}

int main(int argc, char * argv[]){
    int ret;
	struct mtcp_conf mcfg;
    char *conf_file;
	int cores[MAX_CPUS];
	int process_cpu = -1;

	char conf_name[] = "client.conf";
	conf_file = conf_name;

    for (int i = 0; i < argc; i++){
        long long unsigned n;
        char junk;
		char s[20];
        if(sscanf(argv[i], "--num_cores=%llu", &n) == 1){
            num_cores = n;
			printf(" >> core num: %d\n", num_cores);
			if (num_cores > MAX_CPUS) {
				TRACE_CONFIG("CPU limit should be smaller than the "
					     "number of CPUs: %d\n", MAX_CPUS);
				return FALSE;
			}
			mtcp_getconf(&mcfg);
			mcfg.num_cores = num_cores;
			mtcp_setconf(&mcfg);
        }else if(sscanf(argv[i], "--num_flow=%llu%c", &n, &junk) == 1){
            num_flow = n; 
			printf(" >> flow num: %d\n", num_flow);
        }else if(sscanf(argv[i], "--size=%llu%c", &n, &junk) == 1){
            buff_size = n;
			printf(" >> buff size: %d\n", buff_size);
        }else if(sscanf(argv[i], "--time=%llu", &n) == 1){
            execution_time = n;
			printf(" >> total time of execution: %d\n", execution_time);
        }else if(sscanf(argv[i], "--server_ip=%s%c", server_ip, &junk) == 1) {
            printf(" >> server ip: %s\n", server_ip);
        }else if(sscanf(argv[i], "--server_port=%d%c", &server_port, &junk) == 1) {
            printf(" >> server port: %d\n", server_port);
        }else if(sscanf(argv[i], "--test_mode=%s%c", s, &junk)){
            if (!strcmp(s, "open")) {
                benchmark = OPENLOOP;
                printf(" >> running open loop test");
            } else if (!strcmp(s, "close")) {
                benchmark = CLOSELOOP;
                printf(" >> running close loop test");
            }
        }else if(i > 0){
            printf("error (%s)!\n", argv[i]);
        }
    }

	/* initialize mtcp */
	if (conf_file == NULL) {
		TRACE_CONFIG("You forgot to pass the mTCP startup config file!\n");
		exit(EXIT_FAILURE);
	}

	ret = mtcp_init(conf_file);
	if (ret) {
		TRACE_CONFIG("Failed to initialize mtcp\n");
		exit(EXIT_FAILURE);
	}

    mtcp_getconf(&mcfg);
	mcfg.max_concurrency = max_fds;
	mcfg.max_num_buffers = max_fds;
	mtcp_setconf(&mcfg);
	
	/* register signal handler to mtcp */
	mtcp_register_signal(SIGINT, SignalHandler);

	TRACE_INFO("Application initialization finished.\n");

	for (int i = ((process_cpu == -1) ? 0 : process_cpu); i < num_cores; i++) {
		cores[i] = i;
		done[i] = FALSE;
		
		if (pthread_create(&app_thread[i], 
				   NULL, RunClientThread, (void *)&cores[i])) {
			perror("pthread_create");
			TRACE_CONFIG("Failed to create server thread.\n");
				exit(EXIT_FAILURE);
		}
		if (process_cpu != -1)
			break;
	}
	
	for (int i = ((process_cpu == -1) ? 0 : process_cpu); i < num_cores; i++) {
		pthread_join(app_thread[i], NULL);

		if (process_cpu != -1)
			break;
	}

	printf(" [%s] Test finished!\n", __func__);
	
	mtcp_destroy();

	printf(" [%s] mTCP destroyed\n", __func__);

	return 0;
}