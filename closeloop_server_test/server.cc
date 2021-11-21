#include "server.h"

#include <rte_ethdev.h>

int finish_num = 0;
__thread int num_connection;
__thread struct thread_context *ctx;
__thread mctx_t mctx;

int execution_time;

__thread pthread_mutex_t log_lock;
__thread int record_complete = 0;
__thread int end_alarm = 0;

#define TIMEVAL_TO_USEC(t)  (double)((t).tv_sec * 1000000.00 + (t).tv_usec)

//#define TEST_INTERVAL

#ifdef TEST_INTERVAL
FILE * interval_file;
long long * interval_buff;
int interval_buff_len;

struct timeval last_interval;
int record_interval;
#endif

void CleanServerVariable(struct server_vars *sv){
	sv->recv_len = 0;
	sv->request_len = 0;
	sv->total_read = 0;
	sv->total_sent = 0;
	sv->done = 0;
	sv->rspheader_sent = 0;
	sv->keep_alive = 0;
	sv->total_time = 0;
//    sv->recv_buf = (struct ring_buf *)malloc(RING_BUF_SIZE);
//    init_ring_buff(sv->recv_buf);
	sv->scan_range = 4;
}

void CloseConnection(struct thread_context *ctx, int sockid, struct server_vars *sv){
	mtcp_epoll_ctl(ctx->mctx, ctx->ep, MTCP_EPOLL_CTL_DEL, sockid, NULL);
	mtcp_close(ctx->mctx, sockid);
	gettimeofday(&end, NULL);

	if(!end_alarm) {
		int end_alarm = mtcp_socket(ctx->mctx, AF_INET, SOCK_STREAM, 0);
		struct mtcp_epoll_event ev;
		ev.events = MTCP_EPOLLOUT;
		ev.data.sockid = end_alarm;
		mtcp_epoll_ctl(ctx->mctx, ctx->ep, MTCP_EPOLL_CTL_ADD, end_alarm, &ev);
	}
}

int HandleReadEvent(struct thread_context *ctx, int thread_id, int sockid, struct server_vars *sv){
	int len, sent, recv_len;
	len = sent = 0;

    char buff[buff_size + 1];

	len = mtcp_recv(ctx->mctx, sockid, buff, buff_size, 0);

	if(len == 0){
		return len;
	}

	recv_bytes += len;
	request++;

	if (benchmark == CLOSELOOP) {
		int send_len = mtcp_write(ctx->mctx, sockid, buff, len);

		if(send_len < 0) {
            return send_len;
        }

		send_bytes += send_len;
		reply++;
	}

    return len;
}

int AcceptConnection(struct thread_context *ctx, int listener){
	mctx_t mctx = ctx->mctx;
	struct server_vars *sv;
	struct mtcp_epoll_event ev;
	int c;

	c = mtcp_accept(mctx, listener, NULL, NULL);

	if (c >= 0) {
		if (c >= MAX_FLOW_NUM) {
			TRACE_ERROR("Invalid socket id %d.\n", c);
			return -1;
		}

		sv = &ctx->svars[c];
		CleanServerVariable(sv);
		TRACE_APP("New connection %d accepted.\n", c);
		ev.events = MTCP_EPOLLIN;
		ev.data.sockid = c;
		mtcp_setsock_nonblock(ctx->mctx, c);
		mtcp_epoll_ctl(mctx, ctx->ep, MTCP_EPOLL_CTL_ADD, c, &ev);
		TRACE_APP("Socket %d registered.\n", c);
		num_connection++;

	} else {
		if (errno != EAGAIN) {
			TRACE_ERROR("mtcp_accept() error %s\n", 
					strerror(errno));
		}
	}

	if(!established_flag) {
        gettimeofday(&start, NULL);
        gettimeofday(&log_start, NULL);
        established_flag = 1;
		for (int i = 0; i < num_cores; i++) {
			if (app_thread[i] == pthread_self()) {
				task_start[i] = TRUE;
			}
		}
    }

	return c;
}

struct thread_context * InitializeServerThread(int core){
	struct thread_context *ctx;

	/* affinitize application thread to a CPU core */
#if HT_SUPPORT
	mtcp_core_affinitize(core + (num_cores / 2));
#else
	mtcp_core_affinitize(core + 32);
#endif /* HT_SUPPORT */

	ctx = (struct thread_context *)calloc(1, sizeof(struct thread_context));
	if (!ctx) {
		TRACE_ERROR("Failed to create thread context!\n");
		return NULL;
	}

	/* create mtcp context: this will spawn an mtcp thread */
	ctx->mctx = mtcp_create_context(core);
	if (!ctx->mctx) {
		TRACE_ERROR("Failed to create mtcp context!\n");
		free(ctx);
		return NULL;
	}

	/* create epoll descriptor */
	ctx->ep = mtcp_epoll_create(ctx->mctx, MAX_EVENTS);
	if (ctx->ep < 0) {
		mtcp_destroy_context(ctx->mctx);
		free(ctx);
		TRACE_ERROR("Failed to create epoll descriptor!\n");
		return NULL;
	}

	/* allocate memory for server variables */
	ctx->svars = (struct server_vars *)
			calloc(MAX_FLOW_NUM, sizeof(struct server_vars));
	if (!ctx->svars) {
		mtcp_close(ctx->mctx, ctx->ep);
		mtcp_destroy_context(ctx->mctx);
		free(ctx);
		TRACE_ERROR("Failed to create server_vars struct!\n");
		return NULL;
	}

	return ctx;
}

int CreateListeningSocket(struct thread_context *ctx){
	int listener;
	struct mtcp_epoll_event ev;
	struct sockaddr_in saddr;
	int ret;

	/* create socket and set it as nonblocking */
	listener = mtcp_socket(ctx->mctx, AF_INET, SOCK_STREAM, 0);
	if (listener < 0) {
		TRACE_ERROR("Failed to create listening socket!\n");
		return -1;
	}
	ret = mtcp_setsock_nonblock(ctx->mctx, listener);
	if (ret < 0) {
		TRACE_ERROR("Failed to set socket in nonblocking mode.\n");
		return -1;
	}

	/* bind to port 80 */
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = INADDR_ANY;
	saddr.sin_port = htons(80);
	ret = mtcp_bind(ctx->mctx, listener, 
			(struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
	if (ret < 0) {
		TRACE_ERROR("Failed to bind to the listening socket!\n");
		return -1;
	}
	
	/* listen (backlog: can be configured) */
	ret = mtcp_listen(ctx->mctx, listener, backlog);
	if (ret < 0) {
		TRACE_ERROR("mtcp_listen() failed!\n");
		return -1;
	}
	
	/* wait for incoming accept events */
	ev.events = MTCP_EPOLLIN;
	ev.data.sockid = listener;
	mtcp_epoll_ctl(ctx->mctx, ctx->ep, MTCP_EPOLL_CTL_ADD, listener, &ev);

	return listener;
}

void
ServerSignalHandler(int signum) {
	if (signum == SIGQUIT) {
		printf("========== Clean up ==========\n");
		if (pthread_mutex_trylock(&log_lock) == 0) {
			if (!record_complete) {
				record_complete = 1;
				gettimeofday(&end, NULL);
		
		    	double start_time = (double)start.tv_sec * 1000000 + (double)start.tv_usec;
		    	double end_time = (double)end.tv_sec * 1000000 + (double)end.tv_usec;
    			double total_time = (end_time - start_time)/1000000.00;
		
				int core;

				for (int i = 0; i < num_cores; i++) {
					if (app_thread[i] == pthread_self()) {
						core = i;
					}
				}

				done[core] = true;

			    char result_buff[512];

				char throughput_file_name[32];
				sprintf(throughput_file_name, "throughput_core_%d.txt", core);

				FILE * throughput_file = fopen(throughput_file_name, "a+");

			    sprintf(result_buff, " [%d] recv payload rate: %.2f(Mbps), recv request rate: %.2f, send payload rate: %.2f(Mbps), send reply rate: %.2f\n", 
    			                num_connection, (recv_bytes * 8.0) / (total_time * 1000 * 1000), request / (total_time * 1000), 
        			            (send_bytes * 8.0) / (total_time * 1000 * 1000), reply / (total_time * 1000));
	
				printf("%s", result_buff);

				fprintf(throughput_file, "%s", result_buff);

				fclose(throughput_file);

				int socket = mtcp_socket(ctx->mctx, AF_INET, SOCK_STREAM, 0);
				struct mtcp_epoll_event ev;
				ev.events = MTCP_EPOLLOUT;
				ev.data.sockid = socket;
				mtcp_epoll_ctl(ctx->mctx, ctx->ep, MTCP_EPOLL_CTL_ADD, socket, &ev);

				printf(" >> receive SIGQUIT signal\n");

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

				sleep(5);
	
				mtcp_destroy_context(mctx);
				//pthread_exit(NULL);

			}
			pthread_mutex_unlock(&log_lock);
		}
	}	
}

void * RunServerThread(void *arg){
//	int core = *(int *)arg;
	signal(SIGQUIT, ServerSignalHandler);

	pthread_mutex_init(&log_lock, NULL);

	struct server_arg * args = (struct server_arg *)arg;

	int core = args->core;
	int thread_id = args->thread_id;

	int listener;
	int ep;
	struct mtcp_epoll_event *events;
	int nevents;
	int i, ret;
	int do_accept;
	
	/* initialization */
	ctx = InitializeServerThread(core);
	if (!ctx) {
		TRACE_ERROR("Failed to initialize server thread.\n");
		return NULL;
	}
	mctx = ctx->mctx;
	ep = ctx->ep;

	events = (struct mtcp_epoll_event *)
			calloc(MAX_EVENTS, sizeof(struct mtcp_epoll_event));
	if (!events) {
		TRACE_ERROR("Failed to create event struct!\n");
		exit(-1);
	}

	listener = CreateListeningSocket(ctx);
	if (listener < 0) {
		TRACE_ERROR("Failed to create listening socket.\n");
		exit(-1);
	}
#ifdef TEST_INTERVAL
    interval_file = fopen("epoll_interval.txt", "wb");
    fseek(interval_file, 0, SEEK_END);

    interval_buff = (long long *)calloc(M_128, sizeof(long long));
    interval_buff_len = 0;
#endif
    printf("========== Start to wait events ==========\n");

	while (!done[core]) {
		nevents = mtcp_epoll_wait(mctx, ep, events, MAX_EVENTS, -1);
		if (nevents < 0) {
			if (errno != EINTR)
				perror("mtcp_epoll_wait");
			break;
		}

		if (done[core]) {
			break;
		}
		
#ifdef TEST_INTERVAL
        if (!record_interval) {
            gettimeofday(&last_interval, NULL);
            record_interval = 1;
        } else {
            struct timeval current;
            gettimeofday(&current, NULL);

            double interval = TIMEVAL_TO_USEC(current) - TIMEVAL_TO_USEC(last_interval);
            interval_buff[interval_buff_len++] = interval;
            gettimeofday(&last_interval, NULL);
        }

#endif

		do_accept = FALSE;
		for (i = 0; i < nevents; i++) {

			if (events[i].data.sockid == listener) {
				/* if the event is for the listener, accept connection */
				do_accept = TRUE;
			} else if (events[i].events & MTCP_EPOLLERR) {
#if 0
				int err;
				socklen_t len = sizeof(err);

				/* error on the connection */
				fprintf(stdout, " [%s on core %d] error on socket %d\n", __func__, core, events[i].data.sockid);
				if (mtcp_getsockopt(mctx, events[i].data.sockid, 
						SOL_SOCKET, SO_ERROR, (void *)&err, &len) == 0) {
					if (err != ETIMEDOUT) {
						fprintf(stderr, "Error on socket %d: %s\n", 
								events[i].data.sockid, strerror(err));
					}
				} else {
					perror("mtcp_getsockopt");
				}
				CloseConnection(ctx, events[i].data.sockid, 
						&ctx->svars[events[i].data.sockid]);
#endif
			} else if (events[i].events & MTCP_EPOLLIN) {
				ret = HandleReadEvent(ctx, thread_id, events[i].data.sockid, 
						&ctx->svars[events[i].data.sockid]);

				if (ret == 0) {
					/* connection closed by remote host */
					CloseConnection(ctx, events[i].data.sockid, 
							&ctx->svars[events[i].data.sockid]);
					finish_num++;
    	            if (finish_num == num_connection) {
            	        done[core] = 1;
                	}
/*
					for (i = 0; i < num_cores; i++) {
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
									sleep(1);
									//pthread_kill(app_thread[i], SIGTERM);
								}
							}
						}
					}
*/
				} else if (ret < 0) {
					/* if not EAGAIN, it's an error */
					if (errno != EAGAIN) {
						CloseConnection(ctx, events[i].data.sockid, 
								&ctx->svars[events[i].data.sockid]);
						finish_num++;
	    	            if (finish_num == num_connection) {
    	        	        done[core] = 1;
        	        	}
					}
				}

			} else {
				assert(0);
			}
		}

		/* if do_accept flag is set, accept connections */
		if (do_accept) {
			while (1) {
				ret = AcceptConnection(ctx, listener);
				if (ret < 0)
					break;
			}
		}

		struct timeval current;
		gettimeofday(&current, NULL);
		if(current.tv_sec - start.tv_sec >= execution_time + 5) {
			fprintf(stdout, " [%s] Time's up! End connections\n", __func__);
            CloseConnection(ctx, listener, &ctx->svars[listener]);
            done[core] = TRUE;
		}
		
	}

	printf("========== Clean up ==========\n");
    
    double start_time = (double)start.tv_sec * 1000000 + (double)start.tv_usec;
    double end_time = (double)end.tv_sec * 1000000 + (double)end.tv_usec;
    double total_time = (end_time - start_time)/1000000.00;

	char result_buff[512];

	char throughput_file_name[32];
	sprintf(throughput_file_name, "throughput_core_%d.txt", core);

	if (pthread_mutex_trylock(&log_lock) == 0) {
		if (!record_complete) {
			record_complete = 1;
			FILE * throughput_file = fopen(throughput_file_name, "a+");

		    sprintf(result_buff, " [%d] recv payload rate: %.2f(Mbps), recv request rate: %.2f, send payload rate: %.2f(Mbps), send reply rate: %.2f\n", 
    			                num_connection, (recv_bytes * 8.0) / (total_time * 1000 * 1000), request / (total_time * 1000), 
        			            (send_bytes * 8.0) / (total_time * 1000 * 1000), reply / (total_time * 1000));
		
			printf("%s", result_buff);

			fprintf(throughput_file, "%s", result_buff);
	
			fclose(throughput_file);
		}
		pthread_mutex_unlock(&log_lock);
	}
/*	
	for (i = 0; i < num_cores; i++) {
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
					sleep(1);
					//pthread_kill(app_thread[i], SIGTERM);
				}
			}
		}
	}
*/
	sleep(10);
	/* destroy mtcp context: this will kill the mtcp thread */
	mtcp_destroy_context(ctx->mctx);
	pthread_exit(NULL);

	return NULL;
}

void SignalHandler(int signum){
	int i;

	for (i = 0; i < num_cores; i++) {
		if (app_thread[i] == pthread_self()) {
			//TRACE_INFO("Server thread %d got SIGINT\n", i);
			done[i] = TRUE;
		} else {
			if (!done[i]) {
				printf(" >> kill current thread\n");
				pthread_kill(app_thread[i], SIGQUIT);
			}
		}
	}
}

int main(int argc, char **argv){
	int ret;
	struct mtcp_conf mcfg;
	int cores[MAX_CPUS];
	int process_cpu;

	total_cores = sysconf(_SC_NPROCESSORS_ONLN);
	num_cores = total_cores;
	process_cpu = -1;

	char conf_name[] = "server.conf";
	conf_file = conf_name;

	if (argc < 2) {
		TRACE_CONFIG("$%s directory_to_service\n", argv[0]);
		return FALSE;
	}

	char s[20];

    for (int i = 0; i < argc; i++){
        long long unsigned n;
        char junk;
        if(sscanf(argv[i], "--num_cores=%llu%c", &n, &junk) == 1){
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
        }else if(sscanf(argv[i], "--size=%llu%c", &n, &junk) == 1){
            buff_size = n;
			printf(" >> buff size: %d\n", buff_size);
        }else if(sscanf(argv[i], "--time=%llu%c", &n, &junk) == 1){
            execution_time = n;
			printf(" >> total time of execution: %d\n", execution_time);
        }else if(sscanf(argv[i], "--test_mode=%s%c", s, &junk) == 1){
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
	if (backlog > mcfg.max_concurrency) {
		TRACE_CONFIG("backlog can not be set larger than CONFIG.max_concurrency\n");
		return FALSE;
	}

	/* if backlog is not specified, set it to 4K */
	if (backlog == -1) {
		backlog = 4096;
	}
	
	/* register signal handler to mtcp */
	mtcp_register_signal(SIGINT, SignalHandler);

	TRACE_INFO("Application initialization finished.\n");

	for (int i = ((process_cpu == -1) ? 0 : process_cpu); i < num_cores; i++) {
		cores[i] = i;
		done[i] = FALSE;
		sv_thread_arg[i].core = i;
        sv_thread_arg[i].thread_id = i;
		
		if (pthread_create(&app_thread[i], 
				   NULL, RunServerThread, (void *)&sv_thread_arg[i])) {
			perror("pthread_create");
			TRACE_CONFIG("Failed to create server thread.\n");
				exit(EXIT_FAILURE);
		}
		if (process_cpu != -1)
			break;
	}

	for (int i = ((process_cpu == -1) ? 0 : process_cpu); i < num_cores; i++) {
		pthread_join(app_thread[i], NULL);

		if (process_cpu != -1) {
			break;
		}
	}
	
	printf(" [%s] Test finished!\n", __func__);
	
	return 0;
}
