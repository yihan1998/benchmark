//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/parse.h"
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"

#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <getopt.h>

#include <sys/epoll.h>
#include <sys/timerfd.h>

#define MAX_CONNECT 2100
#define MAX_EVENTS  8192

static char * conf_file = NULL;

__thread int num_conn = 0;

__thread int num_cores = 0;

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

void SignalHandler(int signum){
	int i;

    if (signum == SIGINT) {
        for (i = 0; i < num_cores; i++) {
            if (cl_thread[i] == pthread_self()) {
                //TRACE_INFO("Server thread %d got SIGINT\n", i);
                pthread_kill(cl_thread[i], SIGTERM);
            }
        }
    } else if (signum == SIGTERM) {
        exit(0);
    }
}

double LoadRecord(struct thread_context * ctx, struct mtcp_epoll_event * events, ycsbc::Client &client, const int num_record_ops, const int num_operation_ops, const int port, const int num_flows) {
    int record_per_flow = num_record_ops / num_flows;
    int operation_per_flow = num_operation_ops / num_flows;

    int done = 0;

    int nevents;

    int num_load_complete = 0;

    utils::Timer<double> timer;
    double duration;

    timer.Start();

    while(!done) {
        while(num_conn < num_flows) {
            /* Connect server */
            int sock;
            if ((sock = client.ConnectServer(ctx->mctx, "10.0.0.1", port)) > 0) {
                // fprintf(stdout, " [%s] connect server through sock %d\n", __func__, sock);
                struct conn_info * conn_info = &ctx->info[num_conn];
                conn_info->sockfd = sock;
                conn_info->epfd = ctx->epfd;

                conn_info->ibuf = (char *)calloc(16, 1024);
                conn_info->ioff = 0;

                conn_info->obuf = (char *)calloc(16, 1024);
                conn_info->ooff = 0;

                conn_info->total_record_ops = record_per_flow;
                conn_info->total_operation_ops = operation_per_flow;

                conn_info->actual_record_ops = conn_info->actual_operation_ops = 0;

                num_conn++;

                struct mtcp_epoll_event ev;
                ev.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
                ev.data.ptr = conn_info;
            	mtcp_epoll_ctl(ctx->mctx, ctx->epfd, MTCP_EPOLL_CTL_ADD, sock, &ev);
            } else {
                fprintf(stderr, " [%s] connect server failed!", __func__);
                exit(1);
            }
        }

        nevents = mtcp_epoll_wait(ctx->mctx, ctx->epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & MTCP_EPOLLERR)) {
                client.HandleErrorEvent(ctx->mctx, info);
            }
            
            if ((events[i].events & MTCP_EPOLLIN)) {
                int len = mtcp_read(ctx->mctx, info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);

                if (len > 0) {
                    info->ioff += len;
                }

                if (strchr(info->ibuf,'\n')) {
                    /* Increase actual ops */
                    // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                    if(++info->actual_record_ops == info->total_record_ops) {
                        // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                        if (++num_load_complete == num_conn) {
                            done = 1;
                        }
                    }
                    
                    struct mtcp_epoll_event ev;
                    ev.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
                    ev.data.ptr = info;

            	    mtcp_epoll_ctl(ctx->mctx, ctx->epfd, MTCP_EPOLL_CTL_MOD, info->sockfd, &ev);
                }
            } else if ((events[i].events & MTCP_EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client.InsertRecord(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = mtcp_write(ctx->mctx, info->sockfd, info->obuf + info->ooff, info->oremain);
            
                if(len > 0) {
                    // fprintf(stdout, " [sock %d] # Next record :\t %lld\n", info->sockfd, info->actual_record_ops);  
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct mtcp_epoll_event ev;
                        ev.events = MTCP_EPOLLIN;
                        ev.data.ptr = info;
                        mtcp_epoll_ctl(ctx->mctx, ctx->epfd, MTCP_EPOLL_CTL_MOD, info->sockfd, &ev);
                    }
                }
            } else {
                printf(" >> unknown event!\n");
            }
        }
    }

    duration = timer.End();

    for (int i = 0; i < num_conn; i++) {
        struct mtcp_epoll_event ev;
        ev.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
        ev.data.ptr = &ctx->info[i];

        mtcp_epoll_ctl(ctx->mctx, ctx->epfd, MTCP_EPOLL_CTL_MOD, ctx->info[i].sockfd, &ev);
    }
    
    return duration;
}

double PerformTransaction(struct thread_context * ctx, struct mtcp_epoll_event * events, ycsbc::Client &client) {
    int done = 0;

    utils::Timer<double> timer;
    double duration;

    int num_transaction_complete = 0;

    int nevents;

    int oks = 0;

    timer.Start();

    while(!done) {
		nevents = mtcp_epoll_wait(ctx->mctx, ctx->epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & MTCP_EPOLLERR)) {
                client.HandleErrorEvent(ctx->mctx, info);
                if (++num_transaction_complete == num_conn) {
                    done = 1;
                }
                mtcp_close(ctx->mctx, info->sockfd);
                continue;
            }
            
            if ((events[i].events & MTCP_EPOLLIN)) {
                int len = mtcp_read(ctx->mctx, info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);

                if (ret > 0) {
                    info->ioff += len;
                    fprintf(stdout, " >> recv : %s", info->ibuf);
                }

                struct reply * reply = getReply(info->ibuf, info->ioff);
                if (!reply) {
                    continue;
                }

                client.ReceiveReply(info->ibuf);

                info->ioff = 0;
                oks++;

                /* Increase actual ops */
                if(++info->actual_operation_ops == info->total_operation_ops) {                    // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                    if (++num_transaction_complete == num_conn) {
                        done = 1;
                    }
                }

                freeReply(reply);

                struct mtcp_epoll_event ev;
                ev.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
                ev.data.ptr = info;

                mtcp_epoll_ctl(ctx->mctx, ctx->epfd, MTCP_EPOLL_CTL_MOD, info->sockfd, &ev);
            } else if ((events[i].events & MTCP_EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client.SendRequest(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = mtcp_write(ctx->mctx, info->sockfd, info->obuf + info->ooff, info->oremain);
                
                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct mtcp_epoll_event ev;
                        ev.events = MTCP_EPOLLIN;
                        ev.data.ptr = info;

                        mtcp_epoll_ctl(ctx->mctx, ctx->epfd, MTCP_EPOLL_CTL_MOD, info->sockfd, &ev);
                    }
                }
            } else {
                fprintf(stderr, " unknown event (%d)!\n", events[i].events);
            }

        }
    }

    fprintf(stderr, " # Transaction: %llu\n", oks);

    duration = timer.End();

    char output[256];

    // char output_file_name[32];
	// sprintf(output_file_name, "throughput_core_%d.txt", ctx->core_id);
	// FILE * output_file = fopen(output_file_name, "a+");

    sprintf(output, " [core %d] # Transaction throughput : %.2f (KTPS) \t %s \t %d\n", \
                ctx->core_id, oks / duration / 1000, (*ctx->props)["file"].c_str(), num_conn);

    fprintf(stderr, "%s", output);
    fflush(stderr);

    // fprintf(output_file, "%s", output);
	// fclose(output_file);

    return duration;
}

struct thread_context * InitializeClientThread(int core){
	struct thread_context * ctx;
    
	mtcp_register_signal(SIGINT, SignalHandler);

	/* affinitize application thread to a CPU core */
	mtcp_core_affinitize(core);

	ctx = (struct thread_context *)calloc(1, sizeof(struct thread_context));
	if (!ctx) {
		fprintf(stderr, "Failed to create thread context!\n");
		return NULL;
	}

	/* create mtcp context: this will spawn an mtcp thread */
	ctx->mctx = mtcp_create_context(core);
	if (!ctx->mctx) {
		fprintf(stderr, "Failed to create mtcp context!\n");
		free(ctx);
		return NULL;
	}

	/* create epoll descriptor */
	ctx->epfd = mtcp_epoll_create(ctx->mctx, MAX_EVENTS);
	if (ctx->epfd < 0) {
		mtcp_destroy_context(ctx->mctx);
		free(ctx);
		fprintf(stderr, "Failed to create epoll descriptor!\n");
		return NULL;
	}

	/* Initialize connection info array */
    ctx->info = (struct conn_info *)calloc(MAX_CONNECT, sizeof(struct conn_info));

	return ctx;
}

void * RunClientThread(void * arg) {
    struct client_arg * carg = (struct client_arg *)arg;

    int core = carg->core;
    utils::Properties * props = carg->props;

	int i, ret;

    /* mTCP initialization */
    struct thread_context * ctx;
	ctx = InitializeClientThread(core);
	if (!ctx) {
		fprintf(stderr, "Failed to initialize server thread.\n");
		return NULL;
	}

    ctx->props = props;
    ctx->core_id = core;

    int epfd;
    struct mtcp_epoll_event * events;

    epfd = ctx->epfd;
    /* Initialize epoll event array */
    events = (struct mtcp_epoll_event *)calloc(MAX_EVENTS, sizeof(struct mtcp_epoll_event));

    ycsbc::CoreWorkload wl;
    wl.Init(*props);

    const int num_flows = stoi(props->GetProperty("flows", "1"));

    int record_total_ops = stoi((*props)[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    int operation_total_ops = stoi((*props)[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    fprintf(stdout, " [core %d] # Total records (K) :\t %.2f \n", core, (double)record_total_ops / 1000.0);  
    fprintf(stdout, " [core %d] # Total transactions (K) :\t %.2f\n", core, (double)operation_total_ops / 1000.0);  

    // double duration = DelegateClient(db, &wl, record_total_ops, operation_total_ops, num_flows);

    ycsbc::Client client(wl);
    
    int port = stoi(props->GetProperty("port", "6379"));

    double load_duration = 0.0;
    load_duration = LoadRecord(ctx, events, client, record_total_ops, operation_total_ops, port, num_flows);

    fprintf(stdout, " [core %d] loaded records done! \n", core);  

    double transaction_duration = 0.0;
    transaction_duration = PerformTransaction(ctx, events, client);

    mtcp_destroy_context(ctx->mctx);
	pthread_exit(NULL);

    return NULL;
}

int main(const int argc, const char *argv[]) {
    int ret;
	struct mtcp_conf mcfg;
	int process_cpu = -1;

	char conf_name[] = "client.conf";
	conf_file = conf_name;

    string filename;
    ifstream input;

    utils::Properties props;
	char s[20];

    for (int i = 0; i < argc; i++){
        long long unsigned n;
        char junk;
		char s[20];
        if(sscanf(argv[i], "--num_cores=%llu", &n) == 1){
            num_cores = n;
			printf(" >> core num: %d\n", num_cores);
			if (num_cores > MAX_CPUS) {
				fprintf(stdout, "CPU limit should be smaller than the "
					     "number of CPUs: %d\n", MAX_CPUS);
				return 0;
			}
			mtcp_getconf(&mcfg);
			mcfg.num_cores = num_cores;
			mtcp_setconf(&mcfg);
        } else if (sscanf(argv[i], "--flows=%s\n", s, &junk) == 1){
            props.SetProperty("flows", s);
            std::cout << " Flows: " << props["flows"].c_str() << std::endl;
        } else if (sscanf(argv[i], "--port=%s\n", s, &junk) == 1) {
            props.SetProperty("port", s);
            std::cout << " Port: " << props["port"].c_str() << std::endl;
        } else if (sscanf(argv[i], "--workload=%s\n", s, &junk) == 1) {
            filename.assign(s);
            props.SetProperty("file", filename);
            std::cout << " Workload file: " << props["file"].c_str() << std::endl;
            input.open(filename);
            try {
                props.Load(input);
            } catch (const string &message) {
                cout << message << endl;
                exit(0);
            }
            input.close();
        }
    }

	/* initialize mtcp */
	if (conf_file == NULL) {
		fprintf(stdout, "You forgot to pass the mTCP startup config file!\n");
		exit(EXIT_FAILURE);
	}

	ret = mtcp_init(conf_file);
	if (ret) {
		fprintf(stdout, "Failed to initialize mtcp\n");
		exit(EXIT_FAILURE);
	}

    mtcp_getconf(&mcfg);
	
	/* register signal handler to mtcp */
	mtcp_register_signal(SIGINT, SignalHandler);
	mtcp_register_signal(SIGTERM, SignalHandler);

	fprintf(stdout, "Application initialization finished.\n");

	for (int i = ((process_cpu == -1) ? 0 : process_cpu); i < num_cores; i++) {
		cl_thread_arg[i].core = i;
        cl_thread_arg[i].props = &props;

		if (pthread_create(&cl_thread[i], NULL, RunClientThread, (void *)&cl_thread_arg[i])) {
			perror("pthread_create");
		}
		if (process_cpu != -1)
			break;
	}
	
	for (int i = ((process_cpu == -1) ? 0 : process_cpu); i < num_cores; i++) {
		pthread_join(cl_thread[i], NULL);

		if (process_cpu != -1)
			break;
	}

	printf(" [%s] Test finished!\n", __func__);
    sleep(10);

	return 0;
}

void UsageMessage(const char *command) {
    cout << "Usage: " << command << " [options]" << endl;
    cout << "Options:" << endl;
    cout << "  -threads n: execute using n threads (default: 1)" << endl;
    cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
    cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
    cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
    return strncmp(str, pre, strlen(pre)) == 0;
}

