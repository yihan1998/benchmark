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
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"

#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <getopt.h>

#include <sys/epoll.h>
#include <sys/timerfd.h>

#define MAX_CONNECT 4100
#define MAX_EVENTS  8192

using namespace std;

__thread int num_conn = 0;
__thread int num_cores = 0;
__thread struct conn_info * info;

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
            if ((sock = client.ConnectServer(ctx->mctx, "10.0.1.1", port)) > 0) {
                // fprintf(stdout, " [%s] connect server through sock %d\n", __func__, sock);
                struct conn_info * conn_info = &info[num_conn];
                conn_info->sockfd = sock;
                conn_info->epfd = epfd;

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
            	mtcp_epoll_ctl(ctx->mctx, info->epfd, MTCP_EPOLL_CTL_ADD, sock, &ev);
            } else {
                fprintf(stderr, " [%s] connect server failed!", __func__);
                exit(1);
            }
        }

        nevents = mtcp_epoll_wait(ctx->mctx, ctx->epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & EPOLLERR)) {
                client.HandleErrorEvent(info);
            }
            
            if ((events[i].events & EPOLLIN)) {
                int len = mtcp_read(ctx->mctx, info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);

                if (len > 0) {
                    info->ioff += len;
                }

                if (strchr(info->ibuf,'\n')) {
                    // printf(" [%s:%d] receive reply: %s", __func__, __LINE__, info->ibuf);

                    client.ReceiveReply(info->ibuf);

                    info->ioff = 0;

                    /* Increase actual ops */
                    if(++info->actual_record_ops == info->total_record_ops) {
                        // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                        // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                        if (++num_load_complete == num_conn) {
                            done = 1;
                        }
                    }
                    
                    struct mtcp_epoll_event ev;
                    ev.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
                    ev.data.ptr = info;

                    mtcp_epoll_ctl(ctx->mctx, info->epfd, MTCP_EPOLL_CTL_MOD, info->sockfd, &ev);
                }
            } else if ((events[i].events & EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client.InsertRecord(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = mtcp_write(ctx->mctx, info->sockfd, info->obuf + info->ooff, info->oremain, 0);
                
                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct epoll_event ev;
                        ev.events = EPOLLIN;
                        ev.data.ptr = info;

                        mtcp_epoll_ctl(ctx->mctx, info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
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

        mtcp_epoll_ctl(ctx->mctx, info->epfd, MTCP_EPOLL_CTL_MOD, ctx->info[i].sockfd, &ev);
    }
    
    return duration;
}

double PerformTransaction(int epfd, struct epoll_event * events, ycsbc::Client &client) {
    int done = 0;

    utils::Timer<double> timer;
    double duration;

    int num_transaction_complete = 0;

    int nevents;

    int oks = 0;

    timer.Start();

    while(!done) {
        nevents = mtcp_epoll_wait(ctx->mctx, epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & EPOLLERR)) {
                client.HandleErrorEvent(info);
                if (++num_transaction_complete == num_conn) {
                    done = 1;
                }
                mtcp_close(ctx->mctx, info->sockfd);
                continue;
            }
            
            if ((events[i].events & MTCP_EPOLLIN)) {
                int len = mtcp_read(ctx->mctx, info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);
                // printf(" [%s:%d] receive len: %d, ioff: %d\n", __func__, __LINE__, len, info->ioff);

                if (len > 0) {
                    info->ioff += len;
                }

                int to_recv = 0;
                char recv_buff[1024*16];
                // if (sscanf(info->ibuf, "$%d\r\n%s\r\n", &to_recv, recv_buff) == 2) {
                if (sscanf(info->ibuf, "$%d\r\n", &to_recv) == 1) {
                    // printf(" [%s:%d] to receive len: %d\n", __func__, __LINE__, to_recv);
                    if (to_recv != -1) {
                        if (sscanf(info->ibuf, "$%*d\r\n%s\r\n", recv_buff) == 1) {
                            // printf(" [%s:%d] received len: %d, %.*s\n", __func__, __LINE__, strlen(recv_buff), strlen(recv_buff), recv_buff);
                            if (to_recv != strlen(recv_buff)) {
                                continue;
                            }
                        }
                    }
                }
                
                // printf(" [%s:%d] receive reply: %s", __func__, __LINE__, info->ibuf);

                client.ReceiveReply(info->ibuf);

                info->ioff = 0;
                oks++;

                /* Increase actual ops */
                if(++info->actual_operation_ops == info->total_operation_ops) {
                    // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                    // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                    if (++num_transaction_complete == num_conn) {
                        done = 1;
                    }
                }
                
                struct mtcp_epoll_event ev;
                ev.events = MTCP_EPOLLIN | MTCP_EPOLLOUT;
                ev.data.ptr = info;

                mtcp_epoll_ctl(ctx->mctx, info->epfd, MTCP_EPOLL_CTL_ADD, sock, &ev);

            } else if ((events[i].events & EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client.SendRequest(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = mtcp_write(ctx->mctx, info->sockfd, info->obuf + info->ooff, info->oremain, 0);
                // printf(" [%s:%d] send len: %d\n", __func__, __LINE__, len);

                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct mtcp_epoll_event ev;
                        ev.events = MTCP_EPOLLIN;
                        ev.data.ptr = info;

                        mtcp_epoll_ctl(ctx->mctx, info->epfd, MTCP_EPOLL_CTL_MOD, info->sockfd, &ev);
                    }
                }
            } else {
                printf(" >> unknown event!\n");
            }

        }
    }

    fprintf(stdout, " # Transaction: %llu\n", oks);

    duration = timer.End();
    return duration;
}

int main(const int argc, const char *argv[]) {
    int ret;
    ret = mtcp_init("client.conf");
	if (ret) {
		fprintf(stdout, "Failed to initialize mtcp\n");
		exit(1);
	}

	struct mtcp_conf mcfg;
    mtcp_getconf(&mcfg);
    mcfg.num_cores = 1;
    mtcp_setconf(&mcfg);

    utils::Properties props;
    string file_name = ParseCommandLine(argc, argv, props);
    cout << " Test workload: " << file_name << endl;

    int core_id = atoi(props.GetProperty("core_id", "0").c_str());
	mtcp_core_affinitize(core_id);

	ctx->mctx = mtcp_create_context(core);

    ycsbc::CoreWorkload wl;
    wl.Init(props);

    const int num_flows = stoi(props.GetProperty("flows", "1"));

    int record_total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    int operation_total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    fprintf(stdout, " [core %d] # Total records (K) :\t %.2f \n", core_id, (double)record_total_ops / 1000.0);  
    fprintf(stdout, " [core %d] # Total transactions (K) :\t %.2f\n", core_id, (double)operation_total_ops / 1000.0);  

    // double duration = DelegateClient(db, &wl, record_total_ops, operation_total_ops, num_flows);

    ycsbc::Client client(wl);

    /* Initialize connection info array */
    info = (struct conn_info *)calloc(MAX_CONNECT, sizeof(struct conn_info));

    int epfd;
    struct epoll_event * events;

    /* Create epoll fd */
	ctx->ep = mtcp_epoll_create(ctx->mctx, MAX_EVENTS);
    
    epfd = ctx->ep;

    /* Initialize epoll event array */
    events = (struct epoll_event *)calloc(MAX_EVENTS, sizeof(struct epoll_event));

    int port = stoi(props.GetProperty("port", "6379"));

    double load_duration = 0.0;
    load_duration = LoadRecord(epfd, events, client, record_total_ops, operation_total_ops, port, num_flows);

    fprintf(stdout, " [core %d] loaded records done! \n", core_id);  

    double transaction_duration = 0.0;
    transaction_duration = PerformTransaction(epfd, events, client);

    char output[256];

    char output_file_name[32];
	sprintf(output_file_name, "throughput_core_%d.txt", core_id);

	FILE * output_file = fopen(output_file_name, "a+");
    if (!output_file) {
        perror("Failed to open output file");
    }
    
    sprintf(output, " [core %d] # Transaction throughput : %.2f (KTPS) \t %s \t %d\n", \
                    core_id, operation_total_ops / transaction_duration / 1000, \
                    file_name.c_str(), num_flows);

    fprintf(stdout, "%s", output);
    fflush(stdout);

    fprintf(output_file, "%s", output);
	fclose(output_file);

    return 0;
}

// string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
//     int argindex = 1;
//     string filename;
//     while (argindex < argc && StrStartWith(argv[argindex], "-")) {
//         if (strcmp(argv[argindex], "-flows") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             props.SetProperty("flows", argv[argindex]);
//             argindex++;
//         } else if (strcmp(argv[argindex], "-db") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             props.SetProperty("dbname", argv[argindex]);
//             argindex++;
//         } else if (strcmp(argv[argindex], "-host") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             props.SetProperty("host", argv[argindex]);
//             argindex++;
//         } else if (strcmp(argv[argindex], "-port") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             props.SetProperty("port", argv[argindex]);
//             argindex++;
//         } else if (strcmp(argv[argindex], "-slaves") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             props.SetProperty("slaves", argv[argindex]);
//             argindex++;
//         } else if (strcmp(argv[argindex], "-P") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             filename.assign(argv[argindex]);
//             ifstream input(argv[argindex]);
//             try {
//                 props.Load(input);
//             } catch (const string &message) {
//                 cout << message << endl;
//                 exit(0);
//             }
//             input.close();
//             argindex++;
//         } else {
//             cout << "Unknown option '" << argv[argindex] << "'" << endl;
//             exit(0);
//         }
//     }

//     if (argindex == 1 || argindex != argc) {
//         UsageMessage(argv[0]);
//         exit(0);
//     }

//     return filename;
// }

enum cfg_params {
    PORT,
    WORKLOAD,
    FLOWS,
    CORE_ID,
};

const struct option options[] = {
    {   .name = "port", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = PORT},
    {   .name = "workload", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = WORKLOAD},
    {   .name = "flows", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = FLOWS},
    {   .name = "core_id", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = CORE_ID},
};

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
    int ret, done = 0;
    char * end;
    optind = 1;
    string filename;
    ifstream input;
    string strarg;

    while (!done) {
        ret = getopt_long(argc, (char * const *)argv, "", options, NULL);
        switch (ret) {
            case PORT:
                strarg.assign(optarg);
                props.SetProperty("port", strarg);
                cout << " Port: " << props["port"] << endl;
                cout.flush();
                break;
            
            case FLOWS:
                strarg.assign(optarg);
                props.SetProperty("flows", strarg);
                cout << " Flows: " << props["flows"] << endl;
                cout.flush();
                break;

            case CORE_ID:
                strarg.assign(optarg);
                props.SetProperty("core_id", strarg);
                cout << " Core id: " << props["core_id"] << endl;
                cout.flush();
                break;

            case WORKLOAD:
                filename.assign(optarg);
                input.open(filename);
                try {
                    props.Load(input);
                } catch (const string &message) {
                    cout << message << endl;
                    exit(0);
                }
                input.close();
                break;

            case -1:
                done = 1;
                break;
        }
    }

    fprintf(stdout, " [core %s] port: %s, flows: %s, workload: %s\n", \
                    props["core_id"].c_str(), props["port"].c_str(), props["flows"].c_str(), filename.c_str());

    return filename;
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

