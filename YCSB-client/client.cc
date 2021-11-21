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
__thread struct conn_info * info;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

double LoadRecord(int epfd, struct epoll_event * events, ycsbc::Client &client, const int num_record_ops, const int num_operation_ops, const int port, const int num_flows) {
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
            if ((sock = client.ConnectServer("10.0.1.1", port)) > 0) {
                // fprintf(stdout, " [%s] connect server through sock %d\n", __func__, sock);
                struct conn_info * conn_info = &info[num_conn];
                conn_info->sockfd = sock;
                conn_info->epfd = epfd;

                conn_info->total_record_ops = record_per_flow;
                conn_info->total_operation_ops = operation_per_flow;

                conn_info->actual_record_ops = conn_info->actual_operation_ops = 0;

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
                client.HandleErrorEvent(info);
            }
            
            if ((events[i].events & EPOLLIN)) {
                ycsbc::KVReply reply;
                int len = read(info->sockfd, &reply, sizeof(reply));

                if (len > 0) {
                    client.ReceiveReply(reply);

                    /* Increase actual ops */
                    if(++info->actual_record_ops == info->total_record_ops) {
                        // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                        // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                        if (++num_load_complete == num_conn) {
                            done = 1;
                        }
                    }
                    
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLOUT;
                    ev.data.ptr = info;

                    epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                }
            } else if ((events[i].events & EPOLLOUT)) {
                ycsbc::KVRequest request;
                client.InsertRecord(request);

                int len = send(info->sockfd, &request, sizeof(request), 0);
            
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

    duration = timer.End();

    for (int i = 0; i < num_conn; i++) {
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT;
        ev.data.ptr = &info[i];

        epoll_ctl(info[i].epfd, EPOLL_CTL_MOD, info[i].sockfd, &ev);
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
        nevents = epoll_wait(epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & EPOLLERR)) {
                client.HandleErrorEvent(info);
            }
            
            if ((events[i].events & EPOLLIN)) {
                ret = client.HandleReadEvent(info);
                if (ret > 0) {
                    /* Increase actual ops */
                    oks++;
                    if(++info->actual_operation_ops == info->total_operation_ops) {
                        shutdown(info->sockfd, SHUT_WR);
                        if (++num_transaction_complete == num_conn) {
                            done = 1;
                        }
                        close(info->sockfd);
                    }

                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLOUT;
                    ev.data.ptr = info;

                    epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                }
            } else if ((events[i].events & EPOLLOUT)) {
                ret = client.HandleWriteEvent(info);
                if(ret > 0) {
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

    fprintf(stdout, " # Transaction: %llu\n", oks);

    duration = timer.End();
    return duration;
}

int main(const int argc, const char *argv[]) {
    utils::Properties props;
    string file_name = ParseCommandLine(argc, argv, props);
    cout << " Test workload: " << file_name << endl;

    int core_id = atoi(props.GetProperty("core_id", "1").c_str());

    cpu_set_t core_set;
    CPU_ZERO(&core_set);
    CPU_SET(core_id, &core_set);

    if (pthread_setaffinity_np(pthread_self(), sizeof(core_set), &core_set) == -1){
        printf("warning: could not set CPU affinity, continuing...\n");
    }

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
    epfd = epoll_create1(0);

    /* Initialize epoll event array */
    events = (struct epoll_event *)calloc(MAX_EVENTS, sizeof(struct epoll_event));

    int port = stoi(props.GetProperty("port", "80"));

    double load_duration = 0.0;
    load_duration = LoadRecord(epfd, events, client, record_total_ops, operation_total_ops, port, num_flows);

    fprintf(stdout, " [core %d] loaded records done! \n", core_id);  

    double transaction_duration = 0.0;
    transaction_duration = PerformTransaction(epfd, events, client);

    char output[256];

    char output_file_name[32];
	sprintf(output_file_name, "throughput_core_%d.txt", core_id);
	FILE * output_file = fopen(output_file_name, "a+");

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

