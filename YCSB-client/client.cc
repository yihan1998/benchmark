#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <getopt.h>

#define MAX_CONNECT 4100
#define MAX_EVENTS  8192

using namespace std;

DEFINE_PER_LCORE(int, num_conn);
DEFINE_PER_LCORE(struct conn_info *, info);

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int ConnectServer(int epfd, char * server_ip, uint16_t port, const int num_record_ops, const int num_operation_ops) {
    int sock;

    struct sockaddr_in server_addr;

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(server_ip);

    sock = cetus_socket(AF_INET, SOCK_STREAM, 0);
    if(sock == -1) {
        std::cerr <<  " allocate socket failed! " << std::endl;
        exit(1);
    }

    if(cetus_connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0){
        std::cerr <<  " connect server failed! " << std::endl;
        exit(1);
    }

    if (cetus_fcntl(sock, F_SETFL, O_NONBLOCK) == -1) {
        std::cerr <<  " cetus_fcntl() set sock to non-block failed! " << std::endl;
        exit(EXIT_FAILURE);
    }

    struct conn_info * conn_info = &info[num_conn];
    conn_info->sockfd = sock;
    conn_info->epfd = epfd;
    
    conn_info->total_record_ops = num_record_ops;
    conn_info->total_operation_ops = num_operation_ops;

    conn_info->actual_record_ops = conn_info->actual_operation_ops = 0;

    num_conn++;

    struct cetus_epoll_event ev;
    ev.events = CETUS_EPOLLIN | CETUS_EPOLLOUT;
    ev.data.ptr = conn_info;

    int ret;

    if ((ret = cetus_epoll_ctl(epfd, CETUS_EPOLL_CTL_ADD, sock, &ev)) == -1) {
        std::cerr <<  " cetus_epoll_ctl() failed! " << std::endl;
        exit(EXIT_FAILURE);
    }

    return sock;
}

double LoadRecord(int epfd, struct cetus_epoll_event * events, ycsbc::Client &client, const int num_record_ops, const int num_operation_ops, const int port, const int num_flows) {
    int record_per_flow = num_record_ops / num_flows;
    int operation_per_flow = num_operation_ops / num_flows;

    int done = 0;

    int nevents;

    int num_load_complete = 0;

    utils::Timer<double> timer;
    double duration;

    timer.Start();

    struct timeval log_time;
    gettimeofday(&log_time, NULL);

    int sec_send = 0;
    int sec_recv = 0;

    while(!done) {
        while(num_conn < num_flows) {
            /* Connect server */
            if((ConnectServer(epfd, "10.0.0.1", port, record_per_flow, operation_per_flow)) < 0) {
                break;
            }
        }

        nevents = cetus_epoll_wait(epfd, events, NR_CETUS_EPOLL_EVENTS, -1);

        // struct timeval curr;
        // gettimeofday(&curr, NULL);

        // if (curr.tv_sec - log_time.tv_sec >= 1) {
        //     fprintf(stdout, " [%s on core %d] recv %d (B), send %d (B)\n", __func__, lcore_id, sec_recv, sec_send);
        //     gettimeofday(&log_time, NULL);
        //     sec_recv = sec_send = 0;
        // }

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & CETUS_EPOLLERR)) {
                client.HandleErrorEvent(info);
            }
            
            if ((events[i].events & CETUS_EPOLLIN)) {
                ycsbc::KVReply reply;
                int len = cetus_read(info->sockfd, (char *)&reply, sizeof(reply)); 
                // assert(len == sizeof(reply));

                sec_recv += len;

                if (len > 0) {
                    if(++info->actual_record_ops == info->total_record_ops) {
                        if (++num_load_complete == num_conn) {
                            done = 1;
                        }
                    } else {
                        struct cetus_epoll_event ev;
                        ev.events = CETUS_EPOLLIN | CETUS_EPOLLOUT;
                        ev.data.ptr = info;
                        if ((cetus_epoll_ctl(info->epfd, CETUS_EPOLL_CTL_MOD, info->sockfd, &ev)) == -1) {
                            fprintf(stdout, "cetus_epoll_ctl: wait modify error");
                            exit(EXIT_FAILURE);
                        }
                    }
                }
            } else if ((events[i].events & CETUS_EPOLLOUT)) {
                ycsbc::KVRequest * request = (ycsbc::KVRequest *)malloc(sizeof(ycsbc::KVRequest));
                client.InsertRecord(request);

                int len = cetus_write(info->sockfd, (char *)request, sizeof(ycsbc::KVRequest));
                // fprintf(stdout, " [core %d] request: %p, sock %d send: %x, key: %.*s, value: %.*s\n", \
                                lcore_id, request, info->sockfd, request->op, 32, request->request.first, 32, request->request.second);
                // assert(len == sizeof(request));
                sec_send += len;

                free(request);
            
                if(len > 0) {
                    int ret;
                    struct cetus_epoll_event ev;
                    ev.events = CETUS_EPOLLIN;
                    ev.data.ptr = info;
                    if (ret = (cetus_epoll_ctl(info->epfd, CETUS_EPOLL_CTL_MOD, info->sockfd, &ev)) == -1) {
                        fprintf(stdout, "cetus_epoll_ctl: wait modify error");
                        exit(EXIT_FAILURE);
                    }
                }
            }
        }
    }

    duration = timer.End();

    for (int i = 0; i < num_conn; i++) {
        struct cetus_epoll_event ev;
        ev.events = CETUS_EPOLLIN | CETUS_EPOLLOUT;
        ev.data.ptr = &info[i];

        cetus_epoll_ctl(info[i].epfd, CETUS_EPOLL_CTL_MOD, info[i].sockfd, &ev);
    }
    
    return duration;
}

double PerformTransaction(int epfd, struct cetus_epoll_event * events, ycsbc::Client &client) {
    int done = 0;

    utils::Timer<double> timer;
    double duration;

    int num_transaction_complete = 0;

    int nevents;

    int oks = 0;

    timer.Start();

    struct timeval log_time;
    gettimeofday(&log_time, NULL);

    int sec_send = 0;
    int sec_recv = 0;

    while(!done) {
        nevents = cetus_epoll_wait(epfd, events, NR_CETUS_EPOLL_EVENTS, -1);

        // struct timeval curr;
        // gettimeofday(&curr, NULL);

        // if (curr.tv_sec - log_time.tv_sec >= 1) {
        //     fprintf(stdout, " [%s on core %d] recv %d (B), send %d (B)\n", __func__, lcore_id, sec_recv, sec_send);
        //     gettimeofday(&log_time, NULL);
        //     sec_recv = sec_send = 0;
        // }

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & CETUS_EPOLLERR)) {
                client.HandleErrorEvent(info);
            }
            
            if ((events[i].events & CETUS_EPOLLIN)) {
                int ret = client.HandleReadEvent(info);
                if (ret > 0) {
                    oks++;
                    if(++info->actual_operation_ops >= info->total_operation_ops) {
                        if (++num_transaction_complete >= num_conn) {
                            done = 1;
                        }
                        cetus_close(info->sockfd);
                    } else {
                        struct cetus_epoll_event ev;
                        ev.events = CETUS_EPOLLIN | CETUS_EPOLLOUT;
                        ev.data.ptr = info;
                        if ((cetus_epoll_ctl(info->epfd, CETUS_EPOLL_CTL_MOD, info->sockfd, &ev)) == -1) {
                            fprintf(stdout, "cetus_epoll_ctl: wait modify error");
                            exit(EXIT_FAILURE);
                        }
                    }
                }
            } else if ((events[i].events & CETUS_EPOLLOUT)) {
                int ret = client.HandleWriteEvent(info);
            
                if(ret > 0) {
                    struct cetus_epoll_event ev;
                    ev.events = CETUS_EPOLLIN;
                    ev.data.ptr = info;

                    if ((cetus_epoll_ctl(info->epfd, CETUS_EPOLL_CTL_MOD, info->sockfd, &ev)) == -1) {
                        fprintf(stdout, "cetus_epoll_ctl: wait modify error");
                        exit(EXIT_FAILURE);
                    }
                }
            }
        }
    }

    duration = timer.End();

    fprintf(stdout, " [core %d] # Transaction: %llu\n", lcore_id, oks);

    return duration;
}

void * DelegateClient(void * arg) {
    struct cetus_param * param = (struct cetus_param *)arg;

    utils::Properties props;
    string file_name = ParseCommandLine(param->argc, (const char **)param->argv, props);

    ycsbc::CoreWorkload wl;
    wl.Init(props);

    const int num_flows = stoi(props.GetProperty("flows", "1"));

    int record_total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    int operation_total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

    ycsbc::Client client(wl);

    int oks = 0;

    int epfd;
    struct cetus_epoll_event * events;
    int nevents;

    /* Create epoll fd */
    epfd = cetus_epoll_create(0);

    /* Initialize epoll event array */
    events = (struct cetus_epoll_event *)malloc(NR_CETUS_EPOLL_EVENTS * CETUS_EPOLL_EVENT_SIZE);

    int done = 0;

    /* Initialize connection number */
    num_conn = 0;

    /* Initialize connection info array */
    info = (struct conn_info *)malloc(MAX_CONNECT * sizeof(struct conn_info));

    /* Actual ops number */
    int actual_record_ops, actual_operation_ops;
    actual_record_ops = actual_operation_ops = 0;

    int port = stoi(props.GetProperty("port", "80"));

    double load_duration = 0.0;
    load_duration = LoadRecord(epfd, events, client, record_total_ops, operation_total_ops, port, num_flows);

    fprintf(stdout, " [core %d] loaded records done! \n", lcore_id);  

    double transaction_duration = 0.0;
    transaction_duration = PerformTransaction(epfd, events, client);

    // for (int i = 0; i < num_ops; ++i) {
    //   oks += client.DoTransaction();
    // }

    cetus_sleep(5);

    char output[256];

    char output_file_name[32];
	sprintf(output_file_name, "throughput_core_%d.txt", lcore_id);
	FILE * output_file = fopen(output_file_name, "a+");

    sprintf(output, " [core %d] # Transaction throughput : %.2f (KTPS) \t %s \t %d\n", \
                    lcore_id, operation_total_ops / transaction_duration / 1000, \
                    file_name.c_str(), num_flows);

    fprintf(stdout, "%s", output);
    fflush(stdout);

    fprintf(output_file, "%s", output);
	fclose(output_file);

    return NULL;
}

int client(void * arg) {
    cetus_init((struct cetus_param *)arg);

    for (int i = 0; i < ((struct cetus_param *)arg)->argc; i++) {
        std::cout << "[print param after cetus_init] [" << i << "] " << (((struct cetus_param *)arg)->argv)[i] << std::endl;
    }

    int ret;
    mthread_t mid;

    sail_init();
    
    /* Create polling thread */
    if((ret = mthread_create(&mid, NULL, DelegateClient, arg)) < 0) {
        std::cerr << "mthread_create() failed!" << std::endl;
        exit(1);
    }

    /* Test mthread_join */
    if ((ret = mthread_join(mid, NULL)) < 0) {
        std::cerr << "mthread_join() failed!" << std::endl;
        exit(1);
    }

    std::cout << " [ " << __func__ << " on core " << lcore_id << "] mthread " << mid << " joined!" << std::endl;

    sail_exit();

    return 0;
}

int main(int argc, char ** argv) {
    struct cetus_param * param = cetus_config(argc, argv);

    cetus_spawn(client, param);

    std::cout << " [ " << __func__ << " on core " << lcore_id << "] test finished, return from main" << std::endl;

    return 0;
}

string ParseCommandLine(int argc, const char ** argv, utils::Properties &props) {
    std::cout << __func__ << std::endl;
    int argindex = 1;
    string filename;
    ifstream input;
    
    for (int i = 0; i < argc; i++) {
        std::cout << argv[i] << std::endl;

        char s[MAX_BUFF_LEN];
        char junk;
        
         if (sscanf(argv[i], "--port=%s\n", s, &junk) == 1) {
            props.SetProperty("port", s);
            std::cout << " Port: " << props["port"].c_str() << std::endl;
        } else if (sscanf(argv[i], "--workload=%s\n", s, &junk) == 1) {
            filename.assign(s);
            input.open(filename);
            try {
                props.Load(input);
            } catch (const string &message) {
                cout << message << endl;
                exit(0);
            }
            input.close();
        } else if (sscanf(argv[i], "--flows=%s\n", s, &junk) == 1) {
            props.SetProperty("flows", s);
            std::cout << " Flows: " << props["flows"] << std::endl;
        }
    }

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

