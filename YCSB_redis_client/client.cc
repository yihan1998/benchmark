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

    sock = cygnus_socket(AF_INET, SOCK_STREAM, 0);
    if(sock == -1) {
        std::cerr <<  " allocate socket failed! " << std::endl;
        exit(1);
    }

    if(cygnus_connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0){
        std::cerr <<  " connect server failed! " << std::endl;
        exit(1);
    }

    if (cygnus_fcntl(sock, F_SETFL, O_NONBLOCK) == -1) {
        std::cerr <<  " cygnus_fcntl() set sock to non-block failed! " << std::endl;
        exit(EXIT_FAILURE);
    }

    struct conn_info * conn_info = &info[num_conn];
    conn_info->sockfd = sock;
    conn_info->epfd = epfd;

    conn_info->ibuf = (char *)calloc(16, 1024);
    conn_info->ioff = 0;

    conn_info->obuf = (char *)calloc(16, 1024);
    conn_info->ooff = 0;
    
    conn_info->total_record_ops = num_record_ops;
    conn_info->total_operation_ops = num_operation_ops;

    conn_info->actual_record_ops = conn_info->actual_operation_ops = 0;

    num_conn++;

    struct cygnus_epoll_event ev;
    ev.events = CYGNUS_EPOLLIN | CYGNUS_EPOLLOUT;
    ev.data.ptr = conn_info;

    int ret;

    if ((ret = cygnus_epoll_ctl(epfd, CYGNUS_EPOLL_CTL_ADD, sock, &ev)) == -1) {
        std::cerr <<  " cygnus_epoll_ctl() failed! " << std::endl;
        exit(EXIT_FAILURE);
    }

    return sock;
}

double LoadRecord(int epfd, struct cygnus_epoll_event * events, ycsbc::Client * client, const int num_record_ops, const int num_operation_ops, const int port, const int num_flows) {
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

        nevents = cygnus_epoll_wait(epfd, events, NR_CYGNUS_EPOLL_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & CYGNUS_EPOLLERR)) {
                client->HandleErrorEvent(info);
            }
            
            if ((events[i].events & CYGNUS_EPOLLIN)) {
                int len = cygnus_read(info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff); 

                if (len > 0) {
                    info->ioff += len;
                }

                if (strchr(info->ibuf,'\r\n')) {
                    printf(" [%s:%d] receive reply: %s", __func__, __LINE__, info->ibuf);

                    client->ReceiveReply(info->ibuf);

                    info->ioff = 0;

                    /* Increase actual ops */
                    if(++info->actual_record_ops == info->total_record_ops) {
                        // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                        // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                        if (++num_load_complete == num_conn) {
                            done = 1;
                        }
                    }
                    
                    struct cygnus_epoll_event ev;
                    ev.events = CYGNUS_EPOLLIN | CYGNUS_EPOLLOUT;
                    ev.data.ptr = info;
                    if ((cygnus_epoll_ctl(info->epfd, CYGNUS_EPOLL_CTL_MOD, info->sockfd, &ev)) == -1) {
                        fprintf(stdout, "cygnus_epoll_ctl: wait modify error");
                        exit(EXIT_FAILURE);
                    }
                }
            } else if ((events[i].events & CYGNUS_EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client->InsertRecord(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = cygnus_write(info->sockfd, info->obuf + info->ooff, info->oremain);
                
                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct cygnus_epoll_event ev;
                        ev.events = CYGNUS_EPOLLIN;
                        ev.data.ptr = info;
                        if (ret = (cygnus_epoll_ctl(info->epfd, CYGNUS_EPOLL_CTL_MOD, info->sockfd, &ev)) == -1) {
                            fprintf(stdout, "cygnus_epoll_ctl: wait modify error");
                            exit(EXIT_FAILURE);
                        }
                    }
                }
            }
        }
    }

    duration = timer.End();

    for (int i = 0; i < num_conn; i++) {
        struct cygnus_epoll_event ev;
        ev.events = CYGNUS_EPOLLIN | CYGNUS_EPOLLOUT;
        ev.data.ptr = &info[i];

        cygnus_epoll_ctl(info[i].epfd, CYGNUS_EPOLL_CTL_MOD, info[i].sockfd, &ev);
    }
    
    return duration;
}

double PerformTransaction(int epfd, struct cygnus_epoll_event * events, ycsbc::Client * client) {
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
        nevents = cygnus_epoll_wait(epfd, events, NR_CYGNUS_EPOLL_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & CYGNUS_EPOLLERR)) {
                client->HandleErrorEvent(info);
            }
            
            if ((events[i].events & CYGNUS_EPOLLIN)) {
                int len = cygnus_read(info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);
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
                } else if (!strcmp(info->ibuf, "+OK\r\n")) {

                } else {
                    perror(" unrecognized reply format");
                    exit(1);
                }
                
                printf(" [%s:%d] receive reply: %s", __func__, __LINE__, info->ibuf);

                client->ReceiveReply(info->ibuf);

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

                struct cygnus_epoll_event ev;
                ev.events = CYGNUS_EPOLLIN | CYGNUS_EPOLLOUT;
                ev.data.ptr = info;
                if ((cygnus_epoll_ctl(info->epfd, CYGNUS_EPOLL_CTL_MOD, info->sockfd, &ev)) == -1) {
                    fprintf(stdout, "cygnus_epoll_ctl: wait modify error");
                    exit(EXIT_FAILURE);
                }
            } else if ((events[i].events & CYGNUS_EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client->SendRequest(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }
            
                int len = cygnus_write(info->sockfd, info->obuf + info->ooff, info->oremain);
                
                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct cygnus_epoll_event ev;
                        ev.events = CYGNUS_EPOLLIN;
                        ev.data.ptr = info;
                        if (ret = (cygnus_epoll_ctl(info->epfd, CYGNUS_EPOLL_CTL_MOD, info->sockfd, &ev)) == -1) {
                            fprintf(stdout, "cygnus_epoll_ctl: wait modify error");
                            exit(EXIT_FAILURE);
                        }
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
    struct cygnus_param * param = (struct cygnus_param *)arg;

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
    struct cygnus_epoll_event * events;
    int nevents;

    /* Create epoll fd */
    epfd = cygnus_epoll_create(0);

    /* Initialize epoll event array */
    events = (struct cygnus_epoll_event *)malloc(NR_CYGNUS_EPOLL_EVENTS * CYGNUS_EPOLL_EVENT_SIZE);

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
    load_duration = LoadRecord(epfd, events, &client, record_total_ops, operation_total_ops, port, num_flows);

    fprintf(stdout, " [core %d] loaded records done! \n", lcore_id);  

    double transaction_duration = 0.0;
    transaction_duration = PerformTransaction(epfd, events, &client);

    // for (int i = 0; i < num_ops; ++i) {
    //   oks += client.DoTransaction();
    // }

    cygnus_sleep(5);

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
    cygnus_init((struct cygnus_param *)arg);

    for (int i = 0; i < ((struct cygnus_param *)arg)->argc; i++) {
        std::cout << "[print param after cygnus_init] [" << i << "] " << (((struct cygnus_param *)arg)->argv)[i] << std::endl;
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
    struct cygnus_param * param = cygnus_config(argc, argv);

    cygnus_spawn(client, param);

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
        
         if (sscanf(argv[i], "--server_port=%s\n", s, &junk) == 1) {
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
