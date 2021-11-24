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

/* State for the protocol parser */
struct reader {
    char * buf; /* Read buffer */
    size_t pos; /* Buffer cursor */
    size_t len; /* Buffer length */
    size_t maxbuf; /* Max length of unused buffer */
};

struct reply {
    int type; /* REDIS_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    int len; /* Length of string */
    char * str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
    struct reply ** element; /* elements vector for REDIS_REPLY_ARRAY */
};

static char * readBytes(struct reader * r, unsigned int bytes) {
    char * p;
    if (r->len-r->pos >= bytes) {
        p = r->buf+r->pos;
        r->pos += bytes;
        return p;
    }
    return NULL;
}

/* Read a long long value starting at *s, under the assumption that it will be
 * terminated by \r\n. Ambiguously returns -1 for unexpected input. */
static long long readLongLong(char *s) {
    long long v = 0;
    int dec, mult = 1;
    char c;

    if (*s == '-') {
        mult = -1;
        s++;
    } else if (*s == '+') {
        mult = 1;
        s++;
    }

    while ((c = *(s++)) != '\r') {
        dec = c - '0';
        if (dec >= 0 && dec < 10) {
            v *= 10;
            v += dec;
        } else {
            /* Should not happen... */
            return -1;
        }
    }

    return mult*v;
}

/* Find pointer to \r\n. */
static char * seekNewline(char * s, size_t len) {
    int pos = 0;
    int _len = len-1;

    /* Position should be < len-1 because the character at "pos" should be
     * followed by a \n. Note that strchr cannot be used because it doesn't
     * allow to search a limited length and the buffer that is being searched
     * might not have a trailing NULL character. */
    while (pos < _len) {
        while(pos < _len && s[pos] != '\r') pos++;
        if (s[pos] != '\r') {
            /* Not found. */
            return NULL;
        } else {
            if (s[pos+1] == '\n') {
                /* Found. */
                return s+pos;
            } else {
                /* Continue searching. */
                pos++;
            }
        }
    }
    return NULL;
}

static char *readLine(struct reader * r, int * _len) {
    char *p, *s;
    int len;

    p = r->buf+r->pos;
    s = seekNewline(p,(r->len-r->pos));
    if (s != NULL) {
        len = s-(r->buf+r->pos);
        r->pos += len+2; /* skip \r\n */
        if (_len) *_len = len;
        return p;
    }
    return NULL;
}

int parseReply(struct reader * r, struct reply * reply) {
    char * p;
    fprintf(stdout, " [%s:%d] remain to parse: %.*s\n", __func__, __LINE__, r->len - r->pos, r->buf + r->pos);
    if ((p = readBytes(r, 1)) != NULL) {
        if (p[0] == '-') {
            fprintf(stdout, " - REDIS_REPLY_ERROR");
            reply->type = REDIS_REPLY_ERROR;
            return reply;
        } else if (p[0] == '+') {
            fprintf(stdout, " + REDIS_REPLY_STATUS");
            reply->type = REDIS_REPLY_STATUS;
            return reply;
        } else if (p[0] == ':') {
            fprintf(stdout, " : REDIS_REPLY_INTEGER");
            reply->type = REDIS_REPLY_INTEGER;
            return reply;
        } else if (p[0] == '$') {
            fprintf(stdout, " $ REDIS_REPLY_STRING");
            reply->type = REDIS_REPLY_STRING;
            char * s;
            int len;
            fprintf(stdout, " \t string len: %d\n", reply->len);
            if ((s = readLine(r,&len)) != NULL) {
                reply->len = readLongLong(s);
                int read_len;
                if (reply->str = readLine(r, &read_len) != NULL) {
                    fprintf(stdout, " \t string: %s\n", reply->str);
                    if (reply->len == read_len) {
                        return reply;
                    } else {
                        return NULL;
                    }
                } else {
                    return NULL;
                }
            }
        } else if (p[0] == '*') {
            fprintf(stdout, " * REDIS_REPLY_ARRAY");
            reply->type = REDIS_REPLY_ARRAY;
            char * s;
            int len;
            if ((s = readLine(r,&len)) != NULL) {
                reply->elements = len;
                fprintf(stdout, " \t Array len: %d\n", len);
                reply->element = (struct reply **)calloc(reply->elements, sizeof(struct reply));
                for (int i = 0; i < len; i++) {
                    parseReply(r, reply->element[i]);
                }
            }
        }
    }
}

int getReply(char * buf, int len) {
    // int err;
    // struct reader r = {.buf = buf, .pos = 0, .len = len, .maxbuf = 1024*16};
    // struct reply reply;

    // if ((p = readBytes(&r, 1)) != NULL) {
    //     if (p[0] == '-') {
    //         reply.type = REDIS_REPLY_ERROR;
    //         err = 0;
    //     } else if (p[0] == '+') {
    //         reply.type = REDIS_REPLY_STATUS;
    //         err = 0;
    //     } else if (p[0] == ':') {
    //         reply.type = REDIS_REPLY_INTEGER;
    //         err = 0;
    //     } else if (p[0] == '$') {
    //         reply.type = REDIS_REPLY_STRING;
    //         break;
    //     } else if (p[0] == '*') {
    //         reply.type = REDIS_REPLY_ARRAY;
    //         long long len = readLongLong(p + 1);
    //         reply.elements = len;
    //         for (int i = 0; i < len; i++) {
    //             char * s = seekNewline(p, r->len-r->pos);
    //         }
    //         break;
    //     }
    // }

    // return err;
    struct reader r = {.buf = buf, .pos = 0, .len = len, .maxbuf = 1024*16};
    struct reply * reply = (struct reply *)calloc(1, sizeof(struct reply));

    parseReply(&r, reply);

    exit(1);

    return 0;
}

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
            if ((sock = client.ConnectServer("127.0.0.1", port)) > 0) {
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
                int len = read(info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);
                // printf(" [%s:%d] receive len: %d\n", __func__, __LINE__, len);

                if (len > 0) {
                    info->ioff += len;
                }

                if (getReply(info->ibuf, info->ioff)) {
                    info->ioff = 0;

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

                // if (strchr(info->ibuf,'\n')) {
                //     // printf(" [%s:%d] receive reply: %s", __func__, __LINE__, info->ibuf);

                //     client.ReceiveReply(info->ibuf);

                //     info->ioff = 0;

                //     /* Increase actual ops */
                //     if(++info->actual_record_ops == info->total_record_ops) {
                //         // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                //         // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                //         if (++num_load_complete == num_conn) {
                //             done = 1;
                //         }
                //     }
                    
                //     struct epoll_event ev;
                //     ev.events = EPOLLIN | EPOLLOUT;
                //     ev.data.ptr = info;

                //     epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                // }
            } else if ((events[i].events & EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client.InsertRecord(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = send(info->sockfd, info->obuf + info->ooff, info->oremain, 0);
                // printf(" [%s:%d] send len: %d\n", __func__, __LINE__, len);

                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct epoll_event ev;
                        ev.events = EPOLLIN;
                        ev.data.ptr = info;

                        epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                    }
                }
            } else {
                printf(" >> unknown event!\n");
            }
        }
    }

    duration = timer.End();

    for (int i = 0; i < num_conn; i++) {
        info[i].ioff = info[i].ooff = info[i].oremain = 0;
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
                int len = read(info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);
                printf(" [%s:%d] receive len: %d, ioff: %d\n", __func__, __LINE__, len, info->ioff);
                printf(" \t received: %*.s\n", len, info->ibuf + info->ioff);

                if (len > 0) {
                    info->ioff += len;
                }

                int to_recv = 0;
                int cursor = 0;
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
                    /* Receive Insert/Update reply */
                } else if (sscanf(info->ibuf, "%d\r\n", &cursor) == 1) {
                    fprintf(stdout, "%s", info->ibuf);
                    if (cursor > 0) {
                        fprintf(stdout, "%s", info->ibuf);
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
                
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.ptr = info;

                epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);

            } else if ((events[i].events & EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client.SendRequest(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = send(info->sockfd, info->obuf + info->ooff, info->oremain, 0);
                // printf(" [%s:%d] send len: %d\n", __func__, __LINE__, len);

                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct epoll_event ev;
                        ev.events = EPOLLIN;
                        ev.data.ptr = info;

                        epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
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

