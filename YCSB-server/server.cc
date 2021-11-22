#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/server.h"
#include "db/db_factory.h"

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <getopt.h>

#define MAX_CONNECT 4100
#define MAX_EVENTS  8192

using namespace std;

DEFINE_PER_LCORE(int, num_accept);

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
int ParseCommandLine(int argc, const char ** argv, utils::Properties &props);

void * DelegateServer(void * arg) {
    struct cetus_param * param = (struct cetus_param *)arg;

    utils::Properties props;
    ParseCommandLine(param->argc, (const char **)param->argv, props);

    ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
    if (!db) {
        cout << "Unknown database name " << props["dbname"] << endl;
        exit(0);
    }

    db->Init(stoi(props["keylength"]), stoi(props["valuelength"]));
    ycsbc::Server server(*db);

    int oks = 0;
    
    num_accept = 0;

    int num_complete = 0;

    int epfd;
    struct cetus_epoll_event * events;
    int nevents;

    /* Create epoll fd */
    epfd = cetus_epoll_create(0);

    /* Initialize epoll event array */
    events = (struct cetus_epoll_event *)malloc(NR_CETUS_EPOLL_EVENTS * CETUS_EPOLL_EVENT_SIZE);

    int sock = cetus_socket(AF_INET, SOCK_STREAM, 0);
    if(sock == -1) {
        std::cerr <<  " allocate socket failed! " << std::endl;
        exit(1);
    }

    if (cetus_fcntl(sock, F_SETFL, O_NONBLOCK) == -1) {
        std::cerr <<  " cetus_fcntl() set sock to non-block failed! " << std::endl;
        exit(1);
    }

    /* Bind to port */
	struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;

    int port = stoi(props["port"]);
    addr.sin_port = htons(port);

    int ret;
    if ((ret = cetus_bind(sock, (struct sockaddr *)&addr, sizeof(addr))) == -1) {
        std::cerr <<  " cetus_bind() failed! " << std::endl;
        exit(EXIT_FAILURE);
    }
    
    if((ret = cetus_listen(sock, 1024)) == -1) {
        std::cerr <<  " cetus_listen() failed! " << std::endl;
        exit(EXIT_FAILURE);
    }

    fprintf(stdout, " [%s] wait for connection on sock %d\n", __func__, sock);
    fflush(stdout);

    struct cetus_epoll_event ev;

    ev.events = CETUS_EPOLLIN;
    ev.data.fd = sock;

    if ((ret = cetus_epoll_ctl(epfd, CETUS_EPOLL_CTL_ADD, sock, &ev)) == -1) {
        std::cerr <<  " cetus_epoll_ctl() failed! " << std::endl;
        exit(EXIT_FAILURE);
    }

    int wait_timerfd = cetus_timerfd_create(CLOCK_MONOTONIC, CETUS_TFD_NONBLOCK);
    if (wait_timerfd == -1) {
        fprintf(stderr, " cetus_timerfd_create() wait_timerfd failed!\n");
        exit(1);
    }

    unsigned int wait_time = stoi(props.GetProperty("time", "30"));

    struct itimerspec ts;
    ts.it_interval.tv_sec = 0;
	ts.it_interval.tv_nsec = 0;
	ts.it_value.tv_sec = wait_time;
	ts.it_value.tv_nsec = 0;

	if (cetus_timerfd_settime(wait_timerfd, 0, &ts, NULL) < 0) {
		fprintf(stderr, "cetus_timerfd_settime() failed");
        exit(EXIT_FAILURE);
	}

    ev.events = CETUS_EPOLLIN;
    ev.data.fd = wait_timerfd;
    if (ret = (cetus_epoll_ctl(epfd, CETUS_EPOLL_CTL_ADD, wait_timerfd, &ev)) == -1) {
        fprintf(stderr, " cetus_epoll_ctl() error on wait_timerfd sock\n");
        exit(EXIT_FAILURE);
    }

    int done = 0;
    while(!done) {
        nevents = cetus_epoll_wait(epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            if (events[i].data.fd == sock) {
                /* Accept connection */
                int c;
                if ((c = server.AcceptConnection(sock)) > 0) {
                    // std::cout <<  " accept connection through sock " << c << std::endl;
                    struct cetus_epoll_event ev;
                    ev.events = CETUS_EPOLLIN;
                    ev.data.fd = c;

                    if ((ret = cetus_epoll_ctl(epfd, CETUS_EPOLL_CTL_ADD, c, &ev)) == -1) {
                        std::cerr <<  " cetus_epoll_ctl() failed! " << std::endl;
                        exit(EXIT_FAILURE);
                    }

                    num_accept++;
                }
            } else if (events[i].data.fd == wait_timerfd) {
                fprintf(stdout, " [%s on core %d] wait time's up!", __func__, lcore_id);
                if (!num_accept) {
                    return NULL;
                } else {
                    continue;
                }
            } else if ((events[i].events & CETUS_EPOLLERR)) {
                server.HandleErrorEvent(events[i].data.fd);
                // std::cerr <<  " num_complete : " << num_complete + 1 << std::endl;
                if (++num_complete == num_accept) {
                    done = 1;
                }
            } else if ((events[i].events & CETUS_EPOLLIN)) {
                int ret = server.HandleReadEvent(events[i].data.fd);
                // if (ret < 0) {
                //     done = 1;
                // }
            } else {
                printf(" >> unknown event! (%x)\n", events[i].events);
            }
        }
    }

    // for (int i = 0; i < num_ops; ++i) {
    //   oks += client.DoTransaction();
    // }

    db->Close();

    return NULL;
}

int server(void * arg) {
    cetus_init((struct cetus_param *)arg);

    int ret;
    mthread_t mid;

    sail_init();
    
    /* Create polling thread */
    if((ret = mthread_create(&mid, NULL, DelegateServer, arg)) < 0) {
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
    // utils::Properties props;
    // ParseCommandLine(argc, argv, props);

    // ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
    // if (!db) {
    //     cout << "Unknown database name " << props["dbname"] << endl;
    //     exit(0);
    // }

    // const int num_threads = stoi(props.GetProperty("threadcount", "1"));

    // vector<future<int>> actual_ops;

    // // Peforms transactions
    // actual_ops.clear();
    // utils::Timer<double> timer;
    // timer.Start();
    // for (int i = 0; i < num_threads; ++i) {
    //     fprintf(stdout, " [%s] starting thread(%d)...\n", __func__, i);
    //     fflush(stdout);
    //     actual_ops.emplace_back(async(launch::async, DelegateServer, db));
    // }
    // assert((int)actual_ops.size() == num_threads);

    // int sum = 0;
    // for (auto &n : actual_ops) {
    //     assert(n.valid());
    //     sum += n.get();
    // }
    // double duration = timer.End();
    // cerr << "# Transaction throughput (KTPS)" << endl;
    // cerr << props["dbname"] << '\t' << num_threads << '\t';

    struct cetus_param * param = cetus_config(argc, argv);

    cetus_spawn(server, param);

    std::cout << " [ " << __func__ << " on core " << lcore_id << "] test finished, return from main" << std::endl;

    return 0;
}

int ParseCommandLine(int argc, const char ** argv, utils::Properties &props) {
    std::cout << __func__ << std::endl;
    int argindex = 1;
    string filename;
    ifstream input;
    
    for (int i = 0; i < argc; i++) {
        std::cout << argv[i] << std::endl;

        char s[MAX_BUFF_LEN];
        char junk;
        
        if (sscanf(argv[i], "--db=%s\n", s, &junk) == 1){
            props.SetProperty("dbname", s);
            std::cout << " Database: " << props["dbname"].c_str() << std::endl;
        } else if (sscanf(argv[i], "--port=%s\n", s, &junk) == 1) {
            props.SetProperty("port", s);
            std::cout << " Port: " << props["port"].c_str() << std::endl;
        } else if (sscanf(argv[i], "--time=%s\n", s, &junk) == 1) {
            props.SetProperty("time", s);
            std::cout << " Wait time: " << props["time"].c_str() << std::endl;
        } else if (sscanf(argv[i], "--key_length=%s\n", s, &junk) == 1) {
            props.SetProperty("keylength", s);
            std::cout << " Key length: " << props["keylength"].c_str() << std::endl;
        } else if (sscanf(argv[i], "--value_length=%s\n", s, &junk) == 1) {
            props.SetProperty("valuelength", s);
            std::cout << " Value length: " << props["valuelength"].c_str() << std::endl;
        } 
    }

    return 0;
}

// enum cfg_params {
//     DB,
//     PORT,
// };

// const struct option options[] = {
//     {   .name = "db", 
//         .has_arg = required_argument,
//         .flag = NULL, 
//         .val = DB},
//     {   .name = "port", 
//         .has_arg = required_argument,
//         .flag = NULL, 
//         .val = PORT},
// };

// int ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
//     int ret, done = 0;
//     char * end;
//     optind = 1;

//     while (!done) {
//         ret = getopt_long(argc, (char * const *)argv, "", options, NULL);
//         switch (ret) {
//             case DB:
//                 props.SetProperty("dbname", optarg);
//                 cout << " Database: " << props["dbname"] << endl;
//                 break;

//             case PORT:
//                 props.SetProperty("port", optarg);
//                 cout << " Port: " << props["port"] << endl;
//                 break;

//             case -1:
//                 done = 1;
//                 break;
//         }
//     }

//     return 0;
// }

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

