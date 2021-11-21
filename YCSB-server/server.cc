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

__thread int num_accept = 0;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
int ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

int DelegateServer(utils::Properties &props, ycsbc::DB * db) {
    db->Init(stoi(props["keylength"]), stoi(props["valuelength"]));

    ycsbc::Server server(*db);

    int oks = 0;

    int epfd;
    struct epoll_event * events;
    int nevents;

    int num_complete = 0;

    /* Create epoll fd */
    epfd = epoll_create1(0);

    /* Initialize epoll event array */
    events = (struct epoll_event *)calloc(MAX_EVENTS, sizeof(struct epoll_event));

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if(!sock) {
        printf(" [%s] allocate socket failed !\n", __func__);
        exit(1);
    }
    
    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    fcntl(sock, F_SETFL, O_NONBLOCK);

    /* Bind to port */
	struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;

    int port = stoi(props["port"]);
    addr.sin_port = htons(port);

    int ret;
    ret = bind(sock, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));
    if(ret < 0) {
        perror("bind failed");
        exit(1);
    }
    
    ret = listen(sock, 1024);
    if(ret < 0) {
        perror("listen failed");
        exit(1);
    }

    struct epoll_event ev;

    ev.events = EPOLLIN;
    ev.data.fd = sock;

    epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev);

    int wait_timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (wait_timerfd == -1) {
        fprintf(stderr, " timerfd_create() wait_timerfd failed!\n");
        exit(1);
    }

    struct itimerspec ts;
    ts.it_interval.tv_sec = 0;
	ts.it_interval.tv_nsec = 0;
	ts.it_value.tv_sec = 30;
	ts.it_value.tv_nsec = 0;

	if (timerfd_settime(wait_timerfd, 0, &ts, NULL) < 0) {
		fprintf(stderr, "cetus_timerfd_settime() failed");
        exit(EXIT_FAILURE);
	}

    ev.events = EPOLLIN;
    ev.data.fd = wait_timerfd;
    if (ret = (epoll_ctl(epfd, EPOLL_CTL_ADD, wait_timerfd, &ev)) == -1) {
        fprintf(stderr, " cetus_epoll_ctl() error on wait_timerfd sock\n");
        exit(EXIT_FAILURE);
    }

    int done = 0;
    while(!done) {
        nevents = epoll_wait(epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            if (events[i].data.fd == sock) {
                /* Accept connection */
                int c;
                if ((c = server.AcceptConnection(sock)) > 0) {
                    // std::cout <<  " accept connection through sock " << c << std::endl;
                    struct epoll_event ev;
                    ev.events = EPOLLIN;
                    ev.data.fd = c;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, c, &ev);

                    num_accept++;
                }
            }  else if (events[i].data.fd == wait_timerfd) {
                if (!num_accept) {
                    return NULL;
                } else {
                    continue;
                }
            } else if ((events[i].events & EPOLLERR)) {
                // cout << " Closing sock " << events[i].events << endl;
                server.HandleErrorEvent(events[i].data.fd);
                if (++num_complete == num_accept) {
                    done = 1;
                }
            } else if ((events[i].events & EPOLLIN)) {
                int ret = server.HandleReadEvent(events[i].data.fd);
                if (ret <= 0) {
                    close(events[i].data.fd);
                    if (++num_complete == num_accept) {
                        done = 1;
                    }
                }
                oks++;
            }
        }
    }

    // for (int i = 0; i < num_ops; ++i) {
    //   oks += client.DoTransaction();
    // }

    db->Close();
    return oks;
}

int main(const int argc, const char *argv[]) {
    utils::Properties props;
    ParseCommandLine(argc, argv, props);

    int core_id = stoi(props.GetProperty("core_id", "1"));

    cpu_set_t core_set;
    CPU_ZERO(&core_set);
    CPU_SET(core_id, &core_set);

    if (pthread_setaffinity_np(pthread_self(), sizeof(core_set), &core_set) == -1){
        printf("warning: could not set CPU affinity, continuing...\n");
    }

    ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
    if (!db) {
        cout << "Unknown database name " << props["dbname"] << endl;
        exit(0);
    }

    // Peforms transactions
    utils::Timer<double> timer;

    timer.Start();
    int total_ops = DelegateServer(props, db);
    double duration = timer.End();

    fprintf(stdout, " [core %d] # Transaction throughput : %.2f (KTPS) \t %s\n", \
                    core_id, total_ops / duration / 1000, props["dbname"].c_str());
    fflush(stdout);

    return 0;
}

// int ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
//     int argindex = 1;
//     while (argindex < argc && StrStartWith(argv[argindex], "-")) {
//         if (strcmp(argv[argindex], "-threads") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             props.SetProperty("threadcount", argv[argindex]);
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
//         } else {
//             cout << "Unknown option '" << argv[argindex] << "'" << endl;
//             exit(0);
//         }
//     }

//     if (argindex == 1 || argindex != argc) {
//         UsageMessage(argv[0]);
//         exit(0);
//     }

//     return 0;
// }

enum cfg_params {
    DB,
    PORT,
    CORE_ID,
    KEY_LENGTH,
    VALUE_LENGTH,
};

const struct option options[] = {
    {   .name = "db", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = DB},
    {   .name = "port", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = PORT},
    {   .name = "core_id", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = CORE_ID},
    {   .name = "key_length", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = KEY_LENGTH},
    {   .name = "value_length", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = VALUE_LENGTH},
    {0, 0, 0, 0}
};

int ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
    int ret, done = 0;
    char * end;
    optind = 1;

    while (!done) {
        ret = getopt_long(argc, (char * const *)argv, "", options, NULL);
        switch (ret) {
            case DB:
                props.SetProperty("dbname", optarg);
                cout << " Database: " << props["dbname"] << endl;
                break;

            case PORT:
                props.SetProperty("port", optarg);
                cout << " Port: " << props["port"] << endl;
                break;
            
            case CORE_ID:
                props.SetProperty("core_id", optarg);
                cout << " Core id: " << props["core_id"] << endl;
                break;
            
            case KEY_LENGTH:
                props.SetProperty("keylength", optarg);
                cout << " Key length: " << props["keylength"] << endl;
                break;
            
            case VALUE_LENGTH:
                props.SetProperty("valuelength", optarg);
                cout << " Value length: " << props["valuelength"] << endl;
                break;

            case -1:
                done = 1;
                break;
        }
    }

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

