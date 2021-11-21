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
#include <signal.h>
#include <getopt.h>

#include <sys/epoll.h>
#include <sys/timerfd.h>

#define MAX_CONNECT 2100
#define MAX_EVENTS  8192

using namespace std;

__thread int num_accept = 0;

__thread int num_cores = 0;

static char * conf_file = NULL;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
int ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

void SignalHandler(int signum){
	int i;

    if (signum == SIGINT) {
        for (i = 0; i < num_cores; i++) {
            if (sv_thread[i] == pthread_self()) {
                //TRACE_INFO("Server thread %d got SIGINT\n", i);
                pthread_kill(sv_thread[i], SIGTERM);
            }
        }
    } else if (signum == SIGTERM) {
        exit(0);
    }
}

struct thread_context * InitializeServerThread(int core){
	struct thread_context * ctx;

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

	/* allocate memory for server variables */
	ctx->svars = (struct server_vars *)calloc(MAX_CONNECT, sizeof(struct server_vars));
	if (!ctx->svars) {
		mtcp_close(ctx->mctx, ctx->epfd);
		mtcp_destroy_context(ctx->mctx);
		free(ctx);
		fprintf(stderr, "Failed to create server_vars struct!\n");
		return NULL;
	}

	return ctx;
}

int CreateListeningSocket(struct thread_context * ctx){
	int sock;
	struct sockaddr_in saddr;
	int ret;

	/* create socket and set it as nonblocking */
	sock = mtcp_socket(ctx->mctx, AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		fprintf(stderr, "Failed to create listening socket!\n");
		return -1;
	}

	ret = mtcp_setsock_nonblock(ctx->mctx, sock);
	if (ret < 0) {
		fprintf(stderr, "Failed to set socket in nonblocking mode.\n");
		return -1;
	}

	/* bind to port 80 */
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = INADDR_ANY;
	saddr.sin_port = htons(80);
	ret = mtcp_bind(ctx->mctx, sock, (struct sockaddr *)&saddr, sizeof(struct sockaddr_in));
	if (ret < 0) {
		fprintf(stderr, "Failed to bind to the listening socket!\n");
		return -1;
	}

	ret = mtcp_listen(ctx->mctx, sock, 1024);
	if (ret < 0) {
		fprintf(stderr, "mtcp_listen() failed!\n");
		return -1;
	}
	
	/* wait for incoming accept events */
    struct mtcp_epoll_event ev;
	ev.events = MTCP_EPOLLIN;
	ev.data.sockid = sock;
	mtcp_epoll_ctl(ctx->mctx, ctx->epfd, MTCP_EPOLL_CTL_ADD, sock, &ev);

	return sock;
}

void * RunServerThread(void * arg) {
    struct server_arg * sarg = (struct server_arg *)arg;

    int core = sarg->core;
    utils::Properties * props = sarg->props;

	int i, ret;

    ycsbc::DB *db = ycsbc::DBFactory::CreateDB(*props);
    if (!db) {
        cout << "Unknown database name " << (*props)["dbname"] << endl;
        exit(0);
    }

    db->Init(stoi((*props)["keylength"]), stoi((*props)["valuelength"]));
    ycsbc::Server server(*db);

    int oks = 0;

    int epfd;
    struct mtcp_epoll_event * events;
    struct mtcp_epoll_event ev;
    int nevents;

    int num_complete = 0;

    /* mTCP initialization */
    struct thread_context * ctx;
	ctx = InitializeServerThread(core);
	if (!ctx) {
		fprintf(stderr, "Failed to initialize server thread.\n");
		return NULL;
	}

    epfd = ctx->epfd;
    /* Initialize epoll event array */
    events = (struct mtcp_epoll_event *)calloc(MAX_EVENTS, sizeof(struct mtcp_epoll_event));

    int sock;
    sock = CreateListeningSocket(ctx);
	if (sock < 0) {
		fprintf(stderr, "Failed to create listening socket.\n");
		exit(-1);
	}

    int done = 0;
    while(!done) {
        nevents = mtcp_epoll_wait(ctx->mctx, ctx->epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            if (events[i].data.sockid == sock) {
                /* Accept connection */
                int c;
                if ((c = mtcp_accept(ctx->mctx, sock, NULL, NULL)) > 0) {
                    struct server_vars * sv = &ctx->svars[num_accept++];
            		memset(sv, 0, sizeof(sv));

                    sv->epfd = epfd;
                    sv->sockfd = c;
                    sv->ctx = ctx;

                    // std::cout <<  " accept connection through sock " << c << std::endl;
                    
                    ev.events = MTCP_EPOLLIN;
                    ev.data.ptr = sv;

                    mtcp_setsock_nonblock(ctx->mctx, c);
                    
                    mtcp_epoll_ctl(ctx->mctx, ctx->epfd, MTCP_EPOLL_CTL_ADD, c, &ev);
                }
            } else if ((events[i].events & MTCP_EPOLLERR)) {
                // cout << " Closing sock " << events[i].events << endl;
                struct server_vars * sv = (struct server_vars *)events[i].data.ptr;
                server.HandleErrorEvent(sv);
                if (++num_complete == num_accept) {
                    done = 1;
                }
            } else if ((events[i].events & MTCP_EPOLLIN)) {
                struct server_vars * sv = (struct server_vars *)events[i].data.ptr;
                int ret = server.HandleReadEvent(sv);
                if (ret <= 0) {
                    mtcp_close(ctx->mctx, sv->sockfd);
                    if (++num_complete == num_accept) {
                        done = 1;
                    }
                }
                oks++;
            }
        }
    }

    db->Close();

    mtcp_destroy_context(ctx->mctx);
	pthread_exit(NULL);

    return NULL;
}

int main(const int argc, const char *argv[]) {
    int ret;
	struct mtcp_conf mcfg;
	int process_cpu;

    int num_cores, total_cores;

	total_cores = sysconf(_SC_NPROCESSORS_ONLN);
	num_cores = total_cores;
	process_cpu = -1;

	char conf_name[] = "server.conf";
	conf_file = conf_name;

	if (argc < 2) {
		fprintf(stdout, "$%s directory_to_service\n", argv[0]);
		return 0;
	}

    utils::Properties props;
	char s[20];
    
    for (int i = 0; i < argc; i++){
        long long unsigned n;
        char junk;
        if (sscanf(argv[i], "--num_cores=%llu%c", &n, &junk) == 1) {
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
        } else if (sscanf(argv[i], "--db=%s\n", s, &junk) == 1){
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

	/* initialize mtcp */
	if (conf_file == NULL) {
		fprintf(stderr, "You forgot to pass the mTCP startup config file!\n");
		exit(EXIT_FAILURE);
	}

	ret = mtcp_init(conf_file);
	if (ret) {
		fprintf(stderr, "Failed to initialize mtcp\n");
		exit(EXIT_FAILURE);
	}

	mtcp_getconf(&mcfg);
	
	/* register signal handler to mtcp */
	mtcp_register_signal(SIGINT, SignalHandler);
	mtcp_register_signal(SIGTERM, SignalHandler);

	for (int i = ((process_cpu == -1) ? 0 : process_cpu); i < num_cores; i++) {
		sv_thread_arg[i].core = i;
        sv_thread_arg[i].props = &props;
		
		if (pthread_create(&sv_thread[i], NULL, RunServerThread, (void *)&sv_thread_arg[i])) {
			perror("pthread_create() failed!");
		}
		if (process_cpu != -1)
			break;
	}

	for (int i = ((process_cpu == -1) ? 0 : process_cpu); i < num_cores; i++) {
		pthread_join(sv_thread[i], NULL);

		if (process_cpu != -1) {
			break;
		}
	}
	
	printf(" [%s] Test finished!\n", __func__);
	
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

