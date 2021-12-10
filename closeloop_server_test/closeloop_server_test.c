#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <sched.h>

#include "cygnus.h"
#include "cygnus_api.h"

#include "mthread.h"

#include "sail.h"

#define MAX_CONNECT 8200

#define BUFF_SIZE   8192

DEFINE_PER_LCORE(int, num_conn);
DEFINE_PER_LCORE(int, test_time);
DEFINE_PER_LCORE(int, buff_size);

struct conn_info {
    int         sockfd;
    int         epfd;

    long long   recv_bytes;
    long long   send_bytes;
    
    char        * buff;
    int         recv_ptr;
    int         send_ptr;
};

DEFINE_PER_LCORE(struct conn_info *, info);

DEFINE_PER_LCORE(int, start_flag);
DEFINE_PER_LCORE(struct timeval, start);
DEFINE_PER_LCORE(struct timeval, end);

DEFINE_PER_LCORE(int, timerfd);
DEFINE_PER_LCORE(int, wait_timerfd);

DEFINE_PER_LCORE(int, epfd);
DEFINE_PER_LCORE(struct cygnus_epoll_event *, events);

int handle_write_event(struct conn_info * conn_info) {
    if (conn_info->send_bytes == conn_info->recv_bytes) {
        return 0;
    }

    int len = cygnus_write(conn_info->sockfd, conn_info->buff + conn_info->recv_ptr, MIN(buff_size, conn_info->recv_bytes - conn_info->send_bytes));
    
    if (len < 0) {
        return -1;
    }
#if 0
    char name[32];

    sprintf(name, "../output_%d.dat", conn_info->sockfd);

    FILE * fp = fopen(name, "ab");

    fwrite(conn_info->file + conn_info->output_file_ptr, 1, len, fp);

    fclose(fp);
    #endif
    
    // logging(DEBUG, " [%s on core %d] send len: %d, send msg: %.*s", \
            __func__, lcore_id, len, len, conn_info->buff + conn_info->recv_ptr);

    conn_info->send_bytes += len;

    return 0;
}

int handle_read_event(struct conn_info * conn_info) {
    char buff[BUFF_SIZE];
    int recv_len = cygnus_read(conn_info->sockfd, buff, buff_size);
        
    if (recv_len < 0) {
        return -1;
    }

    conn_info->recv_bytes += recv_len;
    
    // logging(DEBUG, " >> recv len: %d, recv msg: %.*s", recv_len, recv_len, buff);

    int send_len = cygnus_write(conn_info->sockfd, buff, recv_len);
    
    if (send_len < 0) {
        return -1;
    }

    conn_info->send_bytes += send_len;

    return recv_len;
}

int parse_command_line(int argc, const char ** argv) {    
    for (int i = 0; i < argc; i++) {
        int n;
        char junk;
        
        if (sscanf(argv[i], "--test_time=%d\n", &n, &junk) == 1) {
            test_time = n;
            fprintf(stdout, " [%s on core %d] test for %d seconds\n", __func__, lcore_id, test_time);
        } else if (sscanf(argv[i], "--buff_size=%d\n", &n, &junk) == 1) {
            buff_size = n;
            fprintf(stdout, " [%s on core %d] buffer size: %d Bytes\n", __func__, lcore_id, buff_size);
        } 
    }

    return 0;
}

void * server_thread(void * arg) {
    struct cygnus_param * param = (struct cygnus_param *)arg;

    parse_command_line(param->argc, param->argv);

    logging(INFO, " [%s on core %d] testing server on %d cores!", __func__, param->core_id, param->num_cores);

    epfd = cygnus_epoll_create(0);
    if (epfd == -1) {
        logging(ERROR, " [%s on core %d] cygnus_epoll_create failed!", __func__, lcore_id);
    } else {
        logging(INFO, " [%s on core %d] create epoll fd: %d", __func__, lcore_id, epfd);
    }

    int sock = cygnus_socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        logging(ERROR, "cygnus_socket() failed!");
        exit(EXIT_FAILURE);
    } else {
        logging(INFO, " [%s on core %d] create socket fd: %d", __func__, lcore_id, sock);
    }

    if (cygnus_fcntl(sock, F_SETFL, O_NONBLOCK) == -1) {
        logging(ERROR, "cygnus_fcntl() set sock to non-block failed!");
        exit(EXIT_FAILURE);
    }

    int ret;
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;

#ifndef RSS_IN_USE
    uint16_t port = (lcore_id - 1) << 12;
#else
    uint16_t port = 1234;
#endif

    addr.sin_port = htons(port);

    if ((ret = cygnus_bind(sock, (struct sockaddr *)&addr, sizeof(addr))) == -1) {
        logging(ERROR, "cygnus_bind() failed!");
        exit(EXIT_FAILURE);
    } else {
        logging(INFO, " [%s on core %d] bind to port %u", __func__, lcore_id, port);
    }

    cygnus_create_flow(0, 0, 0, 0, 0, 0, port, 0xffff);

    if((ret = cygnus_listen(sock, 1024)) == -1) {
        logging(ERROR, "cygnus_listen() failed!");
        exit(EXIT_FAILURE);
    } else {
        logging(INFO, " [%s on core %d] listen to port %u", __func__, lcore_id, port);
    }

    struct cygnus_epoll_event ev;
    ev.events = CYGNUS_EPOLLIN;
    ev.data.fd = sock;

    if ((ret = cygnus_epoll_ctl(epfd, CYGNUS_EPOLL_CTL_ADD, sock, &ev)) == -1) {
        logging(ERROR, "cygnus_epoll_ctl() failed!");
        exit(EXIT_FAILURE);
    }

    int wait_timerfd = cygnus_timerfd_create(CLOCK_MONOTONIC, CYGNUS_TFD_NONBLOCK);
    if (wait_timerfd == -1) {
        logging(ERROR, " [%s on core %d] cygnus_timerfd_create wait timerfd failed!", __func__, lcore_id);
    } else {
        logging(INFO, " [%s on core %d] create wait timer fd: %d", __func__, lcore_id, timerfd);
    }

    struct itimerspec ts;
    ts.it_interval.tv_sec = 0;
	ts.it_interval.tv_nsec = 0;
	ts.it_value.tv_sec = param->test_time;
	ts.it_value.tv_nsec = 0;

	if (cygnus_timerfd_settime(wait_timerfd, 0, &ts, NULL) < 0) {
		logging(ERROR, "cygnus_timerfd_settime() failed");
        exit(EXIT_FAILURE);
	}

    ev.events = CYGNUS_EPOLLIN;
    ev.data.fd = wait_timerfd;
    if (ret = (cygnus_epoll_ctl(epfd, CYGNUS_EPOLL_CTL_ADD, wait_timerfd, &ev)) == -1) {
        logging(ERROR, " >> cygnus_epoll_ctl() error on listen sock");
        exit(EXIT_FAILURE);
    }

    char msg[64];
    int tot_recv = 0;

    
    //struct timeval epoll_wait_start;

    //struct timeval epoll_wait_end;
    //gettimeofday(&epoll_wait_end, NULL);

    while (1) {
        int nevent;

#ifdef DEBUG_EPOLLWAIT
        char name[32];

        sprintf(name, "../core_%d_epollwait.dat", lcore_id);

        FILE * fp = fopen(name, "ab");

        gettimeofday(&epoll_wait_start, NULL);
#endif

        //struct timeval wait_start;
        //gettimeofday(&wait_start, NULL);

        nevent = cygnus_epoll_wait(epfd, events, NR_CYGNUS_EPOLL_EVENTS, -1);
        if (nevent == -1) {
            logging(ERROR, " >> cygnus_epoll_wait return %d events!", nevent);
            exit(EXIT_FAILURE);
        }

        //struct timeval wait_end;
        //gettimeofday(&wait_end, NULL);

        //long long start_time = TIMEVAL_TO_USEC(wait_start);
        //long long end_time = TIMEVAL_TO_USEC(wait_end);
        //logging(DEBUG, " >> start sec: %lu, usec: %lu", wait_start.tv_sec, wait_start.tv_usec);
        //logging(DEBUG, " >> end sec: %lu, usec: %lu", wait_end.tv_sec, wait_end.tv_usec);
        //logging(DEBUG, " >> epoll wait: %llu (us), nevent: %d", end_time - start_time, nevent);

        //logging(INFO, " \t >> server receive %d events", nevent);

#ifdef DEBUG_EPOLLWAIT
        gettimeofday(&epoll_wait_end, NULL);

        fprintf(fp, "epollwait wait time: %lu (us)\n", TIMEVAL_TO_USEC(epoll_wait_end) - TIMEVAL_TO_USEC(epoll_wait_start));

        fclose(fp);
#endif

#ifdef DEBUG_PROCESS_TIME
        char name[32];

        sprintf(name, "../core_%d_process.dat", lcore_id);

        FILE * fp = fopen(name, "ab");

        struct timeval process_start;
        gettimeofday(&process_start, NULL);
#endif

        for (int i = 0; i < nevent; i++) {
            // logging(INFO, " \t >> event: %u, fd: %d", events[i].events, events[i].data.fd);
            if (events[i].data.fd == wait_timerfd) {
                logging(INFO, " [%s on core %d] wait time's up!", __func__, lcore_id);
                if (!num_conn) {
                    return NULL;
                } else {
                    continue;
                }
            }

            if (events[i].data.fd == timerfd) {
                logging(INFO, " [%s on core %d] time's up! End test", __func__, lcore_id);
                gettimeofday(&end, NULL);
                return NULL;
            }

            if (events[i].data.fd == sock) {
                
                //logging(INFO, " [%s on core %d] incoming connection on sock %d", __func__, lcore_id, sock);
                
                struct sockaddr_in client;
                socklen_t len = sizeof(client);
                
                int new_sock;
                new_sock = cygnus_accept(sock, (struct sockaddr *)&client, &len);

                if(new_sock > 0) {
                    //logging(DEBUG, " [%s on core %d] accept connection, create socket %d", __func__, lcore_id, new_sock);

                    if (cygnus_fcntl(new_sock, F_SETFL, O_NONBLOCK) == -1) {
                        logging(ERROR, "cygnus_fcntl() set sock to non-block failed!");
                        exit(EXIT_FAILURE);
                    }

                    struct conn_info * conn_info = &info[num_conn];
                    conn_info->sockfd = new_sock;
                    conn_info->epfd = epfd;
                    conn_info->recv_bytes = conn_info->send_bytes = 0;
                    conn_info->recv_ptr = conn_info->send_ptr = 0;
                    // conn_info->buff = (char *)calloc(1, BUFF_SIZE);
                    conn_info->buff = (char *)malloc(BUFF_SIZE);

                    num_conn++;

                    struct cygnus_epoll_event ev;
                    ev.events = CYGNUS_EPOLLIN;
                    ev.data.ptr = conn_info;
                    
                    if (ret = (cygnus_epoll_ctl(epfd, CYGNUS_EPOLL_CTL_ADD, new_sock, &ev)) == -1) {
                        logging(ERROR, " >> cygnus_epoll_ctl() error on accept soc");
                        exit(EXIT_FAILURE);
                    }
                }/* else {
                    logging(ERROR, " [%s on core %d] accept return %d", __func__, lcore_id, new_sock);
                }*/

                if (!start_flag) {
                    gettimeofday(&start, NULL);
                    start_flag = 1;

                    timerfd = cygnus_timerfd_create(CLOCK_MONOTONIC, CYGNUS_TFD_NONBLOCK);
                    if (timerfd == -1) {
                        logging(ERROR, " [%s on core %d] cygnus_timerfd_create failed!", __func__, lcore_id);
                    } else {
                        logging(INFO, " [%s on core %d] create timer fd: %d", __func__, lcore_id, timerfd);
                    }

                    struct itimerspec ts;
                    ts.it_interval.tv_sec = 0;
                	ts.it_interval.tv_nsec = 0;
                	ts.it_value.tv_sec = param->test_time;
                	ts.it_value.tv_nsec = 0;

                	if (cygnus_timerfd_settime(timerfd, 0, &ts, NULL) < 0) {
                		logging(ERROR, "cygnus_timerfd_settime() failed");
                        exit(EXIT_FAILURE);
	                }

                    ev.events = CYGNUS_EPOLLIN;
                    ev.data.fd = timerfd;
                    if (ret = (cygnus_epoll_ctl(epfd, CYGNUS_EPOLL_CTL_ADD, timerfd, &ev)) == -1) {
                        logging(ERROR, " >> cygnus_epoll_ctl() error on listen sock");
                        exit(EXIT_FAILURE);
                    }
                }

                continue;
            }

            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            if (events[i].events == CYGNUS_EPOLLIN) {
                handle_read_event(info);
            }/* else if (events[i].events == CYGNUS_EPOLLOUT) {
                handle_write_event(info);
            }*/ else if (events[i].events == CYGNUS_EPOLLERR) {
                cygnus_close(info->sockfd);
            }

        }
#ifdef DEBUG_PROCESS_TIME
        struct timeval process_end;
        gettimeofday(&process_end, NULL);

        fprintf(fp, " process time: %lu (us)\n", TIMEVAL_TO_USEC(process_end) - TIMEVAL_TO_USEC(process_start));

        fclose(fp);
#endif
    }

    return NULL;
}

void * test_net(void * arg) {
    cygnus_init((struct cygnus_param *)arg);

    int ret;
    mthread_t mid;

    num_conn = 0;

    info = (struct conn_info *)calloc(MAX_CONNECT, sizeof(struct conn_info));

    // for (int i = 0; i < MAX_CONNECT; i++) {
    //     info[i].buff = (char *)calloc(1, BUFF_SIZE);
    // }
    
    events = (struct cygnus_epoll_event *)calloc(NR_CYGNUS_EPOLL_EVENTS, CYGNUS_EPOLL_EVENT_SIZE);
    
    sail_init();
    
    /* Create polling thread */
    if((ret = mthread_create(&mid, NULL, server_thread, arg)) < 0) {
        printf("mthread_create() error: %d\n", ret);
        exit(1);
    } else {
        logging(INFO, "[%s on core %d] server_thread create done(mid: %lu)", __func__, lcore_id, mid);
    }

    /* Test mthread_join */
    if ((ret = mthread_join(mid, NULL)) < 0) {
        printf("mthread_join() error: %d\n", ret);
        exit(1);
    }

    logging(INFO, "[%s on core %d] mthread %lu joined!", __func__, lcore_id, mid);

    sail_exit();

    logging(INFO, " ========== [core %d] Test end ==========", lcore_id);

    long long recv_bytes, send_bytes;
    recv_bytes = send_bytes = 0;

    for (int i = 0; i < num_conn; i++) {
        recv_bytes += info[i].recv_bytes;
        send_bytes += info[i].send_bytes;
    }

    long long start_time = TIMEVAL_TO_USEC(start);
    long long end_time = TIMEVAL_TO_USEC(end);
    double total_time = (end_time - start_time) / 1000000.00;

    char * file_name = (char *)calloc(1, 32);

	snprintf(file_name, 31, "throughput_core_%d.txt", lcore_id);
	FILE * thp_file = fopen(file_name, "a+");

    char * result_buff = (char *)calloc(1, 512);

    printf(" [PAYLOAD] recv bytes: %llu (Mb), send rate: %llu (Mb)\n", recv_bytes, send_bytes);
    
    snprintf(result_buff, 511, " [PAYLOAD] connection: %d, recv rate: %.2f (Mbps), send rate: %.2f (Mbps)\n", 
            num_conn, (recv_bytes * 8.0) / (total_time * 1000 * 1000), (send_bytes * 8.0) / (total_time * 1000 * 1000));

    printf("%s", result_buff);

	fwrite(result_buff, 1, strlen(result_buff), thp_file);
    fprintf(thp_file, " ====================\n");
	
	fclose(thp_file);

    return NULL;
}

int main(int argc, char ** argv) {
    struct cygnus_param * param = cygnus_config(argc, argv);

    cygnus_spawn(test_net, param);
    // config.io_config.io_module->init_module(&config.io_config);

    // start_core(test_net, param);

    logging(DEBUG, " [%s on core %d] test finished, return from main", __func__, lcore_id);

    return 0;
}