#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include "cygnus.h"
#include "cygnus_api.h"

#include "mthread.h"

#include "sail.h"

#define MAX_CONNECT 1200

#define BUFF_SIZE   8192

#define EVAL_RTT

#ifdef EVAL_RTT
__thread long long * rtt_buff;
__thread int rtt_buff_len;
__thread FILE * rtt_file;
#endif

DEFINE_PER_LCORE(int, num_conn);
DEFINE_PER_LCORE(int, test_time);
DEFINE_PER_LCORE(int, num_server_cores);
DEFINE_PER_LCORE(int, buff_size);

DEFINE_PER_LCORE(int, epfd);
DEFINE_PER_LCORE(struct cygnus_epoll_event *, events);

struct conn_info {
    int     sockfd;
    int     epfd;

    char    * input_file;
    int     input_file_ptr;

    char    * recv_buff;
    int     recv_buff_len;

    long long   recv_bytes;
    long long   send_bytes;

#ifdef EVAL_RTT
    struct timeval start;
#endif
};

DEFINE_PER_LCORE(char *, file);
DEFINE_PER_LCORE(struct conn_info *, info);

DEFINE_PER_LCORE(int, start_flag);
DEFINE_PER_LCORE(struct timeval, start);
DEFINE_PER_LCORE(struct timeval, end);

int connect_server(int epfd, char * server_ip, uint16_t port) {
    int sock;

    struct sockaddr_in server_addr;

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(server_ip);

    sock = cygnus_socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        logging(ERROR, "cygnus_socket() failed!");
        exit(EXIT_FAILURE);
    }/* else {
        logging(INFO, " [%s on core %d] create socket fd: %d", __func__, lcore_id, sock);
    }*/

    // logging(INFO, " [%s on core %d] connecting to server %s:%x...", __func__, lcore_id, server_ip, port);

    if(cygnus_connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0){
        logging(ERROR, " [%s on core %d] connect server failed!", __func__, lcore_id);
        exit(1);
    }

    if (cygnus_fcntl(sock, F_SETFL, O_NONBLOCK) == -1) {
        logging(ERROR, "cygnus_fcntl() set sock to non-block failed!");
        exit(EXIT_FAILURE);
    }
    
    //logging(INFO, " [%s on core %d] sock %d connect server %s:%u", __func__, lcore_id, sock, server_ip, port);

    struct conn_info * conn_info = &info[num_conn];
    conn_info->sockfd = sock;
    conn_info->epfd = epfd;

    conn_info->input_file = file;
    conn_info->input_file_ptr = 0;
    
    // conn_info->recv_buff = (char *)calloc(1, BUFF_SIZE);
    conn_info->recv_buff = (char *)malloc(buff_size);
    conn_info->recv_buff_len = 0;

    num_conn++;

    if (!start_flag) {
        gettimeofday(&start, NULL);
        start_flag = 1;
    }

    struct cygnus_epoll_event ev;
    ev.events = CYGNUS_EPOLLIN | CYGNUS_EPOLLOUT;
    ev.data.ptr = conn_info;

    int ret;

    if ((ret = cygnus_epoll_ctl(epfd, CYGNUS_EPOLL_CTL_ADD, sock, &ev)) == -1) {
        logging(ERROR, "cygnus_epoll_ctl() failed!");
        exit(EXIT_FAILURE);
    }

    return sock;
}

int handle_read_event(struct conn_info * conn_info) {
#ifdef DEBUG_THP
    struct timeval curr;
    gettimeofday(&curr, NULL);
    logging(DEBUG, " [%s on core %d] sec: %lu, usec: %lu", __func__, lcore_id, curr.tv_sec, curr.tv_usec);
#endif
    char buff[buff_size];

    int len = cygnus_read(conn_info->sockfd, buff, buff_size);
    
    if (len < 0) {
        return -1;
    }

    // logging(DEBUG, " [%s on core %d] recv len: %d, recv msg: %.*s", __func__, lcore_id, len, len, buff);
    // struct timeval curr;
    // gettimeofday(&curr, NULL);
    // logging(DEBUG, " [%s on core %d] recv len: %d at %llu", __func__, lcore_id, len, TIMEVAL_TO_USEC(curr));

#ifdef DEBUG_OUTPUT
    char name[32];

    sprintf(name, "../output_sock_%d_core_%d.dat", conn_info->sockfd, lcore_id);

    FILE * fp = fopen(name, "ab");

    fwrite(conn_info->recv_buff + conn_info->recv_buff_len, 1, len, fp);
/*
    char buff[2048];
    sprintf(buff, " >> recv len: %d, recv msg: %.*s\n", len, len, conn_info->output_file + conn_info->output_file_ptr);
    
    fwrite(buff, 1, strlen(buff), fp);
*/
    fclose(fp);
#endif

    conn_info->recv_bytes += len;

    if (conn_info->send_bytes == conn_info->recv_bytes) {
#ifdef EVAL_RTT
        if (rtt_buff_len < M_1) {
            struct timeval curr;
            gettimeofday(&curr, NULL);

            long long elapsed;
            if (curr.tv_usec < conn_info->start.tv_usec) {
                elapsed = 1000000 + curr.tv_usec - conn_info->start.tv_usec;
            } else {
                elapsed = curr.tv_usec - conn_info->start.tv_usec;
            }
            
            rtt_buff[rtt_buff_len++] = elapsed;
        }
#endif
        //logging(DEBUG, " >> re-enable write event(input ptr: %u, output ptr: %u)", conn_info->input_file_ptr, conn_info->output_file_ptr);
        int ret;
        struct cygnus_epoll_event ev;
        ev.events = CYGNUS_EPOLLIN | CYGNUS_EPOLLOUT;
        ev.data.ptr = conn_info;
        if (ret = (cygnus_epoll_ctl(conn_info->epfd, CYGNUS_EPOLL_CTL_MOD, conn_info->sockfd, &ev)) == -1) {
            logging(ERROR, "epoll_ctl: wait receive");
            exit(EXIT_FAILURE);
        }
    }

    return len;
}

int handle_write_event(struct conn_info * conn_info) {
#if 0
    struct timeval curr;
    gettimeofday(&curr, NULL);
    logging(DEBUG, " [%s on core %d] sec: %lu, usec: %lu", __func__, lcore_id, curr.tv_sec, curr.tv_usec);
#endif
    if (conn_info->input_file_ptr + buff_size >= M_128) {
        conn_info->input_file_ptr = 0;
    }

    int len = cygnus_write(conn_info->sockfd, conn_info->input_file + conn_info->input_file_ptr, buff_size);
    
    if (len < 0) {
        return -1;
    }

#ifdef EVAL_RTT
    if (rtt_buff_len < M_1) {
        gettimeofday(&conn_info->start, NULL);
    }
#endif

#ifdef DEBUG_OUTPUT
    char name[32];

    sprintf(name, "../input_sock_%d_core_%d.dat", conn_info->sockfd, lcore_id);

    FILE * fp = fopen(name, "ab");

    fwrite(conn_info->input_file + conn_info->input_file_ptr, 1, len, fp);
/*    
    char buff[2048];
    sprintf(buff, " >> send len: %d, send msg: %.*s\n", len, len, conn_info->input_file + conn_info->input_file_ptr);
    
    fwrite(buff, 1, strlen(buff), fp);
*/
    fclose(fp);
#endif

    //logging(DEBUG, " >> send len: %d, send msg: %.*s", len, len, conn_info->input_file + conn_info->input_file_ptr);
    //logging(DEBUG, " [%s on core %d] send data len: %d", __func__, lcore_id, len);
    
    conn_info->input_file_ptr += len;
    conn_info->send_bytes += len;

    int ret;
    struct cygnus_epoll_event ev;
    ev.events = CYGNUS_EPOLLIN;
    ev.data.ptr = conn_info;
    if (ret = (cygnus_epoll_ctl(conn_info->epfd, CYGNUS_EPOLL_CTL_MOD, conn_info->sockfd, &ev)) == -1) {
        exit(EXIT_FAILURE);
    }/* else {
        logging(DEBUG, " >> wait for receive(input ptr: %u, output ptr: %u)", conn_info->input_file_ptr, conn_info->output_file_ptr);
    }*/

    return len;
}

int parse_command_line(int argc, const char ** argv) {    
    for (int i = 0; i < argc; i++) {
        int n;
        char junk;
        
        if (sscanf(argv[i], "--test_time=%d\n", &n, &junk) == 1) {
            test_time = n;
            fprintf(stdout, " [%s on core %d] test for %d seconds\n", __func__, lcore_id, test_time);
        } else if (sscanf(argv[i], "--num_server_cores=%d\n", &n, &junk) == 1) {
            num_server_cores = n;
            fprintf(stdout, " [%s on core %d] %d core(s) on server side\n", __func__, lcore_id, num_server_cores);
        } else if (sscanf(argv[i], "--buff_size=%d\n", &n, &junk) == 1) {
            buff_size = n;
            fprintf(stdout, " [%s on core %d] buffer size: %d Bytes\n", __func__, lcore_id, buff_size);
        } 
    }

    return 0;
}

void * client_thread(void * arg) {
    struct cygnus_param * param = (struct cygnus_param *)arg;

    parse_command_line(param->argc, param->argv);

    logging(INFO, " [%s on core %d] testing client!", __func__, lcore_id);

    epfd = cygnus_epoll_create(0);
    if (epfd == -1) {
        logging(ERROR, " [%s on core %d] cygnus_epoll_create failed!", __func__, lcore_id);
    } else {
        logging(INFO, " [%s on core %d] create epoll fd: %d", __func__, lcore_id, epfd);
    }

    int timerfd = cygnus_timerfd_create(CLOCK_MONOTONIC, CYGNUS_TFD_NONBLOCK);
    if (timerfd == -1) {
        logging(ERROR, " [%s on core %d] cygnus_timerfd_create failed!", __func__, lcore_id);
    } else {
        logging(INFO, " [%s on core %d] create timer fd: %d", __func__, lcore_id, timerfd);
    }

    struct itimerspec ts;
    ts.it_interval.tv_sec = 0;
	ts.it_interval.tv_nsec = 0;
	ts.it_value.tv_sec = test_time;
	ts.it_value.tv_nsec = 0;

	if (cygnus_timerfd_settime(timerfd, 0, &ts, NULL) < 0) {
		logging(ERROR, "cygnus_timerfd_settime() failed");
        exit(EXIT_FAILURE);
	}

    int ret;

    struct cygnus_epoll_event ev;
    ev.events = CYGNUS_EPOLLIN;
    ev.data.fd = timerfd;
    if (ret = (cygnus_epoll_ctl(epfd, CYGNUS_EPOLL_CTL_ADD, timerfd, &ev)) == -1) {
        logging(ERROR, "epoll_ctl: listen_sock");
        exit(EXIT_FAILURE);
    }

#ifndef RSS_IN_USE
        uint16_t port = (lcore_id % num_server_cores) << 12;
#else
        uint16_t port = 1234;
#endif

    char name[32];

    cygnus_create_flow(0, 0, 0, 0, port, 0xf000, (lcore_id - 1) << 12, 0xf000);

    while (1) {
        while(num_conn < param->num_flows) {
            if((connect_server(epfd, "10.0.0.1", port)) < 0) {
                break;
            }

            //logging(DEBUG, " [%s on core %d] connected to server", __func__, lcore_id);
        }

        int nevent;
        //struct timeval wait_start;
        //gettimeofday(&wait_start, NULL);
        nevent = cygnus_epoll_wait(epfd, events, NR_CYGNUS_EPOLL_EVENTS, -1);
        if (nevent == -1) {
            logging(ERROR, " [%s on core %d] cygnus_epoll_wait return %d events!", __func__, lcore_id, nevent);
            exit(EXIT_FAILURE);
        }
        //struct timeval wait_end;
        //gettimeofday(&wait_end, NULL);

        //long long start_time = TIMEVAL_TO_USEC(wait_start);
        //long long end_time = TIMEVAL_TO_USEC(wait_end);
        //logging(DEBUG, " >> epoll wait: %llu (us), nevent: %d", end_time - start_time, nevent);
        //logging(DEBUG, " >> nevent: %d", nevent);

        for (int i = 0; i < nevent; i++) {
            if (events[i].data.fd == timerfd) {
                logging(INFO, " [%s on core %d] time's up! End test", __func__, lcore_id);
                goto done;
            }

            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            //logging(INFO, " [%s on core %d] recv event %u on sock %d", __func__, lcore_id, events[i].events, info->sockfd);
            if (events[i].events == CYGNUS_EPOLLIN) {
                handle_read_event(info);
            } else if (events[i].events == CYGNUS_EPOLLOUT) {
                handle_write_event(info);
            } else if (events[i].events == CYGNUS_EPOLLERR) {
                cygnus_close(info->sockfd);
            }
        }
    }
done:
    //logging(DEBUG, " [%s on core %d] close sock %d", __func__, lcore_id, sock);
    //cygnus_close(sock);

    //cygnus_close(epfd);
#if 0
    sprintf(name, "../conn_%d.dat", lcore_id);

    FILE * fp = fopen(name, "ab");

    for (int i = 0; i < num_conn; i++) {
        char buff[2048];
        sprintf(buff, " sock %d recv bytes: %lu, send bytes: %lu\n", \
                    info[i].sockfd, info[i].recv_bytes, info[i].send_bytes);
    
        fwrite(buff, 1, strlen(buff), fp);
    }

    fclose(fp);
#endif

    return 0;
}

int test_net(void * arg) {
    cygnus_init((struct cygnus_param *)arg);

    int ret;
    mthread_t mid;

    num_conn = 0;

    // info = (struct conn_info *)calloc(MAX_CONNECT, sizeof(struct conn_info));
    info = (struct conn_info *)calloc(MAX_CONNECT, sizeof(struct conn_info));

    // for (int i = 0; i < MAX_CONNECT; i++) {
    //     info[i].recv_buff = (char *)calloc(1, BUFF_SIZE);
    // }

    file = (char *)malloc(M_128);

    FILE * fp = fopen("input.dat", "rb");

    fread(file, 1, M_128, fp);

    fclose(fp);

    events = (struct cygnus_epoll_event *)calloc(NR_CYGNUS_EPOLL_EVENTS, CYGNUS_EPOLL_EVENT_SIZE);

#ifdef EVAL_RTT
    char * name = (char *)aligned_alloc(16, 32);
    sprintf(name, "rtt_core_%d.txt", lcore_id);
    rtt_file = fopen(name, "wb");
    fseek(rtt_file, 0, SEEK_END);

    rtt_buff = (long long *)calloc(M_1, sizeof(long long));
    rtt_buff_len = 0;
#endif

    sail_init();

    /* Create polling thread */
    if((ret = mthread_create(&mid, NULL, client_thread, arg)) < 0) {
        printf("mthread_create() error: %d\n", ret);
        exit(1);
    } else {
        logging(INFO, "[%s on core %d] client_thread create done(mid: %lu)", __func__, lcore_id, mid);
    }

    /* Test mthread_join */
    if ((ret = mthread_join(mid, NULL)) < 0) {
        printf("mthread_join() error: %d\n", ret);
        exit(1);
    }

    logging(INFO, "[%s on core %d] mthread %lu joined!", __func__, lcore_id, mid);
    
    sail_exit();

    gettimeofday(&end, NULL);

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

    printf(" [PAYLOAD] recv bytes: %llu (Mb), send rate: %llu (Mb)\n", recv_bytes, send_bytes);

    char * result_buff = (char *)calloc(1, 512);

    snprintf(result_buff, 511, " [PAYLOAD] recv rate: %.2f (Mbps), send rate: %.2f (Mbps)\n", 
            (recv_bytes * 8.0) / (total_time * 1000 * 1000), (send_bytes * 8.0) / (total_time * 1000 * 1000));

    printf("%s", result_buff);

    fwrite(result_buff, 1, strlen(result_buff), thp_file);

    fprintf(thp_file, " ====================\n");
	
	fclose(thp_file);

#ifdef EVAL_RTT
    for (int i = 0; i < rtt_buff_len; i++) {
        fprintf(rtt_file, "%llu\n", rtt_buff[i]);
        fflush(rtt_file);
    }

    fclose(rtt_file);
#endif
}

int main(int argc, char ** argv) {
    struct cygnus_param * param = cygnus_config(argc, argv);

    cygnus_spawn(test_net, param);
    // config.io_config.io_module->init_module(&config.io_config);

    // start_core(test_net, param);

    logging(DEBUG, " [%s on core %d] test finished, return from main", __func__, lcore_id);

    return 0;
}