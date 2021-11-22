#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include "cetus.h"
#include "cetus_api.h"

#include "mthread.h"

#define BUFF_SIZE   8192

DEFINE_PER_LCORE(int, num_conn);

struct conn_info {
    int     sockfd;
    int     epfd;

    char    * input_file;
    int     input_file_ptr;

    char    * recv_buff;
    int     recv_buff_len;

    unsigned long   recv_bytes;
    unsigned long   send_bytes;
};

DEFINE_PER_LCORE(char *, file);
DEFINE_PER_LCORE(struct conn_info *, info);

int connect_server(int epfd, char * server_ip, int port) {
    int sock;

    struct sockaddr_in server_addr;

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(server_ip);

    sock = cetus_socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        log(ERROR, "cetus_socket() failed!");
        exit(EXIT_FAILURE);
    }/* else {
        log(INFO, " [%s on core %d] create socket fd: %d", __func__, lcore_id, sock);
    }*/

    //log(INFO, " [%s on core %d] connecting to server %s:%u...", __func__, lcore_id, server_ip, port);

    if(cetus_connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0){
        log(ERROR, " [%s on core %d] connect server failed!", __func__, lcore_id);
        exit(1);
    }

    if (cetus_fcntl(sock, F_SETFL, O_NONBLOCK) == -1) {
        log(ERROR, "cetus_fcntl() set sock to non-block failed!");
        exit(EXIT_FAILURE);
    }
    
    log(INFO, " [%s on core %d] sock %d connect server %s:%u", __func__, lcore_id, sock, server_ip, port);

    struct conn_info * conn_info = &info[num_conn];
    conn_info->sockfd = sock;
    conn_info->epfd = epfd;

    conn_info->input_file = file;
    conn_info->input_file_ptr = 0;
    
    conn_info->recv_buff = (char *)malloc(BUFF_SIZE);
    conn_info->recv_buff_len = 0;

    num_conn++;

    struct cetus_epoll_event ev;
    ev.events = CETUS_EPOLLIN | CETUS_EPOLLOUT;
    ev.data.ptr = conn_info;

    int ret;

    if ((ret = cetus_epoll_ctl(epfd, CETUS_EPOLL_CTL_ADD, sock, &ev)) == -1) {
        log(ERROR, "cetus_epoll_ctl() failed!");
        exit(EXIT_FAILURE);
    }

    return sock;
}

int handle_read_event(struct conn_info * conn_info) {
    char buff[BUFF_SIZE];

    int len = cetus_read(conn_info->sockfd, buff, buff_size);
    
    if (len < 0) {
        return -1;
    }

    //log(DEBUG, " [%s on core %d] recv len: %d, recv msg: %.*s", __func__, lcore_id, len, len, conn_info->recv_buff + conn_info->recv_buff_len);

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

    //conn_info->recv_buff_len = (conn_info->recv_buff_len + len) % BUFF_SIZE;
    conn_info->recv_bytes += len;

    return len;
}

int handle_write_event(struct conn_info * conn_info) {
    if (conn_info->input_file_ptr + buff_size >= M_128) {
        conn_info->input_file_ptr = 0;
    }

    int len = cetus_write(conn_info->sockfd, conn_info->input_file + conn_info->input_file_ptr, buff_size);
    
    if (len < 0) {
        return -1;
    }

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

    //log(DEBUG, " >> send len: %d, send msg: %.*s\n", len, len, conn_info->input_file + conn_info->input_file_ptr);
    
    conn_info->input_file_ptr += len;
    conn_info->send_bytes += len;

    return len;
}

void * client_thread(void * arg) {
    struct cetus_param * param = (struct cetus_param *)arg;

    log(INFO, " [%s on core %d] testing client!", __func__, lcore_id);

    int epfd;
    epfd = cetus_epoll_create(0);
    if (epfd == -1) {
        log(ERROR, " [%s on core %d] cetus_epoll_create failed!", __func__, lcore_id);
    } else {
        log(INFO, " [%s on core %d] create epoll fd: %d", __func__, lcore_id, epfd);
    }

    int timerfd = cetus_timerfd_create(CLOCK_MONOTONIC, CETUS_TFD_NONBLOCK);
    if (timerfd == -1) {
        log(ERROR, " [%s on core %d] cetus_timerfd_create failed!", __func__, lcore_id);
    } else {
        log(INFO, " [%s on core %d] create timer fd: %d", __func__, lcore_id, timerfd);
    }

    struct itimerspec ts;
    ts.it_interval.tv_sec = 0;
	ts.it_interval.tv_nsec = 0;
	ts.it_value.tv_sec = param->test_time;
	ts.it_value.tv_nsec = 0;

	if (cetus_timerfd_settime(timerfd, 0, &ts, NULL) < 0) {
		log(ERROR, "cetus_timerfd_settime() failed");
        exit(EXIT_FAILURE);
	}

    int ret;

    struct cetus_epoll_event ev;
    ev.events = CETUS_EPOLLIN | CETUS_EPOLLOUT;
    ev.data.fd = timerfd;
    if (ret = (cetus_epoll_ctl(epfd, CETUS_EPOLL_CTL_ADD, timerfd, &ev)) == -1) {
        log(ERROR, "epoll_ctl: listen_sock");
        exit(EXIT_FAILURE);
    }

    char name[32];

    struct cetus_epoll_event * events = (struct cetus_epoll_event *)calloc(NR_CETUS_EPOLL_EVENTS, CETUS_EPOLL_EVENT_SIZE);
    while (1) {
        while(num_conn < 1) {
            if((connect_server(epfd, "10.0.0.1", 1234)) < 0) {
                break;
            }

            //log(DEBUG, " [%s on core %d] connected to server", __func__, lcore_id);
        }

        //struct timeval wait_start;
        //gettimeofday(&wait_start, NULL);

        int nevent;
        nevent = cetus_epoll_wait(epfd, events, NR_CETUS_EPOLL_EVENTS, -1);
        if (nevent == -1) {
            log(ERROR, " [%s on core %d] cetus_epoll_wait return %d events!", __func__, lcore_id, nevent);
            exit(EXIT_FAILURE);
        }

        //struct timeval wait_end;
        //gettimeofday(&wait_end, NULL);

        //long long start_time = TIMEVAL_TO_USEC(wait_start);
        //long long end_time = TIMEVAL_TO_USEC(wait_end);
        //log(DEBUG, " >> epoll wait: %llu (us), nevent: %d", end_time - start_time, nevent);
        log(DEBUG, " >> nevent: %d", nevent);

        for (int i = 0; i < nevent; i++) {
            if (events[i].data.fd == timerfd) {
                log(INFO, " [%s on core %d] time's up! End test", __func__, lcore_id);
                goto done;
            }

            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            //log(INFO, " [%s on core %d] recv event %u on sock %d", __func__, lcore_id, events[i].events, info->sockfd);
            if (events[i].events == CETUS_EPOLLIN) {
                handle_read_event(info);
            } else if (events[i].events == CETUS_EPOLLOUT) {
                handle_write_event(info);
            } else if (events[i].events == CETUS_EPOLLERR) {
                cetus_close(info->sockfd);
            }
        }
    }
done:
    //log(DEBUG, " [%s on core %d] close sock %d", __func__, lcore_id, sock);
    //cetus_close(sock);

    //cetus_close(epfd);

    snprintf(name, 31, "../conn_%d.dat", lcore_id);

    FILE * fp = fopen(name, "ab");

    for (int i = 0; i < num_conn; i++) {
        char buff[2048];
        snprintf(buff, 2047, " sock %d recv bytes: %lu, send bytes: %lu\n", \
                    info[i].sockfd, info[i].recv_bytes, info[i].send_bytes);
    
        fwrite(buff, 1, strlen(buff), fp);
    }

    fclose(fp);
    
    return 0;
}

int test_net(void * arg) {
    cetus_init((struct cetus_param *)arg);

    int ret;
    mthread_t mid;

    num_conn = 0;

    info = (struct conn_info *)calloc(2048, sizeof(struct conn_info));

    file = (char *)malloc(M_128);

    FILE * fp = fopen("../input.dat", "rb");

    fread(file, 1, M_128, fp);

    fclose(fp);

    /* Create polling thread */
    if((ret = mthread_create(&mid, NULL, client_thread, arg)) < 0) {
        printf("mthread_create() error: %d\n", ret);
        exit(1);
    } else {
        log(INFO, "[%s on core %d] client_thread create done(mid: %lu)", __func__, lcore_id, mid);
    }

    /* Test mthread_join */
    if ((ret = mthread_join(mid, NULL)) < 0) {
        printf("mthread_join() error: %d\n", ret);
        exit(1);
    }

    log(INFO, "[%s on core %d] mthread %lu joined!", __func__, lcore_id, mid);
}

int main(int argc, char ** argv) {
    struct cetus_param * param = cetus_config(argc, argv);

    cetus_spawn(test_net, param);

    return 0;
}