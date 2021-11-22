#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include "cetus.h"
#include "cetus_api.h"

#include "mthread.h"

#include "sail.h"

#define MAX_CONNECT 4100

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
DEFINE_PER_LCORE(struct cetus_epoll_event *, events);

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
    struct cetus_param * param = (struct cetus_param *)arg;

    parse_command_line(param->argc, param->argv);

    logging(INFO, " [%s on core %d] testing server on %d cores!", __func__, param->core_id, param->num_cores);

    epfd = cetus_epoll_create(0);
    if (epfd == -1) {
        logging(ERROR, " [%s on core %d] cetus_epoll_create failed!", __func__, lcore_id);
    } else {
        logging(INFO, " [%s on core %d] create epoll fd: %d", __func__, lcore_id, epfd);
    }

    int sock = cetus_socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        logging(ERROR, "cetus_socket() failed!");
        exit(EXIT_FAILURE);
    } else {
        logging(INFO, " [%s on core %d] create socket fd: %d", __func__, lcore_id, sock);
    }

    int ret;
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;

#ifndef RSS_IN_USE
    uint16_t port = lcore_id << 8;
#else
    uint16_t port = 1234;
#endif

    addr.sin_port = htons(port);

    if ((ret = cetus_bind(sock, (struct sockaddr *)&addr, sizeof(addr))) == -1) {
        logging(ERROR, "cetus_bind() failed!");
        exit(EXIT_FAILURE);
    } else {
        logging(INFO, " [%s on core %d] bind to port %u", __func__, lcore_id, port);
    }

    if((ret = cetus_listen(sock, 1024)) == -1) {
        logging(ERROR, "cetus_listen() failed!");
        exit(EXIT_FAILURE);
    } else {
        logging(INFO, " [%s on core %d] listen to port %u", __func__, lcore_id, port);
    }

    struct sockaddr_in client;
    socklen_t len = sizeof(client);
                
    int c;
    c = cetus_accept(sock, (struct sockaddr *)&client, &len);

    if(c <= 0) {
        logging(ERROR, " [%s on core %d] fail to accept connection", __func__, lcore_id);
        return NULL;
    } else {
        logging(INFO, " [%s on core %d] accept connection(%d)", __func__, lcore_id, c);
    }

    int send_len, recv_len;
    char recv_buff[buff_size];
    for (int i = 0; i < 1001; i++) {
        int bytesReadLeft, bytesRead;
        char * r;

        bytesReadLeft = buff_size;
        bytesRead = 0;
        r = recv_buff;
        while (bytesReadLeft > 0 && (bytesRead = cetus_read(c, r, bytesReadLeft)) > 0) {
            // logging(DEBUG, " [%s on core %d] recv len: %d, %.*s", __func__, lcore_id, bytesRead, bytesRead, r);
            // logging(DEBUG, " [%s on core %d] recv len: %d", __func__, lcore_id, bytesRead);
            bytesReadLeft -= bytesRead;
            r += bytesRead;
        }
        
        if (bytesReadLeft > 0 && bytesRead == 0) {
            logging(INFO, " \"end of file\" encountered on reading from socket");
        } else if (bytesRead == -1) {
            logging(INFO, " read: error encountered ");
            exit(401);
        }

        int bytesWriteLeft, bytesWrite;
        char * s;

        bytesWriteLeft = buff_size;
        bytesWrite = 0;
        s = recv_buff;
        while (bytesWriteLeft > 0 && (bytesWrite = cetus_write(c, s, bytesWriteLeft)) > 0) {
            // logging(DEBUG, " [%s on core %d] write len: %d, %.*s", __func__, lcore_id, bytesWrite, bytesWrite, s);
            // logging(DEBUG, " [%s on core %d] write len: %d", __func__, lcore_id, bytesWrite);
            bytesWriteLeft -= bytesWrite;
            s += bytesWrite;
        }
        
        if (bytesWrite == -1) {
            logging(INFO, " write: error encountered ");
            exit(401);
        }
    }

    return NULL;
}

int test_net(void * arg) {
    cetus_init((struct cetus_param *)arg);

    int ret;
    mthread_t mid;

    num_conn = 0;

    info = (struct conn_info *)calloc(MAX_CONNECT, sizeof(struct conn_info));

    // for (int i = 0; i < MAX_CONNECT; i++) {
    //     info[i].buff = (char *)calloc(1, BUFF_SIZE);
    // }
    
    events = (struct cetus_epoll_event *)calloc(NR_CETUS_EPOLL_EVENTS, CETUS_EPOLL_EVENT_SIZE);
    
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

#ifdef TIMESTAMP
    char * name = (char *)calloc(1, 32);

	snprintf(name, 31, "overhead.txt", lcore_id);
	FILE * fp = fopen(name, "a+");

    char * overhead = (char *)calloc(1, 512);

    for (int k = 0; k < ts_count; k++) {
        long long t[ts[k].count];
        for (int i = 0; i < ts[k].count; i++) {
            printf(" [%s] %llu sec, %llu usec\n", ts[k].func[i], ts[k].time[i].tv_sec, ts[k].time[i].tv_usec);
            t[i] = TIMEVAL_TO_USEC(ts[k].time[i]);
        }

        long long network_in = t[1] - t[0];
        long long network_out = t[9] - t[8];
        long long schedule = (t[3] - t[2]) + (t[7] - t[6]);
        long long context_switch = (t[2] - t[1]) + (t[4] - t[3]) + (t[6] - t[5]) + (t[8] - t[7]);
        long long application = t[5] - t[4];
        long long total = t[9] - t[0];

        // printf(" Packet %d total \t %5llu usec\n", k, total);
        // printf(" \t Network(in) \t %5llu usec\n", network_in);
        // printf(" \t Network(out) \t %5llu usec\n", network_out);
        // printf(" \t Schedule \t %5llu usec\n", schedule);
        // printf(" \t Context switch \t %5llu usec\n", context_switch);
        // printf(" \t Application \t %5llu usec\n", application);

        sprintf(overhead, " Total : %llu us | Network(in) : %llu us | Network(out) : %llu us | Schedule : %llu us | ContextSwitch : %llu us | App : %llu us\n", \
                        total, network_in, network_out, schedule, context_switch, application);
    
        fwrite(overhead, 1, strlen(overhead), fp);
    }

	fclose(fp);
#endif

    // long long recv_bytes, send_bytes;
    // recv_bytes = send_bytes = 0;

    // for (int i = 0; i < num_conn; i++) {
    //     recv_bytes += info[i].recv_bytes;
    //     send_bytes += info[i].send_bytes;
    // }

    // long long start_time = TIMEVAL_TO_USEC(start);
    // long long end_time = TIMEVAL_TO_USEC(end);
    // double total_time = (end_time - start_time) / 1000000.00;

    // char * file_name = (char *)calloc(1, 32);

	// snprintf(file_name, 31, "throughput_core_%d.txt", lcore_id);
	// FILE * thp_file = fopen(file_name, "a+");

    // char * result_buff = (char *)calloc(1, 512);

    // printf(" [PAYLOAD] recv bytes: %llu (Mb), send rate: %llu (Mb)\n", recv_bytes, send_bytes);
    
    // snprintf(result_buff, 511, " [PAYLOAD] connection: %d, recv rate: %.2f (Mbps), send rate: %.2f (Mbps)\n", 
    //         num_conn, (recv_bytes * 8.0) / (total_time * 1000 * 1000), (send_bytes * 8.0) / (total_time * 1000 * 1000));

    // printf("%s", result_buff);

	// fwrite(result_buff, 1, strlen(result_buff), thp_file);
    // fprintf(thp_file, " ====================\n");
	
	// fclose(thp_file);
}

int main(int argc, char ** argv) {
    struct cetus_param * param = cetus_config(argc, argv);

    cetus_spawn(test_net, param);

    logging(DEBUG, " [%s on core %d] test finished, return from main", __func__, lcore_id);

    return 0;
}