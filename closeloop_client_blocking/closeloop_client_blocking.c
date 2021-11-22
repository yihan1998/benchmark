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

#ifdef EVAL_RTT
__thread long long * rtt_buff;
__thread int rtt_buff_len;
__thread FILE * rtt_file;
#endif

DEFINE_PER_LCORE(int, num_conn);
DEFINE_PER_LCORE(int, test_time);
DEFINE_PER_LCORE(int, buff_size);

DEFINE_PER_LCORE(char *, input_file);
DEFINE_PER_LCORE(int, input_file_ptr);
DEFINE_PER_LCORE(long long, recv_bytes);
DEFINE_PER_LCORE(long long, send_bytes);

__thread int ts_count;
__thread char ts[1000][544];

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

DEFINE_PER_LCORE(int, start_flag);
DEFINE_PER_LCORE(struct timeval, start);
DEFINE_PER_LCORE(struct timeval, end);

int connect_server(char * server_ip, uint16_t port) {
    int sock;

    struct sockaddr_in server_addr;

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(server_ip);

    sock = cetus_socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        logging(ERROR, "cetus_socket() failed!");
        exit(EXIT_FAILURE);
    }/* else {
        logging(INFO, " [%s on core %d] create socket fd: %d", __func__, lcore_id, sock);
    }*/

    //logging(INFO, " [%s on core %d] connecting to server %s:%u...", __func__, lcore_id, server_ip, port);

    if(cetus_connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0){
        logging(ERROR, " [%s on core %d] connect server failed!", __func__, lcore_id);
        exit(1);
    }

    num_conn++;

    if (!start_flag) {
        gettimeofday(&start, NULL);
        start_flag = 1;
    }

    return sock;
}

int read_data(int sock) {
#ifdef TIMESTAMP_CLIENT
    char * buff;
    char tmp_buff[buff_size];
    if (ts_count < 1000) {
        buff = &ts[ts_count];
    } else {
        buff = tmp_buff;
    }
#else
    char buff[buff_size];
#endif
    // int len = cetus_read(conn_info->sockfd, buff, buff_size);
    int bytesReadLeft, bytesRead;
    char * r;

    bytesReadLeft = buff_size;
    bytesRead = 0;
    r = buff;
    while (bytesReadLeft > 0 && (bytesRead = cetus_read(sock, r, bytesReadLeft)) > 0) {
        // logging(DEBUG, " [%s on core %d] recv len: %d, %.*s", __func__, lcore_id, bytesRead, bytesRead, r);
        // logging(DEBUG, " [%s on core %d] recv len: %d", __func__, lcore_id, bytesRead);
        bytesReadLeft -= bytesRead;
        r += bytesRead;
        recv_bytes += bytesRead;
    }

#ifdef TIMESTAMP_CLIENT
    if (ts_count < 1000) {
        ts_count++;
    }
#endif  // TIMESTAMP_CLIENT
    
    if (bytesReadLeft > 0 && bytesRead == 0) {
        logging(INFO, " \"end of file\" encountered on reading from socket");
    } else if (bytesRead == -1) {
        logging(INFO, " read: error encountered ");
        exit(401);
    }

    return buff_size;
}

int write_data(int sock) {
    int bytesWriteLeft, bytesWrite;
    char * s;

    bytesWriteLeft = buff_size;
    bytesWrite = 0;

#ifndef TIMESTAMP_CLIENT
    if (input_file_ptr + buff_size >= M_128) {
        input_file_ptr = 0;
    }
    s = input_file + input_file_ptr;
#else
    char buff[buff_size];
    memset(buff, 0, buff_size);
    s = buff;
#endif  // TIMESTAMP_CLIENT

    while (bytesWriteLeft > 0 && (bytesWrite = cetus_write(sock, s, bytesWriteLeft)) > 0) {
        // logging(DEBUG, " [%s on core %d] write len: %d, %.*s", __func__, lcore_id, bytesWrite, bytesWrite, s);
        // logging(DEBUG, " [%s on core %d] write len: %d", __func__, lcore_id, bytesWrite);
        bytesWriteLeft -= bytesWrite;
        s += bytesWrite;

#ifndef TIMESTAMP_CLIENT
        input_file_ptr += bytesWrite;
#endif  // TIMESTAMP_CLIENT

        send_bytes += bytesWrite;
    }
    
    if (bytesWrite == -1) {
        logging(INFO, " write: error encountered ");
        exit(401);
    }

    return buff_size;
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

void * client_thread(void * arg) {
    struct cetus_param * param = (struct cetus_param *)arg;

    parse_command_line(param->argc, param->argv);
    
    input_file = file;
    input_file_ptr = 0;

#ifdef TIMESTAMP_CLIENT
    char * name = (char *)calloc(1, 32);

	snprintf(name, 31, "overhead.txt", lcore_id);
	FILE * fp = fopen(name, "a+");

    char * overhead = (char *)calloc(1, 512);
#endif

    logging(INFO, " [%s on core %d] testing client!", __func__, lcore_id);

#ifndef RSS_IN_USE
        uint16_t port = ((lcore_id - 1) % param->port_range + 1) << 8;
#else
        uint16_t port = 1234;
#endif
    int c;
    if((c = connect_server("10.0.0.1", port)) < 0) {
        logging(ERROR, " [%s on core %d] connect to server failed", __func__, lcore_id);
        return 0;
    }

    for (int i = 0; i < 1001; i++) {
        write_data(c);
        read_data(c);
    }

    cetus_close(c);
done:
#ifdef TIMESTAMP_CLIENT
    for (int i = 0; i < ts_count; i++) {
        int * count = (int *)(ts[i]);
        struct timeval * tv = (struct timeval *)((char *)(ts[i]) + sizeof(int));

        long long t[4];
        for (int i = 0; i < *(count) && i < 4; i++) {
            t[i] = TIMEVAL_TO_USEC(tv[i]);
        }

        long long network_out = t[1] - t[0];
        long long network_in = t[3] - t[2];
        long long in_flight = t[2] - t[1];
        long long total = t[3] - t[0];

        fprintf(stdout, " Packet %d total %llu us\n", i, total);
        fprintf(stdout, " \t Network(out) %llu us\n", network_out);
        fprintf(stdout, " \t InFlight %llu us\n", in_flight);
        fprintf(stdout, " \t Network(in) %llu us\n", network_in);

        sprintf(overhead, " Total : %llu us | Network(out) : %llu us | Inflight : %llu us | Network(in) : %llu us\n", \
                        total, network_out, in_flight, network_in);

        fwrite(overhead, 1, strlen(overhead), fp);
    }

	fclose(fp);
#endif  // TIMESTAMP_CLIENT

    return 0;
}

int test_net(void * arg) {
    cetus_init((struct cetus_param *)arg);

    int ret;
    mthread_t mid;

    file = (char *)malloc(M_128);

    FILE * fp = fopen("input.dat", "rb");

    fread(file, 1, M_128, fp);

    fclose(fp);

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

    // printf(" [PAYLOAD] recv bytes: %llu (Mb), send rate: %llu (Mb)\n", recv_bytes, send_bytes);

    // char * result_buff = (char *)calloc(1, 512);

    // snprintf(result_buff, 511, " [PAYLOAD] recv rate: %.2f (Mbps), send rate: %.2f (Mbps)\n", 
    //         (recv_bytes * 8.0) / (total_time * 1000 * 1000), (send_bytes * 8.0) / (total_time * 1000 * 1000));

    // printf("%s", result_buff);

    // fwrite(result_buff, 1, strlen(result_buff), thp_file);

    // fprintf(thp_file, " ====================\n");
	
	// fclose(thp_file);
}

int main(int argc, char ** argv) {
    struct cetus_param * param = cetus_config(argc, argv);

    cetus_spawn(test_net, param);

    return 0;
}