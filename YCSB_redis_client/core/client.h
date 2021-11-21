//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"


#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>

#include <fcntl.h>

//mTCP library
#include <mtcp_api.h>
#include <mtcp_epoll.h>

#define BUFF_SIZE   1024

#define TABLE_NAME_SIZE 16
#define KEY_SIZE        32
#define VALUE_SIZE      32

struct conn_info {
    int     sockfd;
    int     epfd;

    char    * ibuf;
    int     ioff;

    char    * obuf;
    int     ooff;
    int     oremain;

    long long   total_record_ops;
    long long   total_operation_ops;

    long long   actual_record_ops;
    long long   actual_operation_ops;
};

struct thread_context {
    int                 core_id;
	mctx_t              mctx;
	int                 epfd;
    int                 num_conn;
	struct conn_info    * info;
    utils::Properties   * props;
};

struct client_arg {
    int                 core;
    utils::Properties   * props;
};

#ifndef MAX_CPUS
#define MAX_CPUS		16
#endif

static pthread_t cl_thread[MAX_CPUS];
static struct client_arg cl_thread_arg[MAX_CPUS];

namespace ycsbc {
class KVRequest {
    public:
        // KVRequest(Operation op, std::string table, DB::KVPair request) : op(op), table(table), request(request) { }
        KVRequest() { }

        Operation op;
        char table[TABLE_NAME_SIZE];
        std::pair<char[KEY_SIZE], char[VALUE_SIZE]> request;
};

class KVReply {
    public:
        // KVReply(Operation op, int return_val, DB::KVPair result) : op(op), return_val(return_val), result(result) { }
        KVReply() { }

        Operation op;
        int return_val;
        std::pair<char[KEY_SIZE], char[VALUE_SIZE]> result;
};

class Client {
    public:
        Client(CoreWorkload &wl) : workload_(wl) { }

        virtual int InsertRecord(char * request);

        virtual int SendRequest(char * obuf);
        virtual int ReceiveReply(char * ibuf);

        virtual int ConnectServer(const std::string &ip, int port);
        virtual int HandleReadEvent(struct conn_info * info);
        virtual int HandleWriteEvent(struct conn_info * info);
        virtual int HandleErrorEvent(struct conn_info * info);

        virtual ~Client() { }
    
    protected:
    
        virtual int ReadRequest(KVRequest &request);
        virtual int ReadModifyWriteRequest(KVRequest &request);
        virtual int ScanRequest(KVRequest &request);
        virtual int UpdateRequest(KVRequest &request);
        virtual int InsertRequest(KVRequest &request);

        virtual int ReadReply(KVReply &reply);
        virtual int ReadModifyWriteReply(KVReply &reply);
        virtual int ScanReply(KVReply &reply);
        virtual int UpdateReply(KVReply &reply);
        virtual int InsertReply(KVReply &reply);

        CoreWorkload &workload_;
};

inline int Client::InsertRecord(char * request) {
    const std::string &table = workload_.NextTable();
    std::string key = workload_.NextSequenceKey();
    std::string value;
    workload_.BuildValues(value);

    // request.op = INSERT;
    // strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    // strncpy(request.request.first, key.c_str(), KEY_SIZE);
    // strncpy(request.request.second, value.c_str(), VALUE_SIZE);
    // std::cout <<  " Insert record to table: " << request.table << ", key: " << request.request.first << ", value: " << request.request.second << "\n" << std::endl;
    
    std::string cmd("SET");
    size_t len = cmd.length() + 1 + key.length() + 1 + value.length() + 1;
    cmd.reserve(len);

    cmd.append(" ").append(key);
    cmd.append(" ").append(value);

    strncpy(request, cmd.c_str(), len);

    request[len-1] = '\n';

    // printf(" >> Insert record: %s", request);

    return len;
}

inline int Client::SendRequest(char * obuf) {
    int len = -1;
    switch (workload_.NextOperation()) {
        case READ:
            len = ReadRequest(obuf);
            break;
        case UPDATE:
            len = UpdateRequest(obuf);
            break;
        case INSERT:
            len = InsertRequest(obuf);
            break;
        // case SCAN:
        //     status = ScanRequest(obuf);
        //     break;
        case READMODIFYWRITE:
            len = ReadModifyWriteRequest(obuf);
            break;
        default:
            throw utils::Exception("Operation request is not recognized!");
    }
    assert(len > 0);
    return len;
}

inline int Client::ReceiveReply(char * ibuf) {
    // printf(" >> reply: %s", ibuf);
    return 0;
}

inline int Client::HandleReadEvent(mctx_t mctx, struct conn_info * info) {
    // char buff[BUFF_SIZE];

    // int len = read(info->sockfd, buff, BUFF_SIZE);
    // printf("%s\n", buff);

    // return len;
    KVReply reply;
    int len = mtcp_read(mctx, info->sockfd, (char *)&reply, sizeof(reply));

    if (len <= 0) {
        return len;
    }
    
    // int ret = ReceiveReply(reply);

    return len;
}

inline int Client::HandleWriteEvent(mctx_t mctx, struct conn_info * info) {
    // char buff[BUFF_SIZE];
    // sprintf(buff, "Hello from client(%d)", counter++);

    // int len = send(info->sockfd, buff, BUFF_SIZE, 0);
    // printf("Hello message sent: %s\n", buff);

    // char buff[BUFF_SIZE];
    // snprintf(buff, sizeof(request), (char *)&request);
    // std::cout <<  " Send request: " << buff << "\n" << std::endl;

    KVRequest request;
    int ret = SendRequest(request);
    
    int len = mtcp_write(mctx, info->sockfd, (char *)&request, sizeof(request));

    return len;
}

inline int Client::HandleErrorEvent(mctx_t mctx, struct conn_info * info) {
    return 0;
}

inline int Client::ReadRequest(char * request) {
    // const std::string &table = workload_.NextTable();
    // const std::string &key = workload_.NextTransactionKey();

    // request.op = READ;
    
    // std::string empty;
    // strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    // strncpy(request.request.first, key.c_str(), KEY_SIZE);
    // strncpy(request.request.second, empty.c_str(), VALUE_SIZE);

    // std::cout <<  " Read table: " <<  request.table << ", key: " << request.request.first << "\n" << std::endl;

    // return DB::kOK;

    const std::string &table = workload_.NextTable();
    std::string key = workload_.NextSequenceKey();

    std::string cmd("GET");
    size_t len = cmd.length() + 1 + key.length() + 1;
    cmd.reserve(len);

    cmd.append(" ").append(key);

    strncpy(request, cmd.c_str(), len);

    request[len-1] = '\n';

    return len;
}

inline int Client::ReadModifyWriteRequest(char * request) {
    // const std::string &table = workload_.NextTable();
    // const std::string &key = workload_.NextTransactionKey();

    // std::string value;
    // workload_.BuildUpdate(value);

    // request.op = READMODIFYWRITE;

    // strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    // strncpy(request.request.first, key.c_str(), KEY_SIZE);
    // strncpy(request.request.second, value.c_str(), VALUE_SIZE);
    
    // std::cout <<  " ReadModifyWrite table: " << request.table << ", key: " << request.request.first << ", value: " << request.request.second << "\n" << std::endl;
    
    // return DB::kOK;

    const std::string &table = workload_.NextTable();
    std::string key = workload_.NextSequenceKey();
    std::string value;
    workload_.BuildValues(value);

    std::string cmd("GETSET");
    size_t len = cmd.length() + 1 + key.length() + 1 + value.length() + 1;
    cmd.reserve(len);

    cmd.append(" ").append(key);
    cmd.append(" ").append(value);

    strncpy(request, cmd.c_str(), len);

    request[len-1] = '\n';

    return len;
}

inline int Client::ScanRequest(KVRequest &request) {
    const std::string &table = workload_.NextTable();
    const std::string &key = workload_.NextTransactionKey();
    int len = workload_.NextScanLength();
    std::string record_count = std::to_string(len);

    request.op = SCAN;

    strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    strncpy(request.request.first, key.c_str(), KEY_SIZE);
    strncpy(request.request.second, record_count.c_str(), VALUE_SIZE);

    // std::cout <<  " Scan table: " <<  table.c_str() << ", key: " << key.c_str() << ", record len: " << len << "\n" << std::endl;
    
    return DB::kOK;
}

inline int Client::UpdateRequest(char * request) {
    // const std::string &table = workload_.NextTable();
    // const std::string &key = workload_.NextTransactionKey();

    // std::string value;
    // workload_.BuildUpdate(value);
    
    // request.op = UPDATE;

    // strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    // strncpy(request.request.first, key.c_str(), KEY_SIZE);
    // strncpy(request.request.second, value.c_str(), VALUE_SIZE);
    
    // std::cout <<  " Update table: " <<  table.c_str() << ", key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;
    
    // return DB::kOK;

    const std::string &table = workload_.NextTable();
    std::string key = workload_.NextSequenceKey();
    std::string value;
    workload_.BuildValues(value);

    std::string cmd("SET");
    size_t len = cmd.length() + 1 + key.length() + 1 + value.length() + 1;
    cmd.reserve(len);

    cmd.append(" ").append(key);
    cmd.append(" ").append(value);

    strncpy(request, cmd.c_str(), len);

    request[len-1] = '\n';

    return len;
}

inline int Client::InsertRequest(char * request) {
    // const std::string &table = workload_.NextTable();
    // const std::string &key = workload_.NextTransactionKey();

    // std::string value;
    // workload_.BuildValues(value);
    
    // request.op = INSERT;

    // strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    // strncpy(request.request.first, key.c_str(), KEY_SIZE);
    // strncpy(request.request.second, value.c_str(), VALUE_SIZE);
    
    // std::cout <<  " Insert table: " <<  table.c_str() << ", key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;

    const std::string &table = workload_.NextTable();
    std::string key = workload_.NextSequenceKey();
    std::string value;
    workload_.BuildValues(value);

    std::string cmd("SET");
    size_t len = cmd.length() + 1 + key.length() + 1 + value.length() + 1;
    cmd.reserve(len);

    cmd.append(" ").append(key);
    cmd.append(" ").append(value);

    strncpy(request, cmd.c_str(), len);

    request[len-1] = '\n';

    return len;
}

inline int Client::ConnectServer(mctx_t mctx, char * ip, int port) {
    int sock = 0;
    struct sockaddr_in server_addr;

    sock = mtcp_socket(mctx, AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		fprintf(stderr, "Failed to create socket!\n");
		return -1;
	}
   
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip);
   
    int ret;
    ret = mtcp_connect(mctx, sock, (struct sockaddr *)&server_addr, sizeof(struct sockaddr_in));
	if (ret < 0) {
        printf("\nConnection Failed \n");
        return -1;
    }

    ret = mtcp_setsock_nonblock(mctx, sock);
	if (ret < 0) {
		fprintf(stderr, "Failed to set socket in nonblocking mode.\n");
		exit(-1);
	}

    return sock;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
