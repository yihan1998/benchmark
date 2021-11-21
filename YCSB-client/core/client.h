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
#include "utils.h"

#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>

#include <fcntl.h>

#define BUFF_SIZE   1024

#define TABLE_NAME_SIZE 16
#define KEY_SIZE        32
#define VALUE_SIZE      32

struct conn_info {
    int     sockfd;
    int     epfd;

    long long   total_record_ops;
    long long   total_operation_ops;

    long long   actual_record_ops;
    long long   actual_operation_ops;
};

namespace ycsbc {
class KVRequest {
    public:
        // KVRequest(Operation op, std::string table, DB::KVPair request) : op(op), table(table), request(request) { }
        KVRequest() { }

        Operation op;
        char table[TABLE_NAME_SIZE];
        std::pair<char[100], char[100]> request;
};

class KVReply {
    public:
        // KVReply(Operation op, int return_val, DB::KVPair result) : op(op), return_val(return_val), result(result) { }
        KVReply() { }

        Operation op;
        int return_val;
        std::pair<char[100], char[100]> result;
};

class Client {
    public:
        Client(CoreWorkload &wl) : workload_(wl) { }

        virtual int InsertRecord(KVRequest &request);

        virtual int SendRequest(KVRequest &request);
        virtual int ReceiveReply(KVReply &reply);

        virtual int ConnectServer(char * ip, int port);
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

inline int Client::InsertRecord(KVRequest &request) {
    const std::string &table = workload_.NextTable();
    std::string key = workload_.NextSequenceKey();
    std::string value;
    workload_.BuildValues(value);

    request.op = INSERT;
    // request.table = table;
    // request.request = std::make_pair(key, std::string());
    strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    // key.copy(request.request.first, key.size());
    strncpy(request.request.first, key.c_str(), KEY_SIZE);
    // value.copy(request.request.second, value.size());
    strncpy(request.request.second, value.c_str(), VALUE_SIZE);
    std::cout <<  " Insert record to table: " << request.table << ", key: " << request.request.first << ", value: " << request.request.second << "\n" << std::endl;
    return DB::kOK;
}

inline int Client::SendRequest(KVRequest &request) {
    int status = -1;
    switch (workload_.NextOperation()) {
        case READ:
            status = ReadRequest(request);
            break;
        case UPDATE:
            status = UpdateRequest(request);
            break;
        case INSERT:
            status = InsertRequest(request);
            break;
        case SCAN:
            status = ScanRequest(request);
            break;
        case READMODIFYWRITE:
            status = ReadModifyWriteRequest(request);
            break;
        default:
            throw utils::Exception("Operation request is not recognized!");
    }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::ReceiveReply(KVReply &reply) {
    int status = -1;
    switch (reply.op) {
        case READ:
            status = ReadReply(reply);
            break;
        case UPDATE:
            status = UpdateReply(reply);
            break;
        case INSERT:
            status = InsertReply(reply);
            break;
        case SCAN:
            status = ScanReply(reply);
            break;
        case READMODIFYWRITE:
            status = ReadModifyWriteReply(reply);
            break;
        default:
            throw utils::Exception("Operation request is not recognized!");
    }
    assert(status >= 0);
    return (status == DB::kOK);
}

inline int Client::HandleReadEvent(struct conn_info * info) {
    // char buff[BUFF_SIZE];

    // int len = read(info->sockfd, buff, BUFF_SIZE);
    // printf("%s\n", buff);

    // return len;
    KVReply reply;
    int len = read(info->sockfd, &reply, sizeof(reply));

    if (len <= 0) {
        return len;
    }
    
    int ret = ReceiveReply(reply);

    return len;
}

inline int Client::HandleWriteEvent(struct conn_info * info) {
    // char buff[BUFF_SIZE];
    // sprintf(buff, "Hello from client(%d)", counter++);

    // int len = send(info->sockfd, buff, BUFF_SIZE, 0);
    // printf("Hello message sent: %s\n", buff);

    // char buff[BUFF_SIZE];
    // snprintf(buff, sizeof(request), (char *)&request);
    // std::cout <<  " Send request: " << buff << "\n" << std::endl;

    KVRequest request;
    int ret = SendRequest(request);
    
    int len = write(info->sockfd, &request, sizeof(request));

    return len;
}

inline int Client::HandleErrorEvent(struct conn_info * info) {
    return 0;
}

inline int Client::ReadRequest(KVRequest &request) {
    const std::string &table = workload_.NextTable();
    const std::string &key = workload_.NextTransactionKey();

    request.op = READ;
    
    std::string empty;
    strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    strncpy(request.request.first, key.c_str(), KEY_SIZE);
    strncpy(request.request.second, empty.c_str(), VALUE_SIZE);

    // std::cout <<  " Read table: " <<  request.table << ", key: " << request.request.first << "\n" << std::endl;

    return DB::kOK;
}

inline int Client::ReadReply(KVReply &reply) {
    std::string key = std::string(reply.result.first, sizeof(reply.result.first));
    std::string value = std::string(reply.result.second, sizeof(reply.result.second));

    // std::cout <<  " Read reply key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;

    return reply.return_val;
}

inline int Client::ReadModifyWriteRequest(KVRequest &request) {
    const std::string &table = workload_.NextTable();
    const std::string &key = workload_.NextTransactionKey();

    std::string value;
    workload_.BuildUpdate(value);

    request.op = READMODIFYWRITE;

    strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    strncpy(request.request.first, key.c_str(), KEY_SIZE);
    strncpy(request.request.second, value.c_str(), VALUE_SIZE);
    
    // std::cout <<  " ReadModifyWrite table: " << request.table << ", key: " << request.request.first << ", value: " << request.request.second << "\n" << std::endl;
    
    return DB::kOK;
}

inline int Client::ReadModifyWriteReply(KVReply &reply) {
    std::string key = std::string(reply.result.first, sizeof(reply.result.first));
    std::string value = std::string(reply.result.second, sizeof(reply.result.second));

    // std::cout <<  " ReadModifyWrite reply key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;

    return reply.return_val;
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

inline int Client::ScanReply(KVReply &reply) {
    // std::string key = result.first;
    // std::string value = result.second;
    // std::cout <<  " Scan key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;
    // return return_val;
    // std::cout <<  " Scan reply " << std::endl;
    return reply.return_val;
}

inline int Client::UpdateRequest(KVRequest &request) {
    const std::string &table = workload_.NextTable();
    const std::string &key = workload_.NextTransactionKey();

    std::string value;
    workload_.BuildUpdate(value);
    
    request.op = UPDATE;

    strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    strncpy(request.request.first, key.c_str(), KEY_SIZE);
    strncpy(request.request.second, value.c_str(), VALUE_SIZE);
    
    // std::cout <<  " Update table: " <<  table.c_str() << ", key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;
    
    return DB::kOK;
}

inline int Client::UpdateReply(KVReply &reply) {
    std::string key = std::string(reply.result.first, sizeof(reply.result.first));
    std::string value = std::string(reply.result.second, sizeof(reply.result.second));

    // std::cout <<  " Update reply key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;
    return reply.return_val;
}

inline int Client::InsertRequest(KVRequest &request) {
    // const std::string &table = workload_.NextTable();
    // const std::string &key = workload_.NextSequenceKey();
    // std::vector<DB::KVPair> values;
    // workload_.BuildValues(values);
    // return db_.Insert(table, key, values);
    const std::string &table = workload_.NextTable();
    const std::string &key = workload_.NextTransactionKey();

    std::string value;
    workload_.BuildValues(value);
    
    request.op = INSERT;

    strncpy(request.table, table.c_str(), TABLE_NAME_SIZE);
    strncpy(request.request.first, key.c_str(), KEY_SIZE);
    strncpy(request.request.second, value.c_str(), VALUE_SIZE);
    
    // std::cout <<  " Insert table: " <<  table.c_str() << ", key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;
    
    return DB::kOK;
}

inline int Client::InsertReply(KVReply &reply) {
    std::string key = std::string(reply.result.first, sizeof(reply.result.first));
    std::string value = std::string(reply.result.second, sizeof(reply.result.second));

    // std::cout <<  " Insert reply key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;
    return reply.return_val;
}

inline int Client::ConnectServer(char * ip, int port) {
    int sock = 0;
    struct sockaddr_in server_addr;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }
   
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip);
   
    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        printf("\nConnection Failed \n");
        return -1;
    }

    fcntl(sock, F_SETFL, O_NONBLOCK);

    return sock;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
