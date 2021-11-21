#ifndef _SERVER_H_
#define _SERVER_H_

#include <string>
#include "db.h"
#include "properties.h"

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

struct thread_context {
	mctx_t              mctx;
	int                 epfd;
	struct server_vars  * svars;
};

struct server_vars {
	int         epfd;
    int         sockfd;
    struct thread_context * ctx;
};

struct server_arg {
    int         core;
    utils::Properties  * props;
};

#ifndef MAX_CPUS
#define MAX_CPUS		16
#endif

#define TABLE_NAME_SIZE 16
#define KEY_SIZE        32
#define VALUE_SIZE      32

static pthread_t sv_thread[MAX_CPUS];
static struct server_arg sv_thread_arg[MAX_CPUS];

namespace ycsbc {

enum Operation {
  INSERT,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE
};

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

class Server {
    public:
        Server(DB &db) : db_(db) { }

        virtual int HandleReadEvent(struct server_vars * sv);
        virtual int HandleErrorEvent(struct server_vars * sv);

        virtual int ReceiveRequest(KVRequest &request, KVReply &reply);
        
        virtual ~Server() { }
    
    protected:
    
        virtual int Read(KVRequest &request, KVReply &reply);
        virtual int ReadModifyWrite(KVRequest &request, KVReply &reply);
        virtual int Scan(KVRequest &request, KVReply &reply);
        virtual int Update(KVRequest &request, KVReply &reply);
        virtual int Insert(KVRequest &request, KVReply &reply);

        DB &db_;
};

inline int Server::ReceiveRequest(KVRequest &request, KVReply &reply) {
    int status = -1;
    switch (request.op) {
        case READ:
            status = Read(request, reply);
            break;
        case UPDATE:
            status = Update(request, reply);
            break;
        case INSERT:
            status = Insert(request, reply);
            break;
        case SCAN:
            status = Scan(request, reply);
            break;
        case READMODIFYWRITE:
            status = ReadModifyWrite(request, reply);
            break;
        default:
            throw utils::Exception("Operation request is not recognized!");
    }
    return (status == DB::kOK);
}

inline int Server::HandleReadEvent(struct server_vars * sv) {
    // char recv_buff[BUFF_SIZE];

    // int recv_len = read(sock, recv_buff, BUFF_SIZE);
    // std::cout <<  " Receive request: " << recv_buff << "\n" << std::endl;

    KVRequest request;
    int recv_len = mtcp_read(sv->ctx->mctx, sv->sockfd, (char *)&request, sizeof(request));
    if (recv_len <= 0) {
        return recv_len;
    }

    KVReply reply;
    int ret = ReceiveRequest(request, reply);

    // char send_buff[BUFF_SIZE];
    // sprintf(send_buff, "Hello from server(%d)", counter++);

    int send_len = mtcp_write(sv->ctx->mctx, sv->sockfd, (char *)&reply, sizeof(reply));
    // printf("Hello message sent: %s\n", send_buff);

    return recv_len;
}

inline int Server::HandleErrorEvent(struct server_vars * sv) {
    mtcp_close(sv->ctx->mctx, sv->sockfd);
    return 0;
}

inline int Server::Read(KVRequest &request, KVReply &reply) {
    std::string table = std::string(request.table, strlen(request.table));
    std::string key = std::string(request.request.first, sizeof(request.request.first));
    
    std::string value;
    int ret = db_.Read(table, key, value);

    reply.op = READ;
    reply.return_val = ret;

    // std::cout <<  " Read table: " <<  table.c_str() << ", key: " << key.c_str() << ", value: " <<  value.c_str() << "\n" << std::endl;

    strncpy(reply.result.first, key.c_str(), KEY_SIZE);
    strncpy(reply.result.second, value.c_str(), VALUE_SIZE);

    return ret;
}

inline int Server::ReadModifyWrite(KVRequest &request, KVReply &reply) {
    std::string table = std::string(request.table, strlen(request.table));
    std::string key = std::string(request.request.first, sizeof(request.request.first));
    std::string value = std::string(request.request.second, sizeof(request.request.second));

    // std::cout <<  " ReadModifyWrite table: " <<  table.c_str() << ", key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;

    std::string old;
    
    int ret;
    ret = db_.Read(table, key, old);
    
    reply.op = READMODIFYWRITE;

    strncpy(reply.result.first, key.c_str(), KEY_SIZE);
    strncpy(reply.result.second, old.c_str(), VALUE_SIZE);

    ret = db_.Update(table, key, value);
    reply.return_val = ret;

    return ret;
}

inline int Server::Scan(KVRequest &request, KVReply &reply) {
    /* TODO */
    std::string table = std::string(request.table, strlen(request.table));
    std::string key = std::string(request.request.first, sizeof(request.request.first));
    std::string value = std::string(request.request.second, sizeof(request.request.second));

    // std::cout <<  " Scan table: " <<  table.c_str() << ", key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;
    return DB::kOK;
}

inline int Server::Update(KVRequest &request, KVReply &reply) {
    std::string table = std::string(request.table, strlen(request.table));
    std::string key = std::string(request.request.first, sizeof(request.request.first));
    std::string value = std::string(request.request.second, sizeof(request.request.second));
    
    // std::cout <<  " Update table: " <<  table.c_str() << ", key: " << key.c_str() << ", value: " << value.c_str() << "\n" << std::endl;
    
    int ret = db_.Update(table, key, value);
    
    reply.op = UPDATE;

    strncpy(reply.result.first, key.c_str(), KEY_SIZE);
    strncpy(reply.result.second, value.c_str(), VALUE_SIZE);

    reply.return_val = ret;
    return ret;
}

inline int Server::Insert(KVRequest &request, KVReply &reply) {
    std::string table = std::string(request.table, strlen(request.table));
    std::string key = std::string(request.request.first, sizeof(request.request.first));
    std::string value = std::string(request.request.second, sizeof(request.request.second));

    int ret = db_.Insert(table, key, value);
    
    reply.op = INSERT;

    strncpy(reply.result.first, key.c_str(), KEY_SIZE);
    strncpy(reply.result.second, value.c_str(), VALUE_SIZE);

    reply.return_val = ret;
    return ret;
} 

}

#endif // _SERVER_H_
