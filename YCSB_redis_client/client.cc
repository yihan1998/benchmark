//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
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

__thread int num_conn = 0;
__thread struct conn_info * info;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

/* Default set of functions to build the reply. Keep in mind that such a
 * function returning NULL is interpreted as OOM. */
static redisReplyObjectFunctions defaultFunctions = {
    createStringObject,
    createArrayObject,
    createIntegerObject,
    createNilObject,
    freeReplyObject
};

/* Create a reply object */
static redisReply *createReplyObject(int type) {
    redisReply *r = calloc(1,sizeof(*r));

    if (r == NULL)
        return NULL;

    r->type = type;
    return r;
}

/* Free a reply object */
void freeReplyObject(void *reply) {
    redisReply *r = reply;
    size_t j;

    switch(r->type) {
    case REDIS_REPLY_INTEGER:
        break; /* Nothing to free */
    case REDIS_REPLY_ARRAY:
        if (r->element != NULL) {
            for (j = 0; j < r->elements; j++)
                if (r->element[j] != NULL)
                    freeReplyObject(r->element[j]);
            free(r->element);
        }
        break;
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
        if (r->str != NULL)
            free(r->str);
        break;
    }
    free(r);
}

static void *createStringObject(const redisReadTask *task, char *str, size_t len) {
    redisReply *r, *parent;
    char *buf;

    r = createReplyObject(task->type);
    if (r == NULL)
        return NULL;

    buf = malloc(len+1);
    if (buf == NULL) {
        freeReplyObject(r);
        return NULL;
    }

    assert(task->type == REDIS_REPLY_ERROR  ||
           task->type == REDIS_REPLY_STATUS ||
           task->type == REDIS_REPLY_STRING);

    /* Copy string value */
    memcpy(buf,str,len);
    buf[len] = '\0';
    r->str = buf;
    r->len = len;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *createArrayObject(const redisReadTask *task, int elements) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_ARRAY);
    if (r == NULL)
        return NULL;

    if (elements > 0) {
        r->element = calloc(elements,sizeof(redisReply*));
        if (r->element == NULL) {
            freeReplyObject(r);
            return NULL;
        }
    }

    r->elements = elements;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *createIntegerObject(const redisReadTask *task, long long value) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_INTEGER);
    if (r == NULL)
        return NULL;

    r->integer = value;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *createNilObject(const redisReadTask *task) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_NIL);
    if (r == NULL)
        return NULL;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY);
        parent->element[task->idx] = r;
    }
    return r;
}

/* This is the reply object returned by redisCommand() */
typedef struct redisReply {
    int type; /* REDIS_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    int len; /* Length of string */
    char *str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
    struct redisReply **element; /* elements vector for REDIS_REPLY_ARRAY */
} redisReply;

/* State for the protocol parser */
typedef struct redisReader {
    int err; /* Error flags, 0 when there is no error */
    char errstr[128]; /* String representation of error when applicable */

    char *buf; /* Read buffer */
    size_t pos; /* Buffer cursor */
    size_t len; /* Buffer length */
    size_t maxbuf; /* Max length of unused buffer */

    redisReadTask rstack[9];
    int ridx; /* Index of current read task */
    void *reply; /* Temporary reply pointer */

    redisReplyObjectFunctions *fn;
    void *privdata;
} redisReader;

redisReader * redisReaderCreate(char * buff, int len) {
    redisReader * r = (redisReader *)calloc(1, sizeof(redisReader));
    if (r == NULL) {
        return NULL;
    }

    r->err = 0;
    r->errstr[0] = '\0';
    r->fn = &defaultFunctions;
    r->buf = sdsempty();
    r->pos = 0;
    r->len = len;
    r->maxbuf = REDIS_READER_MAX_BUF;
    r->ridx = -1;

    return r;
}

static char *readBytes(redisReader *r, unsigned int bytes) {
    char *p;
    if (r->len-r->pos >= bytes) {
        p = r->buf+r->pos;
        r->pos += bytes;
        return p;
    }
    return NULL;
}


/* Find pointer to \r\n. */
static char *seekNewline(char *s, size_t len) {
    int pos = 0;
    int _len = len-1;

    /* Position should be < len-1 because the character at "pos" should be
     * followed by a \n. Note that strchr cannot be used because it doesn't
     * allow to search a limited length and the buffer that is being searched
     * might not have a trailing NULL character. */
    while (pos < _len) {
        while(pos < _len && s[pos] != '\r') pos++;
        if (s[pos] != '\r') {
            /* Not found. */
            return NULL;
        } else {
            if (s[pos+1] == '\n') {
                /* Found. */
                return s+pos;
            } else {
                /* Continue searching. */
                pos++;
            }
        }
    }
    return NULL;
}

/* Read a long long value starting at *s, under the assumption that it will be
 * terminated by \r\n. Ambiguously returns -1 for unexpected input. */
static long long readLongLong(char *s) {
    long long v = 0;
    int dec, mult = 1;
    char c;

    if (*s == '-') {
        mult = -1;
        s++;
    } else if (*s == '+') {
        mult = 1;
        s++;
    }

    while ((c = *(s++)) != '\r') {
        dec = c - '0';
        if (dec >= 0 && dec < 10) {
            v *= 10;
            v += dec;
        } else {
            /* Should not happen... */
            return -1;
        }
    }

    return mult*v;
}

static char *readLine(redisReader *r, int *_len) {
    char *p, *s;
    int len;

    p = r->buf+r->pos;
    s = seekNewline(p,(r->len-r->pos));
    if (s != NULL) {
        len = s-(r->buf+r->pos);
        r->pos += len+2; /* skip \r\n */
        if (_len) *_len = len;
        return p;
    }
    return NULL;
}

static void moveToNextTask(redisReader * r) {
    redisReadTask * cur, * prv;
    while (r->ridx >= 0) {
        /* Return a.s.a.p. when the stack is now empty. */
        if (r->ridx == 0) {
            r->ridx--;
            return;
        }

        cur = &(r->rstack[r->ridx]);
        prv = &(r->rstack[r->ridx-1]);
        assert(prv->type == REDIS_REPLY_ARRAY);
        if (cur->idx == prv->elements-1) {
            r->ridx--;
        } else {
            /* Reset the type because the next item can be anything */
            assert(cur->idx < prv->elements);
            cur->type = -1;
            cur->elements = -1;
            cur->idx++;
            return;
        }
    }
}

static int processLineItem(redisReader *r) {
    redisReadTask *cur = &(r->rstack[r->ridx]);
    void *obj;
    char *p;
    int len;

    if ((p = readLine(r,&len)) != NULL) {
        if (cur->type == REDIS_REPLY_INTEGER) {
            if (r->fn && r->fn->createInteger)
                obj = r->fn->createInteger(cur,readLongLong(p));
            else
                obj = (void*)REDIS_REPLY_INTEGER;
        } else {
            /* Type will be error or status. */
            if (r->fn && r->fn->createString)
                obj = r->fn->createString(cur,p,len);
            else
                obj = (void*)(size_t)(cur->type);
        }

        if (obj == NULL) {
            __redisReaderSetErrorOOM(r);
            return REDIS_ERR;
        }

        /* Set reply if this is the root object. */
        if (r->ridx == 0) r->reply = obj;
        moveToNextTask(r);
        return REDIS_OK;
    }

    return REDIS_ERR;
}

static int processBulkItem(redisReader *r) {
    redisReadTask *cur = &(r->rstack[r->ridx]);
    void *obj = NULL;
    char *p, *s;
    long len;
    unsigned long bytelen;
    int success = 0;

    p = r->buf+r->pos;
    s = seekNewline(p,r->len-r->pos);
    if (s != NULL) {
        p = r->buf+r->pos;
        bytelen = s-(r->buf+r->pos)+2; /* include \r\n */
        len = readLongLong(p);

        if (len < 0) {
            /* The nil object can always be created. */
            if (r->fn && r->fn->createNil)
                obj = r->fn->createNil(cur);
            else
                obj = (void*)REDIS_REPLY_NIL;
            success = 1;
        } else {
            /* Only continue when the buffer contains the entire bulk item. */
            bytelen += len+2; /* include \r\n */
            if (r->pos+bytelen <= r->len) {
                if (r->fn && r->fn->createString)
                    obj = r->fn->createString(cur,s+2,len);
                else
                    obj = (void*)REDIS_REPLY_STRING;
                success = 1;
            }
        }

        /* Proceed when obj was created. */
        if (success) {
            if (obj == NULL) {
                __redisReaderSetErrorOOM(r);
                return REDIS_ERR;
            }

            r->pos += bytelen;

            /* Set reply if this is the root object. */
            if (r->ridx == 0) r->reply = obj;
            moveToNextTask(r);
            return REDIS_OK;
        }
    }

    return REDIS_ERR;
}

static int processMultiBulkItem(redisReader *r) {
    redisReadTask *cur = &(r->rstack[r->ridx]);
    void *obj;
    char *p;
    long elements;
    int root = 0;

    /* Set error for nested multi bulks with depth > 7 */
    if (r->ridx == 8) {
        __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
            "No support for nested multi bulk replies with depth > 7");
        return REDIS_ERR;
    }

    if ((p = readLine(r,NULL)) != NULL) {
        elements = readLongLong(p);
        root = (r->ridx == 0);

        if (elements == -1) {
            if (r->fn && r->fn->createNil)
                obj = r->fn->createNil(cur);
            else
                obj = (void*)REDIS_REPLY_NIL;

            if (obj == NULL) {
                __redisReaderSetErrorOOM(r);
                return REDIS_ERR;
            }

            moveToNextTask(r);
        } else {
            if (r->fn && r->fn->createArray)
                obj = r->fn->createArray(cur,elements);
            else
                obj = (void*)REDIS_REPLY_ARRAY;

            if (obj == NULL) {
                __redisReaderSetErrorOOM(r);
                return REDIS_ERR;
            }

            /* Modify task stack when there are more than 0 elements. */
            if (elements > 0) {
                cur->elements = elements;
                cur->obj = obj;
                r->ridx++;
                r->rstack[r->ridx].type = -1;
                r->rstack[r->ridx].elements = -1;
                r->rstack[r->ridx].idx = 0;
                r->rstack[r->ridx].obj = NULL;
                r->rstack[r->ridx].parent = cur;
                r->rstack[r->ridx].privdata = r->privdata;
            } else {
                moveToNextTask(r);
            }
        }

        /* Set reply if this is the root object. */
        if (root) r->reply = obj;
        return REDIS_OK;
    }

    return REDIS_ERR;
}

static int processItem(char * buff, int len, redisReader * r) {
    redisReadTask *cur = &(r->rstack[r->ridx]);
    char *p;

    /* check if we need to read type */
    if (cur->type < 0) {
        if ((p = readBytes(r,1)) != NULL) {
            switch (p[0]) {
            case '-':
                cur->type = REDIS_REPLY_ERROR;
                break;
            case '+':
                cur->type = REDIS_REPLY_STATUS;
                break;
            case ':':
                cur->type = REDIS_REPLY_INTEGER;
                break;
            case '$':
                cur->type = REDIS_REPLY_STRING;
                break;
            case '*':
                cur->type = REDIS_REPLY_ARRAY;
                break;
            default:
                __redisReaderSetErrorProtocolByte(r,*p);
                return REDIS_ERR;
            }
        } else {
            /* could not consume 1 byte */
            return REDIS_ERR;
        }
    }

    /* process typed item */
    switch(cur->type) {
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_INTEGER:
        return processLineItem(r);
    case REDIS_REPLY_STRING:
        return processBulkItem(r);
    case REDIS_REPLY_ARRAY:
        return processMultiBulkItem(r);
    default:
        assert(NULL);
        return REDIS_ERR; /* Avoid warning. */
    }
}

int getReply(char * buf, int len) {
    redisReply * reply = NULL;

    redisReader * reader = redisReaderCreate(buf, len);

    if (reader->ridx == -1) {
        /* Set first item to process when the stack is empty. */
        reader->rstack[0].type = -1;
        reader->rstack[0].elements = -1;
        reader->rstack[0].idx = -1;
        reader->rstack[0].obj = NULL;
        reader->rstack[0].parent = NULL;
        reader->rstack[0].privdata = NULL;
        reader->ridx = 0;
    }
    
    /* Process items in reply. */
    while (reader->ridx >= 0) {
        if (processItem(buf, len, reader) != REDIS_OK) {
            break;
        }
    }

    /* Return ASAP when an error occurred. */
    if (r->err)
        return REDIS_ERR;
    
    return REDIS_OK;
}

double LoadRecord(int epfd, struct epoll_event * events, ycsbc::Client &client, const int num_record_ops, const int num_operation_ops, const int port, const int num_flows) {
    int record_per_flow = num_record_ops / num_flows;
    int operation_per_flow = num_operation_ops / num_flows;

    int done = 0;

    int nevents;

    int num_load_complete = 0;

    utils::Timer<double> timer;
    double duration;

    timer.Start();

    while(!done) {
        while(num_conn < num_flows) {
            /* Connect server */
            int sock;
            if ((sock = client.ConnectServer("127.0.0.1", port)) > 0) {
                // fprintf(stdout, " [%s] connect server through sock %d\n", __func__, sock);
                struct conn_info * conn_info = &info[num_conn];
                conn_info->sockfd = sock;
                conn_info->epfd = epfd;

                conn_info->ibuf = (char *)calloc(16, 1024);
                conn_info->ioff = 0;

                conn_info->obuf = (char *)calloc(16, 1024);
                conn_info->ooff = 0;

                conn_info->total_record_ops = record_per_flow;
                conn_info->total_operation_ops = operation_per_flow;

                conn_info->actual_record_ops = conn_info->actual_operation_ops = 0;

                num_conn++;

                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.ptr = conn_info;
                epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev);
            } else {
                fprintf(stderr, " [%s] connect server failed!", __func__);
                exit(1);
            }
        }

        nevents = epoll_wait(epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & EPOLLERR)) {
                client.HandleErrorEvent(info);
            }
            
            if ((events[i].events & EPOLLIN)) {
                int len = read(info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);
                // printf(" [%s:%d] receive len: %d\n", __func__, __LINE__, len);

                if (len > 0) {
                    info->ioff += len;
                }

                if (getReply(info->ibuf, info->ioff)) {
                    info->ioff = 0;

                    /* Increase actual ops */
                    if(++info->actual_record_ops == info->total_record_ops) {
                        // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                        // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                        if (++num_load_complete == num_conn) {
                            done = 1;
                        }
                    }
                    
                    struct epoll_event ev;
                    ev.events = EPOLLIN | EPOLLOUT;
                    ev.data.ptr = info;

                    epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                }

                // if (strchr(info->ibuf,'\n')) {
                //     // printf(" [%s:%d] receive reply: %s", __func__, __LINE__, info->ibuf);

                //     client.ReceiveReply(info->ibuf);

                //     info->ioff = 0;

                //     /* Increase actual ops */
                //     if(++info->actual_record_ops == info->total_record_ops) {
                //         // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                //         // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                //         if (++num_load_complete == num_conn) {
                //             done = 1;
                //         }
                //     }
                    
                //     struct epoll_event ev;
                //     ev.events = EPOLLIN | EPOLLOUT;
                //     ev.data.ptr = info;

                //     epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                // }
            } else if ((events[i].events & EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client.InsertRecord(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = send(info->sockfd, info->obuf + info->ooff, info->oremain, 0);
                // printf(" [%s:%d] send len: %d\n", __func__, __LINE__, len);

                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct epoll_event ev;
                        ev.events = EPOLLIN;
                        ev.data.ptr = info;

                        epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                    }
                }
            } else {
                printf(" >> unknown event!\n");
            }
        }
    }

    duration = timer.End();

    for (int i = 0; i < num_conn; i++) {
        info[i].ioff = info[i].ooff = info[i].oremain = 0;
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT;
        ev.data.ptr = &info[i];

        epoll_ctl(info[i].epfd, EPOLL_CTL_MOD, info[i].sockfd, &ev);
    }
    
    return duration;
}

double PerformTransaction(int epfd, struct epoll_event * events, ycsbc::Client &client) {
    int done = 0;

    utils::Timer<double> timer;
    double duration;

    int num_transaction_complete = 0;

    int nevents;

    int oks = 0;

    timer.Start();

    while(!done) {
        nevents = epoll_wait(epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < nevents; i++) {
            struct conn_info * info = (struct conn_info *)(events[i].data.ptr);
            int ret;
            if ((events[i].events & EPOLLERR)) {
                client.HandleErrorEvent(info);
            }
            
            if ((events[i].events & EPOLLIN)) {
                int len = read(info->sockfd, info->ibuf + info->ioff, 1024*16 - info->ioff);
                printf(" [%s:%d] receive len: %d, ioff: %d\n", __func__, __LINE__, len, info->ioff);
                printf(" \t received: %*.s\n", len, info->ibuf + info->ioff);

                if (len > 0) {
                    info->ioff += len;
                }

                int to_recv = 0;
                int cursor = 0;
                char recv_buff[1024*16];
                // if (sscanf(info->ibuf, "$%d\r\n%s\r\n", &to_recv, recv_buff) == 2) {
                if (sscanf(info->ibuf, "$%d\r\n", &to_recv) == 1) {
                    // printf(" [%s:%d] to receive len: %d\n", __func__, __LINE__, to_recv);
                    if (to_recv != -1) {
                        if (sscanf(info->ibuf, "$%*d\r\n%s\r\n", recv_buff) == 1) {
                            // printf(" [%s:%d] received len: %d, %.*s\n", __func__, __LINE__, strlen(recv_buff), strlen(recv_buff), recv_buff);
                            if (to_recv != strlen(recv_buff)) {
                                continue;
                            }
                        }
                    }
                } else if (!strcmp(info->ibuf, "+OK\r\n")) {
                    /* Receive Insert/Update reply */
                } else if (sscanf(info->ibuf, "%d\r\n", &cursor) == 1) {
                    fprintf(stdout, "%s", info->ibuf);
                    if (cursor > 0) {
                        fprintf(stdout, "%s", info->ibuf);
                    }
                }
                
                // printf(" [%s:%d] receive reply: %s", __func__, __LINE__, info->ibuf);

                client.ReceiveReply(info->ibuf);

                info->ioff = 0;
                oks++;

                /* Increase actual ops */
                if(++info->actual_operation_ops == info->total_operation_ops) {
                    // cerr << " [ sock " << info->sockfd << "] # Loading records " << info->sockfd << " \t" << info->actual_record_ops << flush;
                    // fprintf(stdout, " [sock %d] # Loading records :\t %lld\n", info->sockfd, info->actual_record_ops);  
                    if (++num_transaction_complete == num_conn) {
                        done = 1;
                    }
                }
                
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT;
                ev.data.ptr = info;

                epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);

            } else if ((events[i].events & EPOLLOUT)) {
                if (info->oremain == 0) {
                    info->oremain = client.SendRequest(info->obuf);
                    info->ooff = 0;
                    // printf(" [%s:%d] new request: %d, %.*s", __func__, __LINE__, info->oremain, info->oremain, info->obuf);
                }

                int len = send(info->sockfd, info->obuf + info->ooff, info->oremain, 0);
                // printf(" [%s:%d] send len: %d\n", __func__, __LINE__, len);

                if(len > 0) {
                    info->ooff += len;
                    info->oremain -= len;
                    if (info->oremain == 0) {
                        struct epoll_event ev;
                        ev.events = EPOLLIN;
                        ev.data.ptr = info;

                        epoll_ctl(info->epfd, EPOLL_CTL_MOD, info->sockfd, &ev);
                    }
                }
            } else {
                printf(" >> unknown event!\n");
            }

        }
    }

    fprintf(stdout, " # Transaction: %llu\n", oks);

    duration = timer.End();
    return duration;
}

int main(const int argc, const char *argv[]) {
    utils::Properties props;
    string file_name = ParseCommandLine(argc, argv, props);
    cout << " Test workload: " << file_name << endl;

    int core_id = atoi(props.GetProperty("core_id", "1").c_str());

    cpu_set_t core_set;
    CPU_ZERO(&core_set);
    CPU_SET(core_id, &core_set);

    if (pthread_setaffinity_np(pthread_self(), sizeof(core_set), &core_set) == -1){
        printf("warning: could not set CPU affinity, continuing...\n");
    }

    ycsbc::CoreWorkload wl;
    wl.Init(props);

    const int num_flows = stoi(props.GetProperty("flows", "1"));

    int record_total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    int operation_total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    fprintf(stdout, " [core %d] # Total records (K) :\t %.2f \n", core_id, (double)record_total_ops / 1000.0);  
    fprintf(stdout, " [core %d] # Total transactions (K) :\t %.2f\n", core_id, (double)operation_total_ops / 1000.0);  

    // double duration = DelegateClient(db, &wl, record_total_ops, operation_total_ops, num_flows);

    ycsbc::Client client(wl);

    /* Initialize connection info array */
    info = (struct conn_info *)calloc(MAX_CONNECT, sizeof(struct conn_info));

    int epfd;
    struct epoll_event * events;

    /* Create epoll fd */
    epfd = epoll_create1(0);

    /* Initialize epoll event array */
    events = (struct epoll_event *)calloc(MAX_EVENTS, sizeof(struct epoll_event));

    int port = stoi(props.GetProperty("port", "6379"));

    double load_duration = 0.0;
    load_duration = LoadRecord(epfd, events, client, record_total_ops, operation_total_ops, port, num_flows);

    fprintf(stdout, " [core %d] loaded records done! \n", core_id);  

    double transaction_duration = 0.0;
    transaction_duration = PerformTransaction(epfd, events, client);

    char output[256];

    char output_file_name[32];
	sprintf(output_file_name, "throughput_core_%d.txt", core_id);

	FILE * output_file = fopen(output_file_name, "a+");
    if (!output_file) {
        perror("Failed to open output file");
    }
    
    sprintf(output, " [core %d] # Transaction throughput : %.2f (KTPS) \t %s \t %d\n", \
                    core_id, operation_total_ops / transaction_duration / 1000, \
                    file_name.c_str(), num_flows);

    fprintf(stdout, "%s", output);
    fflush(stdout);

    fprintf(output_file, "%s", output);
	fclose(output_file);

    return 0;
}

// string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
//     int argindex = 1;
//     string filename;
//     while (argindex < argc && StrStartWith(argv[argindex], "-")) {
//         if (strcmp(argv[argindex], "-flows") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             props.SetProperty("flows", argv[argindex]);
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
//         } else if (strcmp(argv[argindex], "-P") == 0) {
//             argindex++;
//             if (argindex >= argc) {
//                 UsageMessage(argv[0]);
//                 exit(0);
//             }
//             filename.assign(argv[argindex]);
//             ifstream input(argv[argindex]);
//             try {
//                 props.Load(input);
//             } catch (const string &message) {
//                 cout << message << endl;
//                 exit(0);
//             }
//             input.close();
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

//     return filename;
// }

enum cfg_params {
    PORT,
    WORKLOAD,
    FLOWS,
    CORE_ID,
};

const struct option options[] = {
    {   .name = "port", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = PORT},
    {   .name = "workload", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = WORKLOAD},
    {   .name = "flows", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = FLOWS},
    {   .name = "core_id", 
        .has_arg = required_argument,
        .flag = NULL, 
        .val = CORE_ID},
};

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
    int ret, done = 0;
    char * end;
    optind = 1;
    string filename;
    ifstream input;
    string strarg;

    while (!done) {
        ret = getopt_long(argc, (char * const *)argv, "", options, NULL);
        switch (ret) {
            case PORT:
                strarg.assign(optarg);
                props.SetProperty("port", strarg);
                cout << " Port: " << props["port"] << endl;
                cout.flush();
                break;
            
            case FLOWS:
                strarg.assign(optarg);
                props.SetProperty("flows", strarg);
                cout << " Flows: " << props["flows"] << endl;
                cout.flush();
                break;

            case CORE_ID:
                strarg.assign(optarg);
                props.SetProperty("core_id", strarg);
                cout << " Core id: " << props["core_id"] << endl;
                cout.flush();
                break;

            case WORKLOAD:
                filename.assign(optarg);
                input.open(filename);
                try {
                    props.Load(input);
                } catch (const string &message) {
                    cout << message << endl;
                    exit(0);
                }
                input.close();
                break;

            case -1:
                done = 1;
                break;
        }
    }

    fprintf(stdout, " [core %s] port: %s, flows: %s, workload: %s\n", \
                    props["core_id"].c_str(), props["port"].c_str(), props["flows"].c_str(), filename.c_str());

    return filename;
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

