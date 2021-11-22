#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <assert.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <limits.h>

#include <hiredis.h>

struct config {
    struct {
        const char *host;
        int port;
        struct timeval timeout;
    } config_tcp;

    struct {
        const char *path;
    } config_unix;
};

static redisContext *select_database(redisContext *c) {
    redisReply *reply;

    /* Switch to DB 9 for testing, now that we know we can chat. */
    reply = redisCommand(c,"SELECT 9");
    assert(reply != NULL);
    freeReplyObject(reply);

    /* Make sure the DB is emtpy */
    reply = redisCommand(c,"DBSIZE");
    assert(reply != NULL);
    if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 0) {
        /* Awesome, DB 9 is empty and we can continue. */
        freeReplyObject(reply);
    } else {
        printf("Database #9 is not empty, test can not continue\n");
        exit(1);
    }

    return c;
}

static int disconnect(redisContext *c, int keep_fd) {
    redisReply *reply;

    /* Make sure we're on DB 9. */
    reply = redisCommand(c,"SELECT 9");
    assert(reply != NULL);
    freeReplyObject(reply);
    reply = redisCommand(c,"FLUSHDB");
    assert(reply != NULL);
    freeReplyObject(reply);

    /* Free the context as well, but keep the fd if requested. */
    if (keep_fd)
        return redisFreeKeepFd(c);
    redisFree(c);
    return -1;
}

static redisContext *connect(struct config config) {
    redisContext * c = NULL;
    redisContext * dummy_ctx = redisConnectUnix(config.config_unix.path);
    if (dummy_ctx) {
        printf("ConnectUnix fd: %d\n", dummy_ctx->fd);
        int fd = disconnect(dummy_ctx, 1);
        printf("Connecting to inherited fd %d\n", fd);
        c = redisConnectFd(fd);
    }

    if (c == NULL) {
        printf("Connection error: can't allocate redis context\n");
        exit(1);
    } else if (c->err) {
        printf("Connection error: %s\n", c->errstr);
        redisFree(c);
        exit(1);
    }

    return select_database(c);
}

int main(int argc, char **argv) {
    unsigned int j;
    redisContext *c;
    redisReply *reply;
    const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? atoi(argv[2]) : 6379;

    struct config cfg = {
        .config_tcp = {
            .host = "127.0.0.1",
            .port = 6379
        },
        .config_unix = {
            .path = "/tmp/redis.sock"
        }
    };

    c = connect(cfg);
    
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    /* PING server */
    reply = redisCommand(c,"PING");
    printf("PING: %s\n", reply->str);
    freeReplyObject(reply);

    /* Set a key */
    reply = redisCommand(c,"SET %s %s", "foo", "hello world");
    printf("SET: %s\n", reply->str);
    freeReplyObject(reply);

    /* Set a key using binary safe API */
    reply = redisCommand(c,"SET %b %b", "bar", (size_t) 3, "hello", (size_t) 5);
    printf("SET (binary API): %s\n", reply->str);
    freeReplyObject(reply);

    /* Try a GET and two INCR */
    reply = redisCommand(c,"GET foo");
    printf("GET foo: %s\n", reply->str);
    freeReplyObject(reply);

    reply = redisCommand(c,"INCR counter");
    printf("INCR counter: %lld\n", reply->integer);
    freeReplyObject(reply);
    /* again ... */
    reply = redisCommand(c,"INCR counter");
    printf("INCR counter: %lld\n", reply->integer);
    freeReplyObject(reply);

    /* Create a list of numbers, from 0 to 9 */
    reply = redisCommand(c,"DEL mylist");
    freeReplyObject(reply);
    for (j = 0; j < 10; j++) {
        char buf[64];

        snprintf(buf,64,"%d",j);
        reply = redisCommand(c,"LPUSH mylist element-%s", buf);
        freeReplyObject(reply);
    }

    /* Let's check what we have inside the list */
    reply = redisCommand(c,"LRANGE mylist 0 -1");
    if (reply->type == REDIS_REPLY_ARRAY) {
        for (j = 0; j < reply->elements; j++) {
            printf("%u) %s\n", j, reply->element[j]->str);
        }
    }
    freeReplyObject(reply);

    /* Disconnects and frees the context */
    redisFree(c);

    return 0;
}
