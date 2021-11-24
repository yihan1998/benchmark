#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

/* State for the protocol parser */
struct reader {
    char * buf; /* Read buffer */
    size_t pos; /* Buffer cursor */
    size_t len; /* Buffer length */
    size_t maxbuf; /* Max length of unused buffer */
};

struct reply {
    int type; /* REDIS_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    int len; /* Length of string */
    char * str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
    struct reply ** element; /* elements vector for REDIS_REPLY_ARRAY */
};

static char * readBytes(struct reader * r, unsigned int bytes) {
    char * p;
    if (r->len-r->pos >= bytes) {
        p = r->buf+r->pos;
        r->pos += bytes;
        return p;
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

/* Find pointer to \r\n. */
static char * seekNewline(char * s, size_t len) {
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

static char *readLine(struct reader * r, int * _len) {
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

int parseReply(struct reader * r, struct reply * reply) {
    char * p;
    fprintf(stdout, " [%s:%d] remain to parse: %.*s\n", __func__, __LINE__, r->len - r->pos, r->buf + r->pos);
    if ((p = readBytes(r, 1)) != NULL) {
        if (p[0] == '-') {
            fprintf(stdout, " - REDIS_REPLY_ERROR");
            reply->type = REDIS_REPLY_ERROR;
            if ((s = readLine(r,&len)) != NULL) {
                return 0;
            } else {
                return -1;
            }
        } else if (p[0] == '+') {
            fprintf(stdout, " + REDIS_REPLY_STATUS");
            reply->type = REDIS_REPLY_STATUS;
            if ((s = readLine(r,&len)) != NULL) {
                return 0;
            } else {
                return -1;
            }
        } else if (p[0] == ':') {
            fprintf(stdout, " : REDIS_REPLY_INTEGER");
            reply->type = REDIS_REPLY_INTEGER;
            if ((s = readLine(r,&len)) != NULL) {
                reply->integer = readLongLong(s);
                return 0;
            } else {
                return -1;
            }
        } else if (p[0] == '$') {
            fprintf(stdout, " $ REDIS_REPLY_STRING");
            reply->type = REDIS_REPLY_STRING;
            char * s;
            int len;
            fprintf(stdout, " \t string len: %d\n", reply->len);
            if ((s = readLine(r,&len)) != NULL) {
                reply->len = readLongLong(s);
                int read_len;
                if (reply->str = readLine(r, &read_len) != NULL) {
                    fprintf(stdout, " \t string: %s\n", reply->str);
                    if (reply->len == read_len) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else {
                    return -1;
                }
            } else {
                return -1;
            }
        } else if (p[0] == '*') {
            fprintf(stdout, " * REDIS_REPLY_ARRAY");
            reply->type = REDIS_REPLY_ARRAY;
            char * s;
            int len;
            if ((s = readLine(r,&len)) != NULL) {
                reply->elements = len;
                fprintf(stdout, " \t Array len: %d\n", len);
                reply->element = (struct reply **)calloc(reply->elements, sizeof(struct reply));
                for (int i = 0; i < len; i++) {
                    if(parseReply(r, reply->element[i]) == -1) {
                        return -1;
                    }
                }
                return 0;
            } else {
                return -1;
            }
        }
    }
}

struct reply * getReply(char * buf, int len) {
    // return err;
    struct reader r = {.buf = buf, .pos = 0, .len = len, .maxbuf = 1024*16};
    struct reply * reply = (struct reply *)calloc(1, sizeof(struct reply));

    int ret = parseReply(&r, reply);

    if (ret == -1) {
        return NULL;
    }

    return reply;
}

int main(int argc, char ** argv) {
    struct reply * reply;

    printf(" ***** Test 1 : OK reply *****\n");
    char test1[] = "+OK\r\n";
    reply = getReply(test1, strlen(test1));
    assert(reply->type == REDIS_REPLY_STATUS);

    printf(" ***** Test 2 : ERROR reply *****\n");
    char test2[] = "-ERROR\r\n";
    reply = getReply(test2, strlen(test2));
    assert(reply->type == REDIS_REPLY_ERROR);

    printf(" ***** Test 3 : Integer reply *****\n");
    char test3[] = ":12\r\n";
    reply = getReply(test3, strlen(test3));
    assert(reply->type == REDIS_REPLY_INTEGER);
    assert(reply->integer == 12);

    printf(" ***** Test 4 : STRING reply *****\n");
    char test4[] = "$2\r\n41\r\n";
    reply = getReply(test4, strlen(test4));
    assert(reply->type == REDIS_REPLY_STRING);
    assert(reply->len == 2);
    print(" \t receive string: %s\n", reply->str);

    printf(" ***** Test 5 : ARRAY reply *****\n");
    char test4[] = "*2\r\n$6\r\nmylist\r\n$8\r\nabcdefgh\r\n";
    reply = getReply(test4, strlen(test4));
    assert(reply->type == REDIS_REPLY_ARRAY);
    assert(reply->elements == 2);
    assert(reply->elements[0]->len == 6);
    print(" \t receive string: %s\n", reply->elements[0]->str);
    assert(reply->elements[1]->len == 8);
    print(" \t receive string: %s\n", reply->elements[1]->str);
}