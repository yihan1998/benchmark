#include "parse.h"

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
    // fprintf(stdout, " [%s:%d] remain to parse: %.*s\n", __func__, __LINE__, r->len - r->pos, r->buf + r->pos);
    if ((p = readBytes(r, 1)) != NULL) {
        if (p[0] == '-') {
            fprintf(stdout, " - REDIS_REPLY_ERROR\n");
            reply->type = REDIS_REPLY_ERROR;
            char * s;
            int len;
            if ((s = readLine(r,&len)) != NULL) {
                return 0;
            } else {
                return -1;
            }
        } else if (p[0] == '+') {
            fprintf(stdout, " + REDIS_REPLY_STATUS\n");
            reply->type = REDIS_REPLY_STATUS;
            char * s;
            int len;
            if ((s = readLine(r,&len)) != NULL) {
                return 0;
            } else {
                return -1;
            }
        } else if (p[0] == ':') {
            fprintf(stdout, " : REDIS_REPLY_INTEGER\n");
            reply->type = REDIS_REPLY_INTEGER;
            char * s;
            int len;
            if ((s = readLine(r,&len)) != NULL) {
                reply->integer = readLongLong(s);
                return 0;
            } else {
                return -1;
            }
        } else if (p[0] == '$') {
            fprintf(stdout, " $ REDIS_REPLY_STRING\n");
            reply->type = REDIS_REPLY_STRING;
            char * s;
            int len;
            if ((s = readLine(r,&len)) != NULL) {
                reply->len = readLongLong(s);
                fprintf(stdout, " \t string len: %d\n", reply->len);
                int read_len;
                if ((reply->str = readLine(r, &read_len)) != NULL) {
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
            fprintf(stdout, " * REDIS_REPLY_ARRAY\n");
            reply->type = REDIS_REPLY_ARRAY;
            char * s;
            int len;
            if ((s = readLine(r,&len)) != NULL) {
                reply->elements = readLongLong(s);
                // fprintf(stdout, " \t Array len: %d\n", reply->elements);
                reply->element = (struct reply **)calloc(reply->elements, sizeof(struct reply *));
                // printf(" [%s:%d] Element address: %p\n", __func__, __LINE__, reply->element);
                for (int i = 0; i < reply->elements; i++) {
                    reply->element[i] = (struct reply *)calloc(1, sizeof(struct reply));
                    if(parseReply(r, reply->element[i]) == -1) {
                        return -1;
                    }
                    fprintf(stdout, " [%s:%d] Array[%d] : %s\n", __func__, __LINE__, i, reply->element[i]);
                }
                return 0;
            } else {
                return -1;
            }
        }
    } else {
        return -1;
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