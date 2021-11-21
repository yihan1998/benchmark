APP = client

SRCS = closeloop_client_test.c

CC = gcc
CFLAGS = -g -O3
LDFLAGS = -lpthread 

ifeq ($(RTT),1)
CFLAGS += -DEVAL_RTT
endif

ifeq ($(TAS_BIND),1)
CFLAGS += -DEVAL_TAS_BIND
endif

ifeq ($(TAS_SEP),1)
CFLAGS += -DEVAL_TAS_SEP
endif

INC = -I ./include

all: $(APP)

$(APP): $(SRCS)
	$(CC) $(CFLAGS) $^ $(INC) -o $@ $(LDFLAGS) 

clean:
	rm -f *.o $(APP)