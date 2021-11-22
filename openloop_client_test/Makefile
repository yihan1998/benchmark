#export RTE_SDK=/home/samaritan/Github/sail/dpdk
export RTE_SDK=/home/yihan-18/cetus/dpdk
export RTE_TARGET=x86_64-native-linuxapp-gcc

include $(RTE_SDK)/mk/rte.vars.mk

# binary name
APP 		=  openloop_client_test

#SRC_DIR		:= /home/samaritan/Github/sail
SRC_DIR		:= /home/yihan-18/cetus

RUNTIME_DIR	= $(SRC_DIR)/Cetus
THREAD_DIR	:= $(SRC_DIR)/mthread
#SAIL_DIR	:= $(SRC_DIR)/SAIL

SRCS-y		:= 	openloop_client_test.c

CFLAGS		+= -O3 -g -fno-stack-protector
CFLAGS		+= -I/usr/include/ -I$(RUNTIME_DIR)/include/ -I$(THREAD_DIR)/include/

LDFLAGS 	+= -L/usr/lib -L$(RUNTIME_DIR)/build -lcetus -L$(THREAD_DIR)/build -lmthread

ASFLAGS 	+= -f elf64 -O0

include $(RTE_SDK)/mk/rte.extapp.mk
