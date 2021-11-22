# #export RTE_SDK=/home/samaritan/Github/sail/dpdk
# export RTE_SDK=/home/yihan-18/cetus/dpdk
# export RTE_TARGET=x86_64-native-linuxapp-gcc

# include $(RTE_SDK)/mk/rte.vars.mk

# # binary name
# APP 		=  closeloop_client_test

# #SRC_DIR		:= /home/samaritan/Github/sail
# SRC_DIR		:= /home/yihan-18/nus-sys/cetus

# RUNTIME_DIR	= $(SRC_DIR)/Cetus
# THREAD_DIR	:= $(SRC_DIR)/mthread

# SRCS-y		:= 	closeloop_client_test.c

# ifeq ($(EVAL_RTT),1)
# CFLAGS += -DEVAL_RTT
# endif

# CFLAGS		+= -O3 -g -fno-stack-protector
# CFLAGS		+= -I/usr/include/ -I$(RUNTIME_DIR)/include/ -I$(THREAD_DIR)/include/

# LDFLAGS 	+= -L/usr/lib -L$(RUNTIME_DIR)/build -lcetus -L$(THREAD_DIR)/build -lmthread

# ASFLAGS 	+= -f elf64 -O0

# include $(RTE_SDK)/mk/rte.app.mk

CC = gcc

CFLAGS 		:= -O3 -g -fno-stack-protector -fPIC

SRC_DIR		:= /home/yihan/nus-sys/cetus

RUNTIME_DIR	= $(SRC_DIR)/Cetus
THREAD_DIR	= $(SRC_DIR)/mthread

LIBPATH 	:= -I/usr/include/ -I$(RUNTIME_DIR)/include/ -I$(THREAD_DIR)/include/
CFLAGS 		+= $(LIBPATH)

TARGET		:= closeloop_client_test

LDFLAGS 	:= -L/usr/lib -L$(THREAD_DIR) -lmthread -L$(RUNTIME_DIR) -lcetus

# DPDK
LIBDPDK_CFLAGS := $(shell pkg-config --cflags libdpdk)
LIBDPDK_LDFLAGS := $(shell pkg-config --libs libdpdk)
CFLAGS += $(LIBDPDK_CFLAGS)
LDFLAGS += $(LIBDPDK_LDFLAGS)

all: $(TARGET)

C_STANDARD	:= -std=gnu11

C_SRC	:= closeloop_client_test.c

OBJECTS := $(C_SRC:.c=.o)

%.o: %.c
	@echo Compiling $(<F)
	$(CC) $(C_STANDARD) $(CFLAGS) -c $< -o $(@)

$(TARGET): $(OBJECTS)
	@echo ' '
	@echo 'Building $(@)'
	@echo 'Invoking Linker'
	$(CC) $(OBJECTS) $(LDFLAGS) -o $(@)
	@echo 'Finished building: $@'
	@echo ' '

clean:
	@rm -rf $(OBJECTS) $(TARGET)