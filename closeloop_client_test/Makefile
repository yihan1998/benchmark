CC = gcc

CFLAGS 		:= -O3 -g -fno-stack-protector -fPIC -msse4.1 -mavx -mavx512f

SRC_DIR		:= /home/yihan/cygnus

RUNTIME_DIR	= $(SRC_DIR)/Cygnus
THREAD_DIR	= $(SRC_DIR)/mthread

LIBPATH 	:= -I/usr/include/ -I$(RUNTIME_DIR)/include/ -I$(THREAD_DIR)/include/
CFLAGS 		+= $(LIBPATH)

TARGET		:= closeloop_client_test

LDFLAGS 	:= -L/usr/lib -L$(THREAD_DIR) -lmthread -L$(RUNTIME_DIR) -lcygnus

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