CXX = g++

SRC_DIR		:= /home/yihan/nus-sys/cygnus

RUNTIME_DIR	= $(SRC_DIR)/Cygnus
THREAD_DIR	= $(SRC_DIR)/mthread
TARGET_DIR	= /home/yihan/benchmark/YCSB_redis_client

HOADRD_DIR	= /home/yihan/Hoard/src

CFLAGS 		:= -O3 -g -fno-stack-protector -Wall -fPIC
LIBPATH 	:= -I/usr/include/ -I$(TARGET_DIR)/ -I$(RUNTIME_DIR)/include/ -I$(THREAD_DIR)/include/
CFLAGS 		+= $(LIBPATH)

TARGET		:= YCSB-client

LDFLAGS 	:= -L/usr/lib -L$(THREAD_DIR) -lmthread -L$(RUNTIME_DIR) -lcygnus -L$(HOADRD_DIR) -lhoard -ltbb -lhiredis

# DPDK
LIBDPDK_CFLAGS := $(shell pkg-config --cflags libdpdk)
LIBDPDK_LDFLAGS := $(shell pkg-config --libs libdpdk)
CFLAGS 		+= $(LIBDPDK_CFLAGS)
LDFLAGS 	+= $(LIBDPDK_LDFLAGS)

CXXFLAGS	+= $(CFLAGS)

all: $(TARGET)

CXX_STANDARD	:= -std=gnu++11

CORE_DIR	:= $(TARGET_DIR)/core

SUBDIRS		:= core

CXX_SRC		:= $(wildcard $(CORE_DIR)/*.cc)

OBJECTS 	:= $(CXX_SRC:.cc=.o)

$(SUBDIRS):
	$(MAKE) -C $@

$(TARGET): $(wildcard *.cc) $(OBJECTS)
	$(CXX) $(CFLAGS) $(CXX_STANDARD) $^ $(LDFLAGS) -o $@

all: $(SUBDIRS) $(TARGET)

clean:
	@rm -rf $(OBJECTS) $(TARGET)