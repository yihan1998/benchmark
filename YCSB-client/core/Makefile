CXX		?= g++

SRC_DIR		:= /home/yihan/nus-sys/cetus

RUNTIME_DIR	= $(SRC_DIR)/Cetus
THREAD_DIR	= $(SRC_DIR)/mthread
TARGET_DIR	= $(SRC_DIR)/test/YCSB-server

CFLAGS 		:= -std=gnu++11 -O3 -g -Wall -fgnu-tm
LIBPATH 	:= -I/usr/include/ -I$(TARGET_DIR)/ -I$(RUNTIME_DIR)/include/ -I$(THREAD_DIR)/include/
CFLAGS 		+= $(LIBPATH)

CXX_STANDARD	:= -std=gnu++11

SRCS	= $(wildcard *.cc)
OBJS	= $(SRCS:.cc=.o)

%.o: %.cc
	@echo Compiling $(<F)
	$(CXX) $(CXX_STANDARD) $(CFLAGS) -c $< -o $(@)

all: $(OBJS)

clean:
	$(RM) $(OBJS)

