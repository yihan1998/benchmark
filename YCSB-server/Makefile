RTE_SDK		= /home/yihan-18/nus-sys/cetus/dpdk
RTE_TARGET	= x86_64-native-linuxapp-gcc

CC		= g++
DPDK	= 1
CCP		= 1
CFLAGS	= -std=c++11 -g -O3 -fkeep-inline-functions -march=x86-64 -msse4.1 -lrte_pmd_mlx5 #-fgnu89-inline
LDFLAGS	= -lpthread -ltbb -lhiredis -lmemcached
SUBDIRS	= core db redis
SUBSRCS	= $(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS	= $(SUBSRCS:.cc=.o)
TARGETS	= server

# Add arch-specific optimization
ifeq ($(shell uname -m),x86_64)
LIBS += -m64
endif

# mtcp library and header 
SRC_FLD    = /home/yihan-18/mtcp

MTCP_FLD	= $(SRC_FLD)/mtcp
MTCP_INC    = -I${MTCP_FLD}/include -I${MTCP_FLD}/src/include
MTCP_LIB    = -L${MTCP_FLD}/lib
MTCP_TARGET = ${MTCP_LIB}/libmtcp.a

UTIL_FLD 	= $(SRC_FLD)/util
UTIL_INC 	= -I${UTIL_FLD}/include
UTIL_OBJ 	= ${UTIL_FLD}/http_parsing.o ${UTIL_FLD}/tdate_parse.o ${UTIL_FLD}/netlib.o

# util library and header
INC 		= -I./ ${UTIL_INC} ${MTCP_INC}
LIBS 		= ${MTCP_LIB} -lpthread 

# dpdk-specific variables
ifeq ($(DPDK),1)
DPDK_MACHINE_LINKER_FLAGS	=${RTE_SDK}/${RTE_TARGET}/lib/ldflags.txt
DPDK_MACHINE_LDFLAGS		=$(shell cat ${DPDK_MACHINE_LINKER_FLAGS})
DPDK_INC	= ${RTE_SDK}/${RTE_TARGET}/include
INC			+= -I$(SRC_FLD)/io_engine/include -I${DPDK_INC} 
LIBS		+= -g -O3 -pthread -lrt -march=native ${MTCP_FLD}/lib/libmtcp.a -lnuma -lmtcp -lpthread -lrt -ldl -lgmp -L${RTE_SDK}/${RTE_TARGET}/lib ${DPDK_MACHINE_LDFLAGS}
endif

ifeq ($(CCP), 1)
# LIBCCP
LIBCCP = $(MTCP_FLD)/src/libccp
LIBS += -L$(LIBCCP) -lccp
INC += -I$(LIBCCP)
endif

all: $(SUBDIRS) $(TARGETS)

$(SUBDIRS):
	$(MAKE) -C $@

$(TARGETS): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) ${MTCP_FLD}/lib/libmtcp.a ${UTIL_OBJ} $^ $(INC) ${LIBS} -o $@ $(LDFLAGS)

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(TARGETS)

.PHONY: $(SUBDIRS) $(TARGETS)

