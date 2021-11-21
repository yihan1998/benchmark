RTE_SDK = /home/yihan/mtcp/dpdk
RTE_TARGET = x86_64-native-linuxapp-gcc

CC		= g++
DPDK	= 1
CCP		= 1
CFLAGS	= -std=c++11 -g -O3 -fkeep-inline-functions -march=x86-64 -msse4.1 #-fgnu89-inline
LDFLAGS	= -lpthread -ltbb -lhiredis
SUBDIRS	= core db redis
SUBSRCS	= $(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS	= $(SUBSRCS:.cc=.o)
TARGETS	= client

# Add arch-specific optimization
ifeq ($(shell uname -m),x86_64)
LIBS += -m64
endif

# mtcp library and header 
SRC_FLD    = /home/yihan/mtcp

MTCP_FLD	= $(SRC_FLD)/mtcp
MTCP_INC    = -I${MTCP_FLD}/include -I${MTCP_FLD}/src/include
MTCP_LIB    = -L${MTCP_FLD}/lib
MTCP_TARGET = ${MTCP_LIB}/libmtcp.a

UTIL_FLD 	= $(SRC_FLD)/util
UTIL_INC 	= -I${UTIL_FLD}/include
UTIL_OBJ 	= ${UTIL_FLD}/http_parsing.o ${UTIL_FLD}/tdate_parse.o ${UTIL_FLD}/netlib.o

# util library and header
INC 		= -I./ ${UTIL_INC} ${MTCP_INC}
LIBS 		= ${MTCP_LIB} -lpthread -lrt -march=native ${MTCP_FLD}/lib/libmtcp.a -lnuma -lmtcp -lrt -ldl -lgmp

# dpdk-specific variables
DPDK_MACHINE_LINKER_FLAGS=${RTE_SDK}/${RTE_TARGET}/lib/ldflags.txt
DPDK_MACHINE_LDFLAGS=$(shell cat ${DPDK_MACHINE_LINKER_FLAGS})
DPDK_INC = ${RTE_SDK}/${RTE_TARGET}/include
INC += -I$(SRC_FLD)/mtcp/io_engine/include -I${DPDK_INC} 
LIBS += -lrt -march=native ${MTCP_FLD}/lib/libmtcp.a -lnuma -lmtcp -lrt -ldl -lgmp -L${RTE_SDK}/${RTE_TARGET}/lib ${DPDK_MACHINE_LDFLAGS}

all: $(SUBDIRS) $(TARGETS)

$(SUBDIRS):
	$(MAKE) -C $@

$(TARGETS): $(wildcard *.cc) $(OBJECTS) ${MTCP_FLD}/lib/libmtcp.a
	$(CC) $(CFLAGS) ${UTIL_OBJ} $^ $(INC) ${LIBS} -o $@ $(LDFLAGS)

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(TARGETS)

.PHONY: $(SUBDIRS) $(TARGETS)

