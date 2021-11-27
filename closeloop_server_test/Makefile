# RTE_SDK=/home/yihan/mtcp/dpdk
# RTE_TARGET=x86_64-native-linuxapp-gcc

TARGETS = server client
CC		= g++
DPDK	= 1
PS		= 0
NETMAP	= 0
ONVM	= 0
CCP		= 0
CFLAGS	= -std=c++11 -g -O3 -fkeep-inline-functions -march=x86-64 -msse4.1 #-fgnu89-inline

# Add arch-specific optimization
ifeq ($(shell uname -m),x86_64)
LIBS += -m64
endif

# mtcp library and header 
MTCP_FLD    =/home/yihan/mtcp/mtcp
MTCP_INC    =-I${MTCP_FLD}/include -I${MTCP_FLD}/src/include
MTCP_LIB    =-L${MTCP_FLD}/lib
MTCP_TARGET = ${MTCP_LIB}/libmtcp.a

UTIL_FLD = /home/yihan/mtcp/util
UTIL_INC = -I${UTIL_FLD}/include
UTIL_OBJ = ${UTIL_FLD}/http_parsing.o ${UTIL_FLD}/tdate_parse.o ${UTIL_FLD}/netlib.o

# util library and header
INC = -I./include/ ${UTIL_INC} ${MTCP_INC} -I${UTIL_FLD}/include -I/home/yihan/mtcp/io_engine/include
LIBS = -g -O3 -pthread -lrt -march=native ${MTCP_FLD}/lib/libmtcp.a -lnuma -lmtcp -lpthread -lrt -ldl -lgmp ${MTCP_LIB} -lpthread 

# DPDK
LIBDPDK_CFLAGS := $(shell pkg-config --cflags libdpdk)
LIBDPDK_LDFLAGS := $(shell pkg-config --libs libdpdk)
CFLAGS += $(LIBDPDK_CFLAGS)
LIB += $(LIBDPDK_LDFLAGS)

ifeq ($V,) # no echo
	export MSG=@echo
	export HIDE=@
else
	export MSG=@\#
	export HIDE=
endif

ifeq ($(CCP), 1)
# LIBCCP
LIBCCP = $(MTCP_FLD)/src/libccp
LIBS += -L$(LIBCCP) -lccp -lstartccp
INC += -I$(LIBCCP)
endif

ifeq ($(RTT),1)
CFLAGS += -DEVAL_RTT
endif

server: server.cc 
	$(CC) $(CFLAGS) ${MTCP_FLD}/lib/libmtcp.a $^ $(INC) ${LIBS} ${UTIL_OBJ} -o $@ $(LDFLAGS) 

client: client.cc 
	$(CC) $(CFLAGS) ${MTCP_FLD}/lib/libmtcp.a $^ $(INC) ${LIBS} ${UTIL_OBJ} -o $@ $(LDFLAGS) 

clean:
		rm -f *.o $(TARGETS)

# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2010-2015 Intel Corporation

# include $(RTE_SDK)/mk/rte.vars.mk

# ifeq ($(CONFIG_RTE_TEST_PMD),y)

# #
# # library name
# #
# APP = server

# #
# # all source are stored in SRCS-y
# #

# MTCP_FLD    =/home/yihan/mtcp/mtcp
# MTCP_INC    =-I${MTCP_FLD}/include -I${MTCP_FLD}/src/include
# MTCP_LIB    =-L${MTCP_FLD}/lib
# MTCP_TARGET = ${MTCP_LIB}/libmtcp.a

# UTIL_FLD = /home/yihan/mtcp/util
# UTIL_INC = -I${UTIL_FLD}/include
# UTIL_OBJ = ${UTIL_FLD}/http_parsing.o ${UTIL_FLD}/tdate_parse.o ${UTIL_FLD}/netlib.o

# SRCS-y := server.cc

# ifeq ($(CONFIG_RTE_BUILD_SHARED_LIB),y)

# ifeq ($(CONFIG_RTE_LIBRTE_PMD_BOND),y)
# LDLIBS += -lrte_pmd_bond
# endif

# ifeq ($(CONFIG_RTE_LIBRTE_DPAA_BUS)$(CONFIG_RTE_LIBRTE_DPAA_PMD),yy)
# LDLIBS += -lrte_pmd_dpaa
# LDLIBS += -lrte_bus_dpaa
# LDLIBS += -lrte_mempool_dpaa
# endif

# ifeq ($(CONFIG_RTE_LIBRTE_IXGBE_PMD),y)
# LDLIBS += -lrte_pmd_ixgbe
# endif

# ifeq ($(CONFIG_RTE_LIBRTE_I40E_PMD),y)
# LDLIBS += -lrte_pmd_i40e
# endif

# ifeq ($(CONFIG_RTE_LIBRTE_BNXT_PMD),y)
# LDLIBS += -lrte_pmd_bnxt
# endif

# ifeq ($(CONFIG_RTE_LIBRTE_PMD_SOFTNIC),y)
# LDLIBS += -lrte_pmd_softnic
# endif

# endif

# CFLAGS		+= -O3 -g -fno-stack-protector -I/usr/include/ -I./include/ -I/home/yihan/mtcp/io_engine/include ${MTCP_INC} ${UTIL_INC}

# LDFLAGS 	+= -lnuma -lmtcp -lpthread -lrt -ldl -lgmp -L/home/yihan/mtcp/mtcp/lib -l:libmtcp.a ${UTIL_OBJ}

# include $(RTE_SDK)/mk/rte.app.mk

# endif
