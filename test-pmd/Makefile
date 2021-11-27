# # SPDX-License-Identifier: BSD-3-Clause
# # Copyright(c) 2010-2015 Intel Corporation

# include $(RTE_SDK)/mk/rte.vars.mk

# ifeq ($(CONFIG_RTE_TEST_PMD),y)

# #
# # library name
# #
# APP = testpmd

# CFLAGS += -DALLOW_EXPERIMENTAL_API
# CFLAGS += -O3
# CFLAGS += $(WERROR_FLAGS)
# CFLAGS += -Wno-deprecated-declarations

# #
# # all source are stored in SRCS-y
# #
# SRCS-y := testpmd.c

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

# include $(RTE_SDK)/mk/rte.app.mk

# endif

CC = gcc

CFLAGS 		:= -O3 -g

TARGET		:= testpmd

LDFLAGS 	:= -L/usr/lib -L$(THREAD_DIR) -lmthread -L$(RUNTIME_DIR) -lcygnus

# DPDK
LIBDPDK_CFLAGS := $(shell pkg-config --cflags libdpdk)
LIBDPDK_LDFLAGS := $(shell pkg-config --libs libdpdk)
CFLAGS += $(LIBDPDK_CFLAGS)
LDFLAGS += $(LIBDPDK_LDFLAGS)

all: $(TARGET)

C_STANDARD	:= -std=gnu11

C_SRC	:= testpmd.c

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