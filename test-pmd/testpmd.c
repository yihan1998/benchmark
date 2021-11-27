/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2017 Intel Corporation
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <errno.h>
#include <stdbool.h>

#include <sys/queue.h>
#include <sys/stat.h>

#include <stdint.h>
#include <unistd.h>
#include <inttypes.h>

#include <rte_common.h>
#include <rte_errno.h>
#include <rte_byteorder.h>
#include <rte_log.h>
#include <rte_debug.h>
#include <rte_cycles.h>
#include <rte_memory.h>
#include <rte_memcpy.h>
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_alarm.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_atomic.h>
#include <rte_branch_prediction.h>
#include <rte_mempool.h>
#include <rte_malloc.h>
#include <rte_mbuf.h>
#include <rte_mbuf_pool_ops.h>
#include <rte_interrupts.h>
#include <rte_pci.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_dev.h>
#include <rte_string_fns.h>
#ifdef RTE_LIBRTE_IXGBE_PMD
#include <rte_pmd_ixgbe.h>
#endif
#ifdef RTE_LIBRTE_PDUMP
#include <rte_pdump.h>
#endif
#include <rte_flow.h>
#include <rte_metrics.h>
#ifdef RTE_LIBRTE_BITRATE
#include <rte_bitrate.h>
#endif
#ifdef RTE_LIBRTE_LATENCY_STATS
#include <rte_latencystats.h>
#endif

#include "testpmd.h"

static void
signal_handler(int signum)
{
	if (signum == SIGINT || signum == SIGTERM) {
		printf("\nSignal %d received, preparing to exit...\n",
				signum);
#ifdef RTE_LIBRTE_PDUMP
		/* uninitialize packet capture framework */
		rte_pdump_uninit();
#endif
		/* exit with the expected status */
		signal(signum, SIG_DFL);
		kill(getpid(), signum);
	}
}

int
main(int argc, char** argv)
{
	int diag;

	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);

	diag = rte_eal_init(argc, argv);
	if (diag < 0)
		rte_exit(EXIT_FAILURE, "Cannot init EAL: %s\n",
			 rte_strerror(rte_errno));
	return 0;
}
