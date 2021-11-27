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

#define MEMPOOL_ELT_SIZE 2048
#define MAX_KEEP 16
#define MEMPOOL_SIZE ((rte_lcore_count()*(MAX_KEEP+RTE_MEMPOOL_CACHE_MAX_SIZE))-1)

/*
 * RX and TX Prefetch, Host, and Write-back threshold values should be
 * carefully set for optimal performance. Consult the network
 * controller's datasheet and supporting DPDK documentation for guidance
 * on how these parameters should be set.
 */
#define RX_PTHRESH 			8 /**< Default values of RX prefetch threshold reg. */
#define RX_HTHRESH 			8 /**< Default values of RX host threshold reg. */
#define RX_WTHRESH 			4 /**< Default values of RX write-back threshold reg. */

/*
 * These default values are optimized for use with the Intel(R) 82599 10 GbE
 * Controller and the DPDK ixgbe PMD. Consider using other values for other
 * network controllers and/or network drivers.
 */
#define TX_PTHRESH 			36 /**< Default values of TX prefetch threshold reg. */
#define TX_HTHRESH			0  /**< Default values of TX host threshold reg. */
#define TX_WTHRESH			0  /**< Default values of TX write-back threshold reg. */

static const struct rte_eth_rxconf rx_conf = {
	.rx_thresh = {
		.pthresh = 		RX_PTHRESH, /* RX prefetch threshold reg */
		.hthresh = 		RX_HTHRESH, /* RX host threshold reg */
		.wthresh = 		RX_WTHRESH, /* RX write-back threshold reg */
	},
	.rx_free_thresh = 		32,
};

static const struct rte_eth_txconf tx_conf = {
	.tx_thresh = {
		.pthresh = 		TX_PTHRESH, /* TX prefetch threshold reg */
		.hthresh = 		TX_HTHRESH, /* TX host threshold reg */
		.wthresh = 		TX_WTHRESH, /* TX write-back threshold reg */
	},
	.tx_free_thresh = 		0, /* Use PMD default values */
	.tx_rs_thresh = 		0, /* Use PMD default values */
};

int
start_port(portid_t pid)
{
	struct rte_eth_conf port_conf;
	int ret;

	struct rte_mempool * mp = rte_mempool_create("mempool",
		MEMPOOL_SIZE,
		MEMPOOL_ELT_SIZE,
		RTE_MEMPOOL_CACHE_MAX_SIZE, 0,
		NULL, 0, NULL, 0, rte_socket_id(), 0);

	ret = rte_eth_dev_configure(pid, 1, 1, &port_conf);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Cannot configure device: err=%d, port=%u, cores: %d\n",
				ret, (unsigned) pid, 1);

	ret = rte_eth_rx_queue_setup(pid, 0, 4096, rte_eth_dev_socket_id(pid), &rx_conf, mp);
	if (ret < 0)
		rte_exit(EXIT_FAILURE,
				"rte_eth_rx_queue_setup:err=%d, port=%u, queueid: %d\n",
				ret, (unsigned) pid, 0);

	return ret;
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

	if (start_port(0) != 0)
		rte_exit(EXIT_FAILURE, "Start ports failed\n");

	return 0;
}
