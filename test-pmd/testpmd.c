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

#ifndef MAP_HUGETLB
/* FreeBSD may not have MAP_HUGETLB (in fact, it probably doesn't) */
#define HUGE_FLAG (0x40000)
#else
#define HUGE_FLAG MAP_HUGETLB
#endif

#ifndef MAP_HUGE_SHIFT
/* older kernels (or FreeBSD) will not have this define */
#define HUGE_SHIFT (26)
#else
#define HUGE_SHIFT MAP_HUGE_SHIFT
#endif

#define EXTMEM_HEAP_NAME "extmem"

uint16_t verbose_level = 0; /**< Silent by default. */
int testpmd_logtype; /**< Log type for testpmd logs */

/* use master core for command line ? */
uint8_t interactive = 0;
uint8_t auto_start = 0;
uint8_t tx_first;
char cmdline_filename[PATH_MAX] = {0};

/*
 * NUMA support configuration.
 * When set, the NUMA support attempts to dispatch the allocation of the
 * RX and TX memory rings, and of the DMA memory buffers (mbufs) for the
 * probed ports among the CPU sockets 0 and 1.
 * Otherwise, all memory is allocated from CPU socket 0.
 */
uint8_t numa_support = 1; /**< numa enabled by default */

/*
 * In UMA mode,all memory is allocated from socket 0 if --socket-num is
 * not configured.
 */
uint8_t socket_num = UMA_NO_CONFIG;

/*
 * Select mempool allocation type:
 * - native: use regular DPDK memory
 * - anon: use regular DPDK memory to create mempool, but populate using
 *         anonymous memory (may not be IOVA-contiguous)
 * - xmem: use externally allocated hugepage memory
 */
uint8_t mp_alloc_type = MP_ALLOC_NATIVE;

/*
 * Store specified sockets on which memory pool to be used by ports
 * is allocated.
 */
uint8_t port_numa[RTE_MAX_ETHPORTS];

/*
 * Store specified sockets on which RX ring to be used by ports
 * is allocated.
 */
uint8_t rxring_numa[RTE_MAX_ETHPORTS];

/*
 * Store specified sockets on which TX ring to be used by ports
 * is allocated.
 */
uint8_t txring_numa[RTE_MAX_ETHPORTS];

/*
 * Record the Ethernet address of peer target ports to which packets are
 * forwarded.
 * Must be instantiated with the ethernet addresses of peer traffic generator
 * ports.
 */
struct rte_ether_addr peer_eth_addrs[RTE_MAX_ETHPORTS];
portid_t nb_peer_eth_addrs = 0;

/*
 * Probed Target Environment.
 */
struct rte_port *ports;	       /**< For all probed ethernet ports. */
portid_t nb_ports;             /**< Number of probed ethernet ports. */
struct fwd_lcore **fwd_lcores; /**< For all probed logical cores. */
lcoreid_t nb_lcores;           /**< Number of probed logical cores. */

portid_t ports_ids[RTE_MAX_ETHPORTS]; /**< Store all port ids. */

/*
 * Test Forwarding Configuration.
 *    nb_fwd_lcores <= nb_cfg_lcores <= nb_lcores
 *    nb_fwd_ports  <= nb_cfg_ports  <= nb_ports
 */
lcoreid_t nb_cfg_lcores; /**< Number of configured logical cores. */
lcoreid_t nb_fwd_lcores; /**< Number of forwarding logical cores. */
portid_t  nb_cfg_ports;  /**< Number of configured ports. */
portid_t  nb_fwd_ports;  /**< Number of forwarding ports. */

unsigned int fwd_lcores_cpuids[RTE_MAX_LCORE]; /**< CPU ids configuration. */
portid_t fwd_ports_ids[RTE_MAX_ETHPORTS];      /**< Port ids configuration. */

struct fwd_stream **fwd_streams; /**< For each RX queue of each port. */
streamid_t nb_fwd_streams;       /**< Is equal to (nb_ports * nb_rxq). */

/*
 * Forwarding engines.
 */
struct fwd_engine * fwd_engines[] = {
	&io_fwd_engine,
	&mac_fwd_engine,
	&mac_swap_engine,
	&flow_gen_engine,
	&rx_only_engine,
	&tx_only_engine,
	&csum_fwd_engine,
	&icmp_echo_engine,
	&noisy_vnf_engine,
#if defined RTE_LIBRTE_PMD_SOFTNIC
	&softnic_fwd_engine,
#endif
#ifdef RTE_LIBRTE_IEEE1588
	&ieee1588_fwd_engine,
#endif
	NULL,
};

struct rte_mempool *mempools[RTE_MAX_NUMA_NODES];
uint16_t mempool_flags;

struct fwd_config cur_fwd_config;
struct fwd_engine *cur_fwd_eng = &io_fwd_engine; /**< IO mode by default. */
uint32_t retry_enabled;
uint32_t burst_tx_delay_time = BURST_TX_WAIT_US;
uint32_t burst_tx_retry_num = BURST_TX_RETRIES;

uint16_t mbuf_data_size = DEFAULT_MBUF_DATA_SIZE; /**< Mbuf data space size. */
uint32_t param_total_num_mbufs = 0;  /**< number of mbufs in all pools - if
                                      * specified on command-line. */
uint16_t stats_period; /**< Period to show statistics (disabled by default) */

/*
 * In container, it cannot terminate the process which running with 'stats-period'
 * option. Set flag to exit stats period loop after received SIGINT/SIGTERM.
 */
uint8_t f_quit;

/*
 * Configuration of packet segments used by the "txonly" processing engine.
 */
uint16_t tx_pkt_length = TXONLY_DEF_PACKET_LEN; /**< TXONLY packet length. */
uint16_t tx_pkt_seg_lengths[RTE_MAX_SEGS_PER_PKT] = {
	TXONLY_DEF_PACKET_LEN,
};
uint8_t  tx_pkt_nb_segs = 1; /**< Number of segments in TXONLY packets */

enum tx_pkt_split tx_pkt_split = TX_PKT_SPLIT_OFF;
/**< Split policy for packets to TX. */

uint8_t txonly_multi_flow;
/**< Whether multiple flows are generated in TXONLY mode. */

uint16_t nb_pkt_per_burst = DEF_PKT_BURST; /**< Number of packets per burst. */
uint16_t mb_mempool_cache = DEF_MBUF_CACHE; /**< Size of mbuf mempool cache. */

/* current configuration is in DCB or not,0 means it is not in DCB mode */
uint8_t dcb_config = 0;

/* Whether the dcb is in testing status */
uint8_t dcb_test = 0;

/*
 * Configurable number of RX/TX queues.
 */
queueid_t nb_hairpinq; /**< Number of hairpin queues per port. */
queueid_t nb_rxq = 1; /**< Number of RX queues per port. */
queueid_t nb_txq = 1; /**< Number of TX queues per port. */

/*
 * Configurable number of RX/TX ring descriptors.
 * Defaults are supplied by drivers via ethdev.
 */
#define RTE_TEST_RX_DESC_DEFAULT 0
#define RTE_TEST_TX_DESC_DEFAULT 0
uint16_t nb_rxd = RTE_TEST_RX_DESC_DEFAULT; /**< Number of RX descriptors. */
uint16_t nb_txd = RTE_TEST_TX_DESC_DEFAULT; /**< Number of TX descriptors. */

#define RTE_PMD_PARAM_UNSET -1
/*
 * Configurable values of RX and TX ring threshold registers.
 */

int8_t rx_pthresh = RTE_PMD_PARAM_UNSET;
int8_t rx_hthresh = RTE_PMD_PARAM_UNSET;
int8_t rx_wthresh = RTE_PMD_PARAM_UNSET;

int8_t tx_pthresh = RTE_PMD_PARAM_UNSET;
int8_t tx_hthresh = RTE_PMD_PARAM_UNSET;
int8_t tx_wthresh = RTE_PMD_PARAM_UNSET;

/*
 * Configurable value of RX free threshold.
 */
int16_t rx_free_thresh = RTE_PMD_PARAM_UNSET;

/*
 * Configurable value of RX drop enable.
 */
int8_t rx_drop_en = RTE_PMD_PARAM_UNSET;

/*
 * Configurable value of TX free threshold.
 */
int16_t tx_free_thresh = RTE_PMD_PARAM_UNSET;

/*
 * Configurable value of TX RS bit threshold.
 */
int16_t tx_rs_thresh = RTE_PMD_PARAM_UNSET;

/*
 * Configurable value of buffered packets before sending.
 */
uint16_t noisy_tx_sw_bufsz;

/*
 * Configurable value of packet buffer timeout.
 */
uint16_t noisy_tx_sw_buf_flush_time;

/*
 * Configurable value for size of VNF internal memory area
 * used for simulating noisy neighbour behaviour
 */
uint64_t noisy_lkup_mem_sz;

/*
 * Configurable value of number of random writes done in
 * VNF simulation memory area.
 */
uint64_t noisy_lkup_num_writes;

/*
 * Configurable value of number of random reads done in
 * VNF simulation memory area.
 */
uint64_t noisy_lkup_num_reads;

/*
 * Configurable value of number of random reads/writes done in
 * VNF simulation memory area.
 */
uint64_t noisy_lkup_num_reads_writes;

/*
 * Receive Side Scaling (RSS) configuration.
 */
uint64_t rss_hf = ETH_RSS_IP; /* RSS IP by default. */

/*
 * Port topology configuration
 */
uint16_t port_topology = PORT_TOPOLOGY_PAIRED; /* Ports are paired by default */

/*
 * Avoids to flush all the RX streams before starts forwarding.
 */
uint8_t no_flush_rx = 0; /* flush by default */

/*
 * Flow API isolated mode.
 */
uint8_t flow_isolate_all;

/*
 * Avoids to check link status when starting/stopping a port.
 */
uint8_t no_link_check = 0; /* check by default */

/*
 * Don't automatically start all ports in interactive mode.
 */
uint8_t no_device_start = 0;

/*
 * Enable link status change notification
 */
uint8_t lsc_interrupt = 1; /* enabled by default */

/*
 * Enable device removal notification.
 */
uint8_t rmv_interrupt = 1; /* enabled by default */

uint8_t hot_plug = 0; /**< hotplug disabled by default. */

/* After attach, port setup is called on event or by iterator */
bool setup_on_probe_event = true;

/* Clear ptypes on port initialization. */
uint8_t clear_ptypes = true;

/*
 * Display or mask ether events
 * Default to all events except VF_MBOX
 */
uint32_t event_print_mask = (UINT32_C(1) << RTE_ETH_EVENT_UNKNOWN) |
			    (UINT32_C(1) << RTE_ETH_EVENT_INTR_LSC) |
			    (UINT32_C(1) << RTE_ETH_EVENT_QUEUE_STATE) |
			    (UINT32_C(1) << RTE_ETH_EVENT_INTR_RESET) |
			    (UINT32_C(1) << RTE_ETH_EVENT_IPSEC) |
			    (UINT32_C(1) << RTE_ETH_EVENT_MACSEC) |
			    (UINT32_C(1) << RTE_ETH_EVENT_INTR_RMV);
/*
 * Decide if all memory are locked for performance.
 */
int do_mlockall = 0;

/*
 * NIC bypass mode configuration options.
 */

#if defined RTE_LIBRTE_IXGBE_PMD && defined RTE_LIBRTE_IXGBE_BYPASS
/* The NIC bypass watchdog timeout. */
uint32_t bypass_timeout = RTE_PMD_IXGBE_BYPASS_TMT_OFF;
#endif


#ifdef RTE_LIBRTE_LATENCY_STATS

/*
 * Set when latency stats is enabled in the commandline
 */
uint8_t latencystats_enabled;

/*
 * Lcore ID to serive latency statistics.
 */
lcoreid_t latencystats_lcore_id = -1;

#endif

/*
 * Ethernet device configuration.
 */
struct rte_eth_rxmode rx_mode = {
	.max_rx_pkt_len = RTE_ETHER_MAX_LEN,
		/**< Default maximum frame length. */
};

struct rte_eth_txmode tx_mode = {
	.offloads = DEV_TX_OFFLOAD_MBUF_FAST_FREE,
};

struct rte_fdir_conf fdir_conf = {
	.mode = RTE_FDIR_MODE_NONE,
	.pballoc = RTE_FDIR_PBALLOC_64K,
	.status = RTE_FDIR_REPORT_STATUS,
	.mask = {
		.vlan_tci_mask = 0xFFEF,
		.ipv4_mask     = {
			.src_ip = 0xFFFFFFFF,
			.dst_ip = 0xFFFFFFFF,
		},
		.ipv6_mask     = {
			.src_ip = {0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF},
			.dst_ip = {0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF},
		},
		.src_port_mask = 0xFFFF,
		.dst_port_mask = 0xFFFF,
		.mac_addr_byte_mask = 0xFF,
		.tunnel_type_mask = 1,
		.tunnel_id_mask = 0xFFFFFFFF,
	},
	.drop_queue = 127,
};

volatile int test_done = 1; /* stop packet forwarding when set to 1. */

struct queue_stats_mappings tx_queue_stats_mappings_array[MAX_TX_QUEUE_STATS_MAPPINGS];
struct queue_stats_mappings rx_queue_stats_mappings_array[MAX_RX_QUEUE_STATS_MAPPINGS];

struct queue_stats_mappings *tx_queue_stats_mappings = tx_queue_stats_mappings_array;
struct queue_stats_mappings *rx_queue_stats_mappings = rx_queue_stats_mappings_array;

uint16_t nb_tx_queue_stats_mappings = 0;
uint16_t nb_rx_queue_stats_mappings = 0;

/*
 * Display zero values by default for xstats
 */
uint8_t xstats_hide_zero;

unsigned int num_sockets = 0;
unsigned int socket_ids[RTE_MAX_NUMA_NODES];

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
#ifdef RTE_LIBRTE_LATENCY_STATS
		if (latencystats_enabled != 0)
			rte_latencystats_uninit();
#endif
		force_quit();
		/* Set flag to indicate the force termination. */
		f_quit = 1;
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

	testpmd_logtype = rte_log_register("testpmd");
	if (testpmd_logtype < 0)
		rte_exit(EXIT_FAILURE, "Cannot register log type");
	rte_log_set_level(testpmd_logtype, RTE_LOG_DEBUG);

	diag = rte_eal_init(argc, argv);
	if (diag < 0)
		rte_exit(EXIT_FAILURE, "Cannot init EAL: %s\n",
			 rte_strerror(rte_errno));
	return 0;
}
