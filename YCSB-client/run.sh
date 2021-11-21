#/bin/bash

# repeat_num=3
# db_names=(
#   "lock_stl"
#   "tbb_rand"
#   "tbb_scan"
# )

# trap 'kill $(jobs -p)' SIGINT

# if [ $# -ne 1 ]; then
#   echo "Usage: $0 [dir of workload specs]"
#   exit 1
# fi

# workload_dir=$1

# for file_name in $workload_dir/workload*.spec; do
#   for ((tn=1; tn<=8; tn=tn*2)); do
#     for db_name in ${db_names[@]}; do
#       for ((i=1; i<=repeat_num; ++i)); do
#         echo "Running $db_name with $tn threads for $file_name"
#         ./ycsbc -db $db_name -threads $tn -P $file_name 2>>ycsbc.output &
#         wait
#       done
#     done
#   done
# done

server_ip='10.0.0.1'
start_port=81

# echo -n "Buffer size(B): "
# read buff_size
#buff_size=1024

# echo -n "Number of CPU cores: "
# read num_cores
#num_cores=1
max_cores=4

# echo -n "Total test time(s): "
# read test_time

# echo -n "Number of CPU cores on server side: "
# read num_server_core

make clean && make

rm throughput_*.txt

db_names=(
    "memcached"
)

for db_name in ${db_names[@]}; do
    for j in $(seq 0 11); do
        ifconfig dpdk0 10.0.0.2 netmask 255.255.255.0
        total_conn=`echo "2^$j" | bc `

        if [ $total_conn -gt $max_cores ]
        then
            num_cores=$max_cores
        else
            num_cores=$total_conn
        fi

        num_flow=`expr $total_conn / $num_cores`

        echo "Testing RTT for $total_conn connections on $num_cores core(s), each have $num_flow connection(s) ..."

        ./client    --port=$port \
                    --num_cores=$num_cores \
                    --flows=$num_flow \
                    --workload=workloads/workloada.spec

        wait

        echo "Test done"
        
        sleep 1
    done
done