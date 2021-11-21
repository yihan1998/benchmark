#!/bin/bash
start_port=81

# echo -n "Buffer size(B): "
# read buff_size
#buff_size=1024

echo -n "Number of CPU cores: "
read num_cores
#num_cores=1

# echo -n "Total test time(s): "
# read test_time

make clean && make

db_names=(
    # "tbb_rand"
    "memcached"
)

key_length=32
value_length=32

for db_name in ${db_names[@]}; do
    
    echo "Running $db_name with $num_cores cores"

    for j in $(seq 0 11); do
        ifconfig dpdk0 10.0.0.1 netmask 255.255.255.0
        total_conn=`echo "2^$j" | bc `

        echo "Testing $total_conn connections on $num_cores core(s) ..."

        ./server    --db=$db_name --time=60 --num_cores=$num_cores --key_length=$key_length --value_length=$value_length

        wait

        echo "Test done"
    done
done

# workload_dir=$1

# for ((tn=1; tn<=8; tn=tn*2)); do
#     for db_name in ${db_names[@]}; do
#         for ((i=1; i<=repeat_num; ++i)); do
#             echo "Running $db_name with $tn threads for $file_name"
#             ./ycsbc -db $db_name -threads $tn -P $file_name 2>>ycsbc.output &
#             wait
#         done
#     done
# done
