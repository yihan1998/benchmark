#!/bin/bash

echo " ***** Test script for YCSB server *****"

echo -n "Number of CPU cores[1 - 4, 1 by default]: "
read num_cores

default_cores=1
num_cores="${num_cores:-$default_cores}"

test_time=60
port=80

make clean && make

rm throughput_*.txt

db_names=(
    # "tbb_rand"
    "memcached"
)

key_length=32
value_length=32

cetus_path=/home/yihan-18/nus-sys/cetus

runtime_lib_path=$cetus_path/Cetus
thread_lib_path=$cetus_path/mthread

hoard_lib_path=/home/yihan-18/Hoard/src

lib_path=$runtime_lib_path:$thread_lib_path:$hoard_lib_path

for db_name in ${db_names[@]}; do
    
    echo "Running $db_name with $num_cores cores"

    for j in $(seq 0 11); do
        total_conn=`echo "2^$j" | bc `

        echo "Testing $total_conn connections on $num_cores core(s) ..."

        LD_LIBRARY_PATH=$lib_path ./YCSB-server --num_cores=$num_cores \
                                                --test_time=$test_time \
                                                --config_path=$cetus_path/test/config \
                                                --db=$db_name \
                                                --port=$port \
                                                --key_length=$key_length \
                                                --value_length=$value_length

        wait

        echo "Test done"

        sleep 5
    done
done