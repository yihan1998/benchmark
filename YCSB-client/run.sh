#!/bin/bash

# echo -n "Number of CPU cores: "
# read num_cores
#num_cores=1

echo " ***** Test script for YCSB client *****"

test_time=60
port=80
max_cores=4

make clean && make

rm throughput_*.txt

cetus_path=/home/yihan/nus-sys/cetus

runtime_lib_path=$cetus_path/Cetus
thread_lib_path=$cetus_path/mthread

hoard_lib_path=/home/yihan/Hoard/src

lib_path=$runtime_lib_path:$thread_lib_path:$hoard_lib_path

for j in $(seq 0 11); do
    total_conn=`echo "2^$j" | bc `

    if [ $total_conn -gt $max_cores ]
    then
        num_cores=$max_cores
    else
        num_cores=$total_conn
    fi

    num_flow=`expr $total_conn / $num_cores`

    echo "Testing $total_conn connections on $num_cores core(s) under workload A ..."

    LD_LIBRARY_PATH=$lib_path ./YCSB-client --num_cores=$num_cores \
                                            --test_time=$test_time \
                                            --config_path=$cetus_path/test/config \
                                            --flows=$num_flow \
                                            --port=$port \
                                            --workload=workloads/workloada.spec

    wait

    echo "Test done"

    sleep 2
done
