#!/bin/bash

trap 'pkill -9 tas; exit' TERM

tas_dir=/home/yihan-18/tas
socket_dir=/home/yihan-18/socket_test

start_port=81

echo -n "Buffer size(s): "
read buff_size
#buff_size=1024

echo -n "Number of CPU cores: "
read num_cores
#num_cores=1

echo -n "Number of server fast path cores: "
read num_server_fp

echo -n "Total test time(s): "
read test_time
#test_time=60

#echo -n "number of connections: "
#read num_connection

#echo -n "core to bind: "
#read core_id

rm throughput_*.txt

make clean && make

for j in $(seq 0 13)
do
    
    num_connection=`echo "2^$j" | bc `

    echo "Testing RTT for $num_connection connections..."

    $tas_dir/tas/tas --ip-addr=10.0.0.1/24 --fp-cores-max=$num_server_fp &

    sleep 15

    LD_PRELOAD=$tas_dir/lib/libtas_interpose.so $socket_dir/server \
                    --size=$buff_size \
                    --time=$test_time \
                    --core_id=$i \
                    --num_server_fp=$num_server_fp 

    echo "Test done"

    pkill -9 tas

    wait

    sleep 2
done
