#!/bin/bash
trap 'exit' TERM

echo -n "Buffer size(B): "
read buff_size
#buff_size=1024

echo -n "number of CPU cores: "
read num_cores
#num_cores=1

echo -n "Total test time(s): "
read test_time

echo -n "benchmark type[open/close loop]?: "
read test_mode

#echo -n "number of connections: "
#read num_connection

make clean && make server 

rm throughput_*.txt

for j in $(seq 0 15)
do
    num_connection=`echo "2^$j" | bc `

    echo "Testing RTT for $num_connection connections..."

    ./server    --num_cores=$num_cores \
                --size=$buff_size \
                --time=$test_time \
                --test_mode=$test_mode 

    wait

    echo "Test done"
done