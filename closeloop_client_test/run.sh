#!/bin/bash

server_ip='10.0.0.1'
server_port=81

echo -n "Buffer size(B): "
read buff_size

max_cores=8

echo -n "Total test time(s): "
read test_time

#echo -n "number of CPU cores: "
#read num_core

echo -n "Number of CPU cores on server side: "
read num_server_core

echo -n "Record Round Trip Time[yes/no]?: "
read record_rtt

if [[ "$record_rtt" == *"yes"* ]];then
    echo " >> evaluting Round Trip Time"
    eval_rtt=1
else
    eval_rtt=0
fi

rm rtt_*.txt

make clean && make RTT=$eval_rtt

for j in $(seq 0 13)
do
    total_conn=`echo "2^$j" | bc `

    if [ $total_conn -gt $max_cores ]
    then
        num_cores=$max_cores
    else
        num_cores=$total_conn
    fi

    num_flow=`expr $total_conn / $num_cores`

    echo "Testing RTT for $total_conn connections on $num_cores core(s), each have $num_flow connection(s) ..."
    
    # ifconfig enp1s0f0 10.0.1.2 netmask 255.255.255.0

    ./client    --server_ip=$server_ip \
                --server_port=$server_port \
                --num_server_core=$num_server_core \
                --size=$buff_size \
                --time=$test_time \
                --num_flow=$num_flow \
                --num_cores=$num_cores 

    wait

    echo "Test done"

    sleep 5

    if [ $eval_rtt -eq 1 ]
    then
        total=`expr $num_cores \* $num_flow`
        python merge_file.py $total
    fi
done