#!/bin/bash
trap 'exit' TERM

server_ip='10.0.0.1'
server_port=80

echo -n "Buffer size(B): "
read buff_size
#buff_size=1024

#echo -n "number of CPU cores: "
#read num_core
max_cores=4

echo -n "Total test time(s): "
read test_time

echo -n "Benchmark type[open/close loop]?: "
read test_mode

echo -n "Record Round Trip Time[yes/no]?: "
read record_rtt

if [[ "$record_rtt" == *"yes"* ]];then
    echo " >> evaluting Round Trip Time"
    eval_rtt=1
else
    eval_rtt=0
fi

rm rtt_*.txt

make clean && make client RTT=$eval_rtt

#echo -n "number of connections: "
#read num_connection

for j in $(seq 0 10)
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

    ./client    --num_cores=$num_cores \
                --num_flow=$num_flow \
                --size=$buff_size \
                --time=$test_time \
                --server_ip=$server_ip \
                --server_port=$server_port \
                --test_mode=$test_mode
    
    wait

    echo "Test done"

    sleep 30

    if [ $eval_rtt -eq 1 ]
    then
        total=`expr $num_cores \* $num_flow`
        python merge_file.py $total
    fi
done