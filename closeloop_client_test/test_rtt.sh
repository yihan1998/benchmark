#!/bin/bash

echo -n "Total test time(s): "
read test_time

#echo -n "Port range: "
#read port_range

max_core=4

make clean && make EVAL_RTT=1

rm throughput_*.txt rtt.txt

lib_path=/home/yihan/nus-sys/cetus

for j in $(seq 0 15)
do
    total_conn=`echo "2^$j" | bc `

    if [ $total_conn -gt $max_core ]
    then
        num_cores=$max_core
    else
        num_cores=$total_conn
    fi

    num_flows=`expr $total_conn / $num_cores`

    echo "Testing RTT for $total_conn connections on $num_cores core(s), each have $num_flows connection(s) ..."

    LD_LIBRARY_PATH=$lib_path ./closeloop_client_test   --num_cores=$num_cores --num_flows=$num_flows \
                                                        --test_time=$test_time --config_path=$lib_path/test/config  #\
                                                        #--port_range=$port_range

    wait
    
    echo "Test done"

    sleep 3

    python merge_file.py $total_conn &

    wait

done