#!/bin/bash
echo " -------------------------------------------------- "
echo -e " \t Test script for closeloop client "
echo " -------------------------------------------------- "

# echo -n "Total test time(s): "
# read test_time
default_time=60
test_time="${test_time:-$default_time}"

echo -n "Buffer size(B) [Enter something (in decimal) less than 1K, 1024 by default]: "
read buff_size
default_size=1024
buff_size="${buff_size:-$default_size}"

echo -n "Number of cores on server side [1 by default]: "
read num_server_cores
default_num_server_cores=1
num_server_cores="${num_server_cores:-$default_num_server_cores}"

max_core=8

make clean && make

rm throughput_*.txt

cygnus_path=/home/yihan/cygnus

runtime_lib_path=$cygnus_path/Cygnus
thread_lib_path=$cygnus_path/mthread

hoard_lib_path=/home/yihan/Hoard/src

lib_path=$runtime_lib_path:$thread_lib_path:$hoard_lib_path

for j in $(seq 0 14)
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

    insmod $cygnus_path/Sail/sail.ko 

    sleep 1

    LD_LIBRARY_PATH=$lib_path $cygnus_path/Lyra/lyra &

    sleep 2

    LD_LIBRARY_PATH=$lib_path ./closeloop_client_test   --num_cores=$num_cores \
                                                        --num_flows=$num_flows \
                                                        --test_time=$test_time \
                                                        --config_path=$cygnus_path/test/config \
                                                        --buff_size=$buff_size \
                                                        --time=$test_time \
                                                        --num_server_cores=$num_server_cores &
    pid=$!

    wait $pid
    
    echo "Test done"

    pkill -9 lyra

    wait

    rmmod sail

    sleep 3

    wait

done