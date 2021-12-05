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

max_core=16

make clean && make

rm throughput_*.txt

cygnus_path=/home/yihan/cygnus

runtime_lib_path=$cygnus_path/Cygnus
thread_lib_path=$cygnus_path/mthread

hoard_lib_path=/home/yihan/Hoard/src

lib_path=$runtime_lib_path:$thread_lib_path:$hoard_lib_path

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

    LD_LIBRARY_PATH=$lib_path ./$cygnus_path/Lyra/lyra

    sleep 1

    LD_LIBRARY_PATH=$lib_path ./closeloop_client_test   --num_cores=$num_cores \
                                                        --num_flows=$num_flows \
                                                        --test_time=$test_time \
                                                        --config_path=$cetus_path/test/config \
                                                        --buff_size=$buff_size
                                                        #--port_range=$port_range

    wait
    
    echo "Test done"

    pkill -9 lyra

    wait

    sleep 3

    wait

done