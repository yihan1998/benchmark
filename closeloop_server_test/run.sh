#!/bin/bash
echo " -------------------------------------------------- "
echo -e " \t Test script for closeloop server "
echo " -------------------------------------------------- "

echo -n "Number of CPU cores[1 - 16, 1 by default]: "
read num_cores
default_cores=1
num_cores="${num_cores:-$default_cores}"

# echo -n "Total test time(s): "
# read test_time
default_time=60
test_time="${test_time:-$default_time}"

echo -n "Buffer size(B) [Enter something (in decimal) less than 1K, 1024 by default]: "
read buff_size
default_size=1024
buff_size="${buff_size:-$default_size}"

make clean && make

rm throughput_*.txt

cygnus_path=/home/yihan/cygnus

runtime_lib_path=$cygnus_path/Cygnus
thread_lib_path=$cygnus_path/mthread

hoard_lib_path=/home/yihan/Hoard/src

lib_path=$runtime_lib_path:$thread_lib_path:$hoard_lib_path

for j in $(seq 0 15)
do
    num_connection=`echo "2^$j" | bc `

    echo "Testing RTT for $num_connection connections on $num_cores cores..."

    LD_LIBRARY_PATH=$lib_path $cygnus_path/Lyra/lyra &

    sleep 2
    
    LD_LIBRARY_PATH=$lib_path ./closeloop_server_test   --num_cores=$num_cores \
                                                        --test_time=$test_time \
                                                        --config_path=$cygnus_path/test/config \
                                                        --buff_size=$buff_size

    wait
    
    echo "Test done"

    pkill -9 lyra

    wait

    sleep 2 

    wait

done