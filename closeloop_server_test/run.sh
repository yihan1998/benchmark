#!/bin/bash
port=81

echo -n "Buffer size(B): "
read buff_size
#buff_size=1024

echo -n "Number of CPU cores: "
read num_cores
#num_cores=1

echo -n "Total test time(s): "
read test_time

make clean && make

rm throughput_*.txt

dir="/proc/irq/"
file_list=`ls $dir`

new_cpu_mask=0

for i in $(seq 1 $num_cores); do
    cpu_id=$(( 1<<i ))
    new_cpu_mask=$(( new_cpu_mask|cpu_id ))
done

printf "Setting new cpu mask to %x\n" $new_cpu_mask

old_cpu_mask=fffff

for file in $file_list
do 
    file_path=$dir$file
    if [[ $file == "default_smp_affinity" ]] ; then
        # printf "Setting new cpu mask to %s\n" $file_path
        echo $new_cpu_mask > $file_path
    fi
    if [[ -d $file_path ]] ; then
        file_name="smp_affinity"
        file_path="${file_path}/${file_name}"
        # printf "Setting new cpu mask to %s\n" $file_path
        echo $new_cpu_mask > $file_path
    fi
done

for j in $(seq 0 13)
do
    num_connection=`echo "2^$j" | bc `

    echo "Testing RTT for $num_connection connections..."

    ifconfig enp1s0f0 10.0.1.1 netmask 255.255.255.0

    ./server    --size=$buff_size \
                --time=$test_time \
                --port=$port \
                --num_cores=$num_cores

    wait

    echo "Test done"
done

for file in $file_list
do 
    file_path=$dir$file
    if [[ $file == "default_smp_affinity" ]] ; then
        # printf "Restoring cpu mask to %s\n" $file_path
        echo $old_cpu_mask > $file_path
    fi
    if [[ -d $file_path ]] ; then
        file_name="smp_affinity"
        file_path="${file_path}/${file_name}"
        # printf "Restoring cpu mask to %s\n" $file_path
        echo $old_cpu_mask > $file_path
    fi
done