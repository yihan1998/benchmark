#/bin/bash

# repeat_num=3
# db_names=(
#   "lock_stl"
#   "tbb_rand"
#   "tbb_scan"
# )

# trap 'kill $(jobs -p)' SIGINT

# if [ $# -ne 1 ]; then
#   echo "Usage: $0 [dir of workload specs]"
#   exit 1
# fi

# workload_dir=$1

# for file_name in $workload_dir/workload*.spec; do
#   for ((tn=1; tn<=8; tn=tn*2)); do
#     for db_name in ${db_names[@]}; do
#       for ((i=1; i<=repeat_num; ++i)); do
#         echo "Running $db_name with $tn threads for $file_name"
#         ./ycsbc -db $db_name -threads $tn -P $file_name 2>>ycsbc.output &
#         wait
#       done
#     done
#   done
# done

server_ip='10.0.1.2'
start_port=81

# echo -n "Buffer size(B): "
# read buff_size
#buff_size=1024

# echo -n "Number of CPU cores: "
# read num_cores
#num_cores=1
max_cores=4

# echo -n "Total test time(s): "
# read test_time

echo -n "Number of CPU cores on server side: "
read num_server_core

make clean && make

rm throughput_*.txt

db_names=(
    # "tbb_rand"
    "memcached"
)

old_cpu_mask=fffff
new_cpu_mask=1e

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

for db_name in ${db_names[@]}; do
    for j in $(seq 0 12); do
        ifconfig enp1s0f0 10.0.1.2 netmask 255.255.255.0
        total_conn=`echo "2^$j" | bc `

        if [ $total_conn -gt $max_cores ]
        then
            num_cores=$max_cores
        else
            num_cores=$total_conn
        fi

        num_flow=`expr $total_conn / $num_cores`

        echo "Testing RTT for $total_conn connections on $num_cores core(s), each have $num_flow connection(s) ..."

        for i in $(seq 1 $num_cores); do

            offset=`echo "$i % $num_server_core" | bc`
            port=`expr $start_port + $offset`

            ./client    --db=$db_name \
                        --port=$port \
                        --core_id=$i \
                        --flows=$num_flow \
                        --workload=workloads/workloada.spec &

        done

        wait

        echo "Test done"
        
        sleep 1
    done
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