trap 'kill $(jobs -p)' SIGINT

for file_name in ./workloads/workload*.spec; do
    for ((tn=1; tn<=8; tn=tn*2)); do
        echo "Running with $tn threads for $file_name"
        ./client --port=6379 --flows=1 --num_cores=$tn --workload=$file_name 2>>ycsbc.output &
        wait
    done
done