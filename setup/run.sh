#!/bin/bash

# Define constants
PROGRAM_PATH="sudo python3 setup.py"
IDENTITY_FILE="--identity_file /users/Khombal2/.ssh/id_rsa"
USERNAME="--username Khombal2"
GIT_SSH_KEY_PATH="--git_ssh_key_path /users/Khombal2/.ssh/id_rsa"
NUM_SERVERS="--num_servers 3"
# POLICY_TYPES=("thread_safe_lru" "nchance" "access_rate")
# CACHE_SIZE=(0.10 0.15 0.20 0.25 0.30 0.334 0.35)
CACHE_SIZE=(0.10 0.15 0.20 0.25 0.30 0.334)
# CACHE_SIZE=(0.334 0.35)
# CACHE_SIZE=(0.10 0.15)
# SYSTEM_NAMES=("A" "B")

# CACHE_SIZE=(0.25 0.334)
# CACHE_SIZE=(0.10)
# SYSTEM_NAMES=("A" "B" "C")
SYSTEM_NAMES=("C")
POLICY_TYPES=("access_rate")
ACCESS_RATE=(100 200 250 400 500 750 1000 1500 2000 2500 3000 3500 4000)
# A : Cache Unaware Routing + Admit + Disk
# B : Cache Unaware Routing + no-Admit + Disk
# C : Cache Unaware Routing + no-Admit + RDMA = old(LDC)
# D : Cache Aware Routing + no-Admit = Perfect system

# SYS_NAME="A"

# Function to execute the command
execute_cmd_with_timeout() {
    local num_clients=$1
    local num_threads=$2
    local num_clients_per_thread=$3
    local step=$4
    local distribution=$5
    local system_name=$6
    local policy=$7
    local cache_size_for_this_run=$8
    local access_rate=$9

    echo "Executing for cache size: $cache_size_for_this_run and policy: $policy and system: $system_name and access rate: $access_rate and workload: $WORKLOAD"
    CMD="$PROGRAM_PATH $IDENTITY_FILE $USERNAME $GIT_SSH_KEY_PATH --step $step $NUM_SERVERS \
    --num_threads $num_threads --policy $policy --system_type $system_name --distribution $distribution --num_clients $num_clients \
    --num_clients_per_thread $num_clients_per_thread --git_branch nchance-addition --cache_size $cache_size_for_this_run --access_rate $access_rate --workload $WORKLOAD"
    echo "Executing: $CMD"

    timeout 15m bash -c "eval $CMD" || (
        echo "Command timed out after 15 minutes. Restarting..."
        execute_cmd_with_timeout $num_clients $num_threads $num_clients_per_thread $step $distribution $system_name $policy $cache_size_for_this_run $access_rate $WORKLOAD
    )
}

execute_cmd() {
    local num_clients=$1
    local num_threads=$2
    local num_clients_per_thread=$3
    local step=$4
    local distribution=$5
    local system_name=$6
    local policy=$7
    local access_rate=$8

    #itr throu
    for cache_size_for_this_run in "${CACHE_SIZE[@]}"; do
        execute_cmd_with_timeout $num_clients $num_threads $num_clients_per_thread $step $distribution $system_name $policy $cache_size_for_this_run $access_rate
    done
}

# Function to iterate through ranges and execute commands
iterate_and_execute() {
    local num_clients_start=$1
    local num_clients_end=$2
    local num_threads_start=$3
    local num_threads_end=$4
    local num_clients_per_thread_start=$5
    local num_clients_per_thread_end=$6
    local distribution=$7
    local system_name=$8
    local policy=$9
    local access_rate=${10}
    
    for ((num_clients=num_clients_start; num_clients<=num_clients_end; num_clients++)); do
        for ((num_threads=num_threads_start; num_threads<=num_threads_end; num_threads++)); do
            for ((num_clients_per_thread=num_clients_per_thread_start; num_clients_per_thread<=num_clients_per_thread_end; num_clients_per_thread++)); do
                execute_cmd $num_clients $num_threads $num_clients_per_thread 12 $distribution $system_name $policy $access_rate
            done
        done
    done
}

CACHE_SIZE=(0.10 0.15 0.20 0.25 0.30 0.334)
SYSTEM_NAMES=("A" "B" "C")
POLICY_TYPES=("thread_safe_lru" "nchance" "access_rate")
ACCESS_RATE=(100 250 500 750 1000 1500 2000 2500 3000 4000)

WORKLOAD="SINGLE_NODE_HOT_KEYS"
# Execute with specified ranges and steps
for system_name in "${SYSTEM_NAMES[@]}"; do
    if [[ $system_name == "C" ]]; then
        for policy in "${POLICY_TYPES[@]}"; do
            if [[ $policy == "access_rate" ]]; then
                for access_rate in "${ACCESS_RATE[@]}"; do
                    echo "Access Rate: $access_rate"
                    iterate_and_execute 3 3 8 8 2 2 "hotspot" $system_name $policy $access_rate
                done
            else
                access_rate=4000
                iterate_and_execute 3 3 8 8 2 2 "hotspot" $system_name $policy $access_rate
            fi
        done
    else
        iterate_and_execute 3 3 8 8 2 2 "hotspot" $system_name "thread_safe_lru" 0
    fi
done

CACHE_SIZE=(0.10 0.15 0.20 0.25 0.30 0.334)
SYSTEM_NAMES=("C")
POLICY_TYPES=("access_rate")
ACCESS_RATE=(100 250 500 750 1000 1500 2000 2500 3000 4000)

WORKLOAD="YCSB"
# Execute with specified ranges and steps
for system_name in "${SYSTEM_NAMES[@]}"; do
    if [[ $system_name == "C" ]]; then
        for policy in "${POLICY_TYPES[@]}"; do
            if [[ $policy == "access_rate" ]]; then
                for access_rate in "${ACCESS_RATE[@]}"; do
                    iterate_and_execute 3 3 8 8 2 2 "hotspot" $system_name $policy $access_rate
                done
            else
                access_rate=4000
                iterate_and_execute 3 3 8 8 2 2 "hotspot" $system_name $policy $access_rate
            fi
        done
    else
        iterate_and_execute 3 3 8 8 2 2 "hotspot" $system_name "thread_safe_lru" 0
    fi
done