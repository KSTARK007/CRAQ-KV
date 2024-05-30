sudo chown -R Khombal2 /mydata
mkdir -p /mydata/
mkdir -p /mydata/ycsb_traces
mkdir -p /mydata/ycsb
cd /mnt/sda4/LDC/third_party/YCSB

function load_ycsb_workload {
    # Load YCSB workload
    cd /mnt/sda4/LDC/third_party/YCSB
    python gen-trace.py load c -t 48 -d hotspot
    mv /mnt/sda4/LDC/third_party/YCSB/client_traces/load/ /mydata/ycsb_traces
}

function uniform_ycsb_workload {
    # Create YCSB workload
    cd /mnt/sda4/LDC/third_party/YCSB
    CMD="python gen-trace.py run c -t 48 -d uniform"
    echo $CMD
    eval $CMD
    DEST_DIR=/mydata/ycsb_traces/uniform
    mkdir -p $DEST_DIR
    mv /mnt/sda4/LDC/third_party/YCSB/client_traces/runc/* $DEST_DIR
    python convert_keys_to_sequential.py -t 8 -c 3 -ct 2 -rtype uniform
    mv /mydata/traces/uniform /mydata/ycsb/
    cd /mnt/sda4/LDC/setup
    python check_freq.py -d /mydata/ycsb/uniform
}

function hotspot_ycsb_workload {
    # Create YCSB workload
    cd /mnt/sda4/LDC/third_party/YCSB
    CMD="python gen-trace.py run c -t 48 -d hotspot --hotspot_data_fraction $2 --hotspot_access_fraction $1"
    echo $CMD
    eval $CMD
    hotspot_access_fraction=$(printf "%.0f" "$(echo "$1*100" | bc)")
    hotspot_data_fraction=$(printf "%.0f" "$(echo "$2*100" | bc)")
    DEST_DIR=/mydata/ycsb_traces/hotspot_${hotspot_access_fraction}_${hotspot_data_fraction}
    mkdir -p $DEST_DIR
    mv /mnt/sda4/LDC/third_party/YCSB/client_traces/runc/* $DEST_DIR
    python convert_keys_to_sequential.py -t 8 -c 3 -ct 2 -rtype hotspot_${hotspot_access_fraction}_${hotspot_data_fraction}
    mv /mydata/traces/hotspot_${hotspot_access_fraction}_${hotspot_data_fraction} /mydata/ycsb/
    cd /mnt/sda4/LDC/setup
    python check_freq.py -d /mydata/ycsb/hotspot_${hotspot_access_fraction}_${hotspot_data_fraction}
    # scp_ycsb_workload /mydata/ycsb/hotspot_${hotspot_access_fraction}_${hotspot_data_fraction}
}

function zipfian_ycsb_workload {
    # Create YCSB workload
    cd /mnt/sda4/LDC/third_party/YCSB
    CMD="python gen-trace.py run c -t 48 -d zipfian --zipfian_constant $1"
    echo $CMD
    eval $CMD
    DEST_DIR=/mydata/ycsb_traces/zipfian_$1
    mkdir -p $DEST_DIR
    mv /mnt/sda4/LDC/third_party/YCSB/client_traces/runc/* $DEST_DIR
    python convert_keys_to_sequential.py -t 8 -c 3 -ct 2 -rtype zipfian_$1
    mv /mydata/traces/zipfian_$1 /mydata/ycsb/
    cd /mnt/sda4/LDC/setup
    python check_freq.py -d /mydata/ycsb/zipfian_$1
}

function create_ycsb_workload {
    # Create YCSB workload
    if [ "$1" == "load" ]; then
        load_ycsb_workload
    elif [ "$1" == "uniform" ]; then
        uniform_ycsb_workload
    elif [ "$1" == "hotspot" ]; then
        hotspot_ycsb_workload $2 $3
    elif [ "$1" == "zipfian" ]; then
        zipfian_ycsb_workload $2
    fi
}

function scp_ycsb_workload {
    for i in {1..6}
    do
        scp -r $1/* 10.10.1.$i:/mnt/sda4/LDC/build
    done
}

load_ycsb_workload

create_ycsb_workload "uniform"
create_ycsb_workload "hotspot" 0.9 0.1
create_ycsb_workload "hotspot" 0.8 0.2
create_ycsb_workload "hotspot" 0.95 0.05
create_ycsb_workload "zipfian" 0.99