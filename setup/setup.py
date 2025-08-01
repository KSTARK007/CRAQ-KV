import datetime
import bs4
import subprocess
from multiprocessing import Pool
#from concurrent.futures import ThreadPoolExecutor as Pool
import argparse

import asyncio, asyncssh, sys
import shutil
from pathlib import Path
import json
from datetime import datetime
import check_freq
import os

parser = argparse.ArgumentParser(description='Run code on cloudlab machines from xml config of cluster')
parser.add_argument('--config_file', type=str, help='XML config containing description for cloudlab machine (Copy paste under Manifest when viewing the experiment)', default='../config/cloudlab_machines.xml')
parser.add_argument('--identity_file', type=str, help='SSH identity file to access cloudlab machine', default='~/cloudlab_nopass')
parser.add_argument('--username', type=str, help='Username', default='makneee')
parser.add_argument('--git_ssh_key_path', type=Path, help='Github SSH key for cloning repos', default=None)
parser.add_argument('--step', type=int, default=0)
parser.add_argument('--dataset_size', type=int, default=10000000)
parser.add_argument('--ops_size', type=int, default=100000000)
parser.add_argument('--cache_size', type=float, default=0.334)
parser.add_argument('--workload', type=str, default='YCSB')
parser.add_argument('--policy', type=str, default='thread_safe_lru')
parser.add_argument('--value_size', type=int, default=100)
parser.add_argument('--num_threads', type=int, default=0)
parser.add_argument('--num_clients', type=int, default=0)
parser.add_argument('--num_clients_per_thread', type=int, default=0)
parser.add_argument('--num_servers', type=int, default=0)
parser.add_argument('--one_sided_rdma_enabled', type=bool, default=True)
parser.add_argument('--git_branch', type=str, default='main')
parser.add_argument('--preload_config', type=str, help='Path to existing configs', default='')
parser.add_argument('--system_type', type=str, help='A, B, C, D', default='T')
parser.add_argument("-d", "--distribution", choices=['uniform', 'zipfian', 'hotspot'], default="zipfian", help="Key distribution type (default: 'uniform')")
parser.add_argument('--rdma_async', type=bool, default=True)
parser.add_argument('--disk_async', type=bool, default=True)
parser.add_argument('--operations_pollute_cache', type=bool, default=True)
parser.add_argument('--access_rate', type=int, default=40000)
parser.add_argument('--access_per_itr', type=int, default=1000000000)
parser.add_argument("--ycsb", type=str, default="a")
parser.add_argument("--write_policy", type=str, default="write_back")
args = parser.parse_args()

UBUNTU_DEPENDENCIES = [
    "libboost-all-dev",
    "libdouble-conversion-dev",
    "libgoogle-glog-dev",
    "build-essential",
    "cmake",
    "gcc",
    "meson",
    "libudev-dev",
    "libnl-3-dev",
    "libnl-route-3-dev",
    "ninja-build",
    "pkg-config",
    "valgrind",
    "python3-dev",
    "cython3",
    "python3-docutils",
    "pandoc",
    "libnuma-dev",
    "ca-certificates",
    "autoconf",
    "libgflags-dev",
    "libgflags2.2",
    "libhugetlbfs-dev",
    "pciutils",
    "libunwind-dev",
    "uuid-dev",
    "nlohmann-json3-dev",
    "npm",
    "libtbb-dev",
    "python3-pip",
    "htop",
    "liburing-dev",
    "python2",
    "maven",
]

PYTHON_PIP_DEPENDENCIES = [
    "numpy",
    "pyelftools",
    "matplotlib",
    "beautifulsoup4",
    "asyncssh",
    "lxml",
    "tqdm"
]

# TODO: Add craq config here
def generate_config(
    num_entries,
    block_size,
    cache_size,
    baseline,
    num_clients=1,
    num_servers=3,
    one_sided_rdma_enabled=False,
    DISK_ASYNC=False,
    use_cache_indexing=True,
    policy_type="thread_safe_lru",
    craq_enabled=False,
    access_rate=4000,
    access_per_itr=100000000,
    shared_log=False,
):
    remote_machines = []
    for i in range(num_clients + num_servers):
        if i < num_clients:
            remote_machines.append(
                {
                    "index": i,
                    "ip": f"10.10.1.{i + 1}",
                    "port": 8000,
                    "server": False,
                }
            )
        else:
            remote_machines.append(
                {
                    "index": i,
                    "ip": f"10.10.1.{i + 1}",
                    "port": 8000,
                    "server": True,
                }
            )
    if shared_log:
        remote_machines.append(
            {
                "index": num_clients + num_servers,
                "ip": f"10.10.1.{num_clients + num_servers + 1}",
                "port": 8000,
                "server": False,
                "shared_log": True,
            }
        )
    # TODO: Add craq config here and make sure we can parse it in c++ code
    config = {
        "ingest_block_index": True,
        "craq_enabled": craq_enabled,
        "policy_type": policy_type,
        "db_type": "block_db",
        "rdma_port": 50000,
        "db": {
            "block_db": {
                "filename": "database.db",
                # TODO: Comment this out if Im generating the db for the first time, then have to manually copy database.db to bak.db in every server node
                # "copied_filename": "bak.db",
                "num_entries": num_entries,
                "block_size": block_size,
                "async": DISK_ASYNC,
                "io_uring_ring_size": 128,
                "io_uring_worker_threads": 1,
                "io_uring_write_ring_size": 128,
                "io_uring_write_worker_threads": 0,
                "batch_write_size": 128,
                "batch_max_pending_requests": 128
                * int(os.environ.get("OUTSTANDING_REQUESTS")),
                # "batch_max_pending_requests": 128 * 1,
                "milliseconds_flush_dirty_cache": 0,
                "async_request_threads": 1,
            }
        },
        "cache": {
            "lru": {"cache_size": cache_size},
            "random": {"cache_size": cache_size},
            "split": {
                "cache_size": cache_size,
                "owning_ratio": 0.75,
                "nonowning_ratio": 0.25,
                "owning_cache_type": "lru",
                "nonowning_cache_type": "lru",
            },
            "thread_safe_lru": {"cache_size": cache_size},
            "paged": True,
            "rdma": {"context_index": 3},
        },
        "baseline": {
            "random": {},
            "client_aware": {},
            "ldc": {},
            "selected": baseline,
            "one_sided_rdma_enabled": one_sided_rdma_enabled,
            "use_cache_indexing": use_cache_indexing,
        },
        "remote_machines": remote_machines,
        "access_rate": access_rate,
        "access_per_itr": access_per_itr,
    }

    config_filename = f"config.json"

    with open(config_filename, "w") as config_file:
        json.dump(config, config_file, indent=4)

    print(f"Configuration saved to {config_filename}")
    return config_filename


def generate_ops_config(
    NUM_KEY_VALUE_PAIRS,
    TOTAL_OPERATIONS,
    VALUE_SIZE,
    DISTRIBUTION_TYPE,
    HOT_THREAD,
    RDMA_THREAD,
    TOTAL_RUNTIME_IN_SECONDS=0.0,
    RDMA_ASYNC=True,
    DISK_ASYNC=False,
    operations_pollute_cache=True,
    use_cache_logs=True,
    dump_snapshot_file="snapshot_",
    enable_fallback_rpc=False,
    fallback_rpc_broadcast=False,
    write_percent=0.0,
    cache_logs_push_entries=True,
    write_policy="write_through",
    writes_linearizable=False,
):
    config = {
        "NUM_KEY_VALUE_PAIRS": NUM_KEY_VALUE_PAIRS,
        "NUM_NODES": 3,
        "KEY_SIZE": 24,
        "VALUE_SIZE": VALUE_SIZE,
        "TOTAL_OPERATIONS": TOTAL_OPERATIONS,
        "OP_FILE": "operations.txt",
        "DATASET_FILE": "dataset.txt",
        "DISTRIBUTION_TYPE": DISTRIBUTION_TYPE,
        "HOT_KEY_PERCENTAGE": 1.0,
        "HOT_KEY_ACCESS_PERCENTAGE": 1.0,
        "HOT_THREAD": HOT_THREAD,
        "RDMA_THREAD": RDMA_THREAD,
        "TOTAL_RUNTIME_IN_SECONDS": TOTAL_RUNTIME_IN_SECONDS,
        "WARMUP_TIME_IN_SECONDS": 0.0,
        "RDMA_ASYNC": RDMA_ASYNC,
        "DISK_ASYNC": DISK_ASYNC,
        "infinity_bound_nic": "mlx5_3",
        "infinity_bound_device_port": 1,
        "operations_pollute_cache": operations_pollute_cache,
        "use_cache_logs": use_cache_logs,
        "cache_log_sync_every_x_operations": 1000,
        "dump_snapshot_period_ms": 0,
        "dump_snapshot_file": dump_snapshot_file,
        "enable_fallback_rpc": enable_fallback_rpc,
        "fallback_rpc_broadcast": fallback_rpc_broadcast,
        "write_percent": write_percent,
        "cache_logs_push_entries": cache_logs_push_entries,
        "write_policy": write_policy,
        "writes_linearizable": writes_linearizable,
    }

    config_filename = f"ops_config.json"

    with open(config_filename, "w") as config_file:
        json.dump(config, config_file, indent=4)

    print(f"Configuration saved to {config_filename}")

    return config_filename

def divide_file(filename, num_files):
    try:
        with open(filename, 'r') as input_file:
            output_files = [open(f'node{i+1}_ops.txt', 'w') for i in range(num_files)]
            
            line_count = sum(1 for line in input_file)
            input_file.seek(0)  # Reset file pointer to beginning
            
            lines_per_file = line_count // num_files
            remainder = line_count % num_files
            
            current_file_index = 0
            lines_written = 0
            
            for line in input_file:
                output_files[current_file_index].write(line)
                lines_written += 1
                
                if lines_written == lines_per_file + (1 if current_file_index < remainder else 0):
                    current_file_index += 1
                    lines_written = 0
            
            for file in output_files:
                file.close()
                print(f"File {file.name} created with approximately {lines_per_file} lines.")
    
    except FileNotFoundError:
        print(f"Error: File {filename} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


config_file = args.config_file
identity_file = args.identity_file
machine_name = args.username
git_ssh_key_path = args.git_ssh_key_path
if git_ssh_key_path is not None:
    git_ssh_key_path = git_ssh_key_path.resolve()

if __name__ == '__main__':
    with open(config_file, 'r') as f:
        xml = f.read()
        try:
            soup = bs4.BeautifulSoup(xml, 'lxml')
        except Exception as e:
            print(e)
            exit(-1)
        
        done_sync = False
        async def run_on_node(i, conn, clients):
            async def run_command(command):
                print(f'[{i}] [{conn._host}] Running {command}...')
                try:
                    result = await conn.run(command, check=True)
                except Exception as e:
                    # print(f'[{i}] [{conn._host}] Failed running {command} with exception: {e}')
                    # exit(-1)
                    raise Exception(f'Failed running {command} with exception: {e}')

                print(f'[{i}] [{conn._host}] Finished running {command}...')
                return result.stdout

            if args.step == 'install_deps' or args.step == 0:
                stdout = await run_command(r'sudo apt update -y')
                await run_command(f'sudo apt-get install {" ".join(UBUNTU_DEPENDENCIES)} -y')
                await run_command(f'sudo pip3 install {" ".join(PYTHON_PIP_DEPENDENCIES)}')

            elif args.step == 'transfer_ssh_key_to_cloud_machines' or args.step == 1:
                base_dir = '/mnt/sda4'
                ldc_dir = f'{base_dir}/LDC'
                third_party_dir = f'{ldc_dir}/third_party'
                machnet_dir = f'{third_party_dir}/machnet'

                ssh_dir = f'~/.ssh'

                # Sync git ssh key
                git_ssh_key_name = git_ssh_key_path.name
                await asyncssh.scp(f'{git_ssh_key_path}', (conn, f'{ssh_dir}/{git_ssh_key_name}'))
                await run_command(f'chmod 600 {ssh_dir}/{git_ssh_key_name}')
                await run_command(f'echo "Host github.com\n  IdentityFile {ssh_dir}/{git_ssh_key_name}" >> {ssh_dir}/config')
                await run_command(f'echo "Host *\nStrictHostKeyChecking no\n" >> {ssh_dir}/config')
            
                # Sync cloudlab ssh key
                await asyncssh.scp(f'{Path(identity_file).expanduser().resolve()}', (conn, f'{identity_file}'))

            elif args.step == 'install_mellanox' or args.step == 2:
                await run_command(f'''
sudo mkfs.ext4 /dev/sda4
sudo mkdir /mnt/sda4
sudo mount /dev/sda4 /mnt/sda4
sudo chown -R {machine_name} /mnt/sda4/
sudo chmod -R g+rw /mnt/sda4

sudo bash -c "echo 2048 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
sudo mkdir /mnt/huge
sudo mount -t hugetlbfs nodev /mnt/huge
''')
                await run_command(f'cd /mnt/sda4 && rm -rf LDC && git clone --recursive git@github.com:dassl-uiuc/LDC.git')
                
                await run_command(f'''
wget http://www.mellanox.com/downloads/ofed/MLNX_OFED_LINUX-5.7-1.0.2.0/MLNX_OFED_LINUX-5.7-1.0.2.0-ubuntu22.04-x86_64.tgz
wget https://content.mellanox.com/ofed/MLNX_OFED-23.10-0.5.5.0/MLNX_OFED_LINUX-23.10-0.5.5.0-ubuntu22.04-x86_64.tgz

tar -xzvf MLNX_OFED_LINUX-23.10-0.5.5.0-ubuntu22.04-x86_64.tgz
cd MLNX_OFED_LINUX-23.10-0.5.5.0-ubuntu22.04-x86_64
#sudo ./mlnxofedinstall --auto-add-kernel-support --without-fw-update
sudo ./mlnxofedinstall --upstream-libs --dpdk --force

sudo /etc/init.d/openibd restart
''')
                print('Finished setting up machine, reboot on your own in cloudlab')
            elif args.step == 'install_rdma_dpdk' or args.step == 3:
                base_dir = '/mnt/sda4'
                await run_command(r'''
sudo apt --purge -y remove rdma-core librdmacm1 ibverbs-providers libibverbs-dev libibverbs1
sudo rm -rf dpdk
sudo rm -rf rdma-core
''')

                await run_command(r'''
# Get rdma-core source
if [ ! -d "rdma-core" ]; then
{
    git clone -b 'stable-v40' --single-branch --depth 1 https://github.com/linux-rdma/rdma-core.git 
    if [[ $? -ne 0 ]]; then
        echo 'Unable to download rdma-core'
        exit 1
    fi
    cd rdma-core
    mkdir build
    cd build
    cmake -GNinja -DNO_PYVERBS=1 -DNO_MAN_PAGES=1 ../
    sudo ninja install
    cd ..
} 1>build.log 2>&1
fi
''')
                await run_command(r'''
sudo apt --purge -y remove rdma-core librdmacm1 ibverbs-providers libibverbs-dev libibverbs1
''')
                      
                await run_command(r'''
# Get dpdk source
if [ ! -d "dpdk" ]; then
{
    git clone --depth 1 --branch 'v21.11' https://github.com/DPDK/dpdk.git
    cd dpdk
    meson build -Dexamples='' -Dplatform=generic -Denable_kmods=false -Dtests=false -Ddisable_drivers='raw/*,crypto/*,baseband/*,dma/*'
    DESTDIR=$PWD/build/install sudo ninja -C build install

    cd ../..
} 1>build.log 2>&1
fi
''')
                await run_command(r'''
cd dpdk
sudo modprobe uio_pci_generic
sudo ./usertools/dpdk-devbind.py --bind=uio_pci_generic 03:00.0
sudo ./usertools/dpdk-devbind.py --bind=uio_pci_generic 07:00.1
''')
            elif args.step == 'build_machnet' or args.step == 4:
                base_dir = '/mnt/sda4'
                ldc_dir = f'{base_dir}/LDC'
                third_party_dir = f'{ldc_dir}/third_party'
                machnet_dir = f'{third_party_dir}/machnet'

                network_device = 'ens1f1np1'

                mac_addr = await run_command(f'ifconfig {network_device} | grep -w ether | tr -s " " | cut -d\' \' -f 3')
                ip_addr = await run_command(f'ifconfig {network_device} | grep -w inet | tr -s " " | cut -d\' \' -f 3')
                mac_addr = mac_addr.strip()
                ip_addr = ip_addr.strip()

                # Overwrite config
                await run_command(f'''
cd {machnet_dir}

sed -i 's/LOCAL_MAC=""/LOCAL_MAC="{mac_addr}"/' machnet.sh
sed -i 's/LOCAL_IP=""/LOCAL_IP="{ip_addr}"/' machnet.sh
sed -i 's/BARE_METAL=0/BARE_METAL=1/' machnet.sh
sed -i 's/1024/8192/' machnet.sh
''')

                # Build machnet
                await run_command(f'''
cd {ldc_dir}
git pull
cd {third_party_dir}
git submodule update --init --recursive
cd machnet
mkdir build
cd build
export RTE_SDK=~/dpdk
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
cd ..
''')
            elif args.step == 'install third_party dependencies' or args.step == 6:
                base_dir = '/mnt/sda4'
                ldc_dir = f'{base_dir}/LDC'
                third_party_dir = f'{ldc_dir}/third_party'
                machnet_dir = f'{third_party_dir}/machnet'

                await run_command(f'''
curl -O https://capnproto.org/capnproto-c++-1.0.2.tar.gz
tar zxf capnproto-c++-1.0.2.tar.gz
cd capnproto-c++-1.0.2
./configure
make -j
sudo make install
''')
            elif args.step == 'install frequency' or args.step == 7:
                base_dir = '/mnt/sda4'
                ldc_dir = f'{base_dir}/LDC'
                third_party_dir = f'{ldc_dir}/third_party'
                machnet_dir = f'{third_party_dir}/machnet'

                await run_command(f'''
sudo apt install cpufrequtils -y
echo \'GOVERNOR="userspace"\' | sudo tee /etc/default/cpufrequtils && sudo systemctl restart cpufrequtils
''')

                await run_command(f'''
sudo apt install linux-tools-common -y 
sudo apt install linux-tools-$(uname -r) -y
sudo cpupower idle-set -E
sudo cpupower frequency-set --governor userspace
sudo cpupower frequency-set -u 4.0GHz
sudo cpupower frequency-set -d 4.0GHz
sudo cpupower frequency-set -f 4.0GHz
''')

            elif args.step == 'bind_stuff_rdma' or args.step == 8:
                await run_command(f'''
cd dpdk
sudo modprobe uio_pci_generic
sudo ./usertools/dpdk-devbind.py --unbind 03:00.0
sudo ./usertools/dpdk-devbind.py --unbind 07:00.1
                                  
sudo ./usertools/dpdk-devbind.py --bind=mlx5_core 03:00.0
sudo ./usertools/dpdk-devbind.py --bind=mlx5_core 07:00.1
                                  
sudo ip link set dev ens1f0np0 up
sudo ip link set dev eno50np1 up

sudo ip addr add 10.10.2.{i}/24 dev ens1f0np0
sudo ip addr add 10.10.3.{i}/24 dev eno50np1
''')
            elif args.step == "compile" or args.step == 11:
                base_dir = "/mnt/sda4"
                ldc_dir = f"{base_dir}/LDC"
                build_dir = f"{base_dir}/LDC/build"
                third_party_dir = f"{ldc_dir}/third_party"
                machnet_dir = f"{third_party_dir}/machnet"
                blkcache_dir = f"{third_party_dir}/blkcache"

                dataset_size = 1000
                true_block_size = 4096
                block_size = true_block_size - 64
                ops_size = dataset_size * 10
                # workload = "RANDOM_DISTRIBUTION"
                workload = "PARTITIONED_DISTRIBUTION"
                value_size = 400
                cache_size = int(dataset_size * 0.34)
                num_threads = 1
                num_servers = 1

                baseline = "random"
                # baseline = "client_aware"
                one_sided_rdma_enabled = True

                if i >= 0:
                    await run_command(
                        f"""
cd {third_party_dir}
cd RDMA
git reset HEAD --hard
cd ..
sed -i "s/INFINITY_ASSERT(returnValue == 0, \\\"\\\\[INFINITY\\\\]\\\\[QUEUES\\\\]\\\\[FACTORY\\\\] Could not connect to server. ret %d\\\\\\n\\\", returnValue);/while (returnValue) {{ connect(connectionSocket, (sockaddr *) \&(remoteAddress), sizeof(sockaddr_in)); }}/g" RDMA/src/infinity/queues/QueuePairFactory.cpp

"""
                    )
                    await run_command(
                        f"""
cd {third_party_dir}
cd YCSB
git stash
git checkout sequential-keys
"""
                    )
                    await run_command(
                        f"""
mkdir -p {build_dir}
cd {build_dir}
pkill ldc || true
rm -f ../config/ops_config.json
git stash
git checkout {args.git_branch}
git submodule update --remote --merge
git pull
cd {blkcache_dir}
git stash
git checkout write_impl
# TODO: git pull was not here before
git pull
cd {build_dir}
export RTE_SDK=~/dpdk
cmake .. -DCMAKE_BUILD_TYPE=Release
make ldc -j
"""
                    )
                    await run_command(
                        f"""
cd {machnet_dir}

sed -i "s/kMaxChannelNr = 32/kMaxChannelNr = 4000/g" src/include/channel.h
sed -i "s/kDefaultRingSize = 256/kDefaultRingSize = 1024/g" src/include/channel.h
sed -i "s/kDefaultBufferCount = 4096/kDefaultBufferCount = 16384/g" src/include/channel.h
sed -i "s/const size_t kSlowTimerIntervalUs = 1000000/const size_t kSlowTimerIntervalUs = 100000/g" src/include/machnet_engine.h
sed -i "s/int max_tries = 10;/int max_tries = 10000;/g" src/ext/machnet.c
sed -i "s/sleep(1);/nanosleep(\&(struct timespec){{.tv_sec = 0, .tv_nsec = 1000 * 1000}}, NULL);/g" src/ext/machnet.c
sed -i "s/machnet_ring_slot_nr, app_ring_slot_nr, buf_ring_slot_nr, buffer_size, 0);/machnet_ring_slot_nr, app_ring_slot_nr, buf_ring_slot_nr, buffer_size, 1);/g" src/ext/machnet_private.h

cd build
make -j
"""
                    )

            elif args.step == "run" or args.step == 12:
                base_dir = "/mnt/sda4"
                ldc_dir = f"{base_dir}/LDC"
                build_dir = f"{base_dir}/LDC/build"
                third_party_dir = f"{ldc_dir}/third_party"
                machnet_dir = f"{third_party_dir}/machnet"
                config_path = f"{ldc_dir}/config/config.json"
                ops_config_path = f"{ldc_dir}/config/ops_config.json"
                cal_latency_path = f"{ldc_dir}/src/cal_latency.py"
                ycsb_dir = f"{third_party_dir}/YCSB"

                dataset_size = 10000
                # dataset_size = 10 * 1000 * 1000
                true_block_size = 4096
                block_size = true_block_size - 64
                ops_size = dataset_size * 10
                # ops_size = dataset_size * 100
                # workload = "RANDOM_DISTRIBUTION"
                # workload = "PARTITIONED_DISTRIBUTION"
                # workload = "SINGLE_NODE_HOT_KEYS"
                # workload = "SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_ONLY"
                # workload = "SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_SPLIT"
                # workload = "SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_RDMA_ONLY"
                workload = "YCSB"
                value_size = 100
                args.cache_size = 0.2
                cache_size = int(dataset_size * args.cache_size)

                # baseline = "random"
                baseline = "client_aware"
                policy_type = args.policy
                # policy_type = "random"
                # TODO: This disables LDC
                # policy_type = "thread_safe_lru"
                # policy_type = "nchance"
                policy_type = "access_rate"
                # policy_type = "access_rate_dynamic"

                # one_sided_rdma_enabled = False
                one_sided_rdma_enabled = True

                operations_pollute_cache = True
                # operations_pollute_cache = False

                # total_runtime_in_seconds = 0.0
                # total_runtime_in_seconds = 15.0
                # total_runtime_in_seconds = 30.0
                total_runtime_in_seconds = 60.0
                # total_runtime_in_seconds = 120.0
                # total_runtime_in_seconds = 240.0
                # total_runtime_in_seconds = 5 * 60.0
                # total_runtime_in_seconds = 15 * 60.0

                # rdma_async = False
                rdma_async = True

                # disk_async = False
                disk_async = True

                use_cache_indexing = True
                # use_cache_indexing = False

                # TONY
                craq_enabled = True

                shared_log = False
                enable_fallback_rpc = False
                fallback_rpc_broadcast = False
                write_percent = 1.0
                cache_logs_push_entries = True
                # write_policy = "write_back"
                # write_policy = "write_around"
                # write_policy = "selective_write_back"
                write_policy = "selective_write_around"
                writes_linearizable = False

                YCSB_RUN = "a"

                YCSB_RUN = args.ycsb
                write_policy = args.write_policy

                if (
                    args.num_threads == 0
                    or args.num_clients == 0
                    or args.num_clients_per_thread == 0
                    or args.num_servers == 0
                ):
                    num_threads = 8
                    num_clients = 1
                    num_clients_per_thread = 2
                    num_servers = 3
                    system_type = "C"
                    distribution = "zipfian"
                else:
                    dataset_size = args.dataset_size
                    ops_size = args.ops_size
                    workload = args.workload
                    num_threads = args.num_threads
                    num_clients = args.num_clients
                    num_clients_per_thread = args.num_clients_per_thread
                    num_servers = args.num_servers
                    value_size = args.value_size
                    one_sided_rdma_enabled = args.one_sided_rdma_enabled
                    system_type = args.system_type
                    distribution = args.distribution
                    rdma_async = args.rdma_async
                    disk_async = args.disk_async
                    operations_pollute_cache = args.operations_pollute_cache
                    YCSB_RUN = args.ycsb
                    write_policy = args.write_policy
                try:
                    j = json.loads(args.preload_config)
                    dataset_size = j["dataset_size"]
                    true_block_size = j["true_block_size"]
                    block_size = j["block_size"]
                    ops_size = j["ops_size"]
                    workload = j["workload"]
                    value_size = j["value_size"]
                    cache_size = j["cache_size"]
                    baseline = j["baseline"]
                    one_sided_rdma_enabled = j["one_sided_rdma_enabled"]
                    num_threads = j["num_threads"]
                    num_clients = j["num_clients"]
                    num_clients_per_thread = j["num_clients_per_thread"]
                    num_servers = j["num_servers"]
                    disk_async = j["disk_async"]
                    operations_pollute_cache = j["operations_pollute_cache"]
                except Exception as e:
                    print(e)

                # num_threads = 1
                # num_clients = 3
                # num_clients_per_thread = 16
                # policy_type = "thread_safe_lru"
                # one_sided_rdma_enabled = False
                # operations_pollute_cache = True

                # policy_type = "thread_safe_lru"
                # one_sided_rdma_enabled = False
                # operations_pollute_cache = False

                # if system_type == "A":
                #     one_sided_rdma_enabled = False
                #     operations_pollute_cache = True
                # elif system_type == "B":
                #     one_sided_rdma_enabled = False
                #     operations_pollute_cache = False
                # elif policy_type == "nchance" or policy_type == "access_rate":
                #     one_sided_rdma_enabled = True
                #     operations_pollute_cache = True
                # else:
                #     one_sided_rdma_enabled = True
                #     operations_pollute_cache = False

                # extra_threads = 0
                # num_clients_per_thread += extra_threads
                # rdma_threads = num_threads * extra_threads * num_clients
                # hot_threads = (
                #     num_threads * num_clients_per_thread * num_clients
                # ) - rdma_threads
                rdma_threads = num_threads * num_clients_per_thread * num_clients
                hot_threads = 0
                # rdma_threads = 0
                # hot_threads = (num_threads * num_clients_per_thread * num_clients)

                try:
                    j = json.loads(args.preload_config)
                    rdma_threads = j["rdma_threads"]
                    hot_threads = j["hot_threads"]
                except Exception as e:
                    print(e)

                # total_clients = num_clients * num_clients_per_thread
                # Generate the dataset.txt and operations.txt and transfer to remote machines
                total_servers = num_clients + num_servers
                total_servers_not_shared = total_servers
                if shared_log:
                    total_servers += 1

                # kill machnet and restart
                if i < total_servers:
                    await run_command(
                        f"""
cd {machnet_dir}
sudo pkill -9 machnet || true
sudo pkill -9 ldc || true
#pm2 del all
#pm2 start 'echo "y" | ./machnet.sh' || true
"""
                    )

                if i == 0:
                    data_path = Path(
                        f"results/{system_type}_{distribution}_{workload}_bench_data_ns{num_servers}_nc{num_clients}_ncpt{num_clients_per_thread}_nt{num_threads}_rdma{one_sided_rdma_enabled}_async{rdma_async}"
                    )
                    run_type = f"run{YCSB_RUN}"
                    if workload == "YCSB":
                        await run_command(
                            f"""
# rm -rf {build_dir}/client_*.txt || true
"""
                        )
                        shutil.rmtree(data_path, ignore_errors=True)
                        data_path.mkdir(exist_ok=True, parents=True)
                        await run_command(
                            f"""
cd {ycsb_dir}
# TODO: Comment this out if I dont change any configs
python3 gen-trace.py load -r {dataset_size}
python3 gen-trace.py run {YCSB_RUN} -d {distribution} -r {dataset_size} -o {ops_size} -t {num_threads * num_clients * num_clients_per_thread}
python3 convert_keys_to_sequential.py -t {num_threads} -c {num_clients} -ct {num_clients_per_thread} -rtype {run_type}
cp -r {ycsb_dir}/traces/{run_type}/* {build_dir}
"""
                        )

                    # for j in range(1, num_clients):
                    #     username, hostname = clients[j]
                    #     await run_command(
                    #         f'rsync -avP -e "ssh -oStrictHostKeyChecking=no -i {args.identity_file}" {ycsb_dir}/traces/{run_type}/* {username}@{hostname}:{ycsb_dir}/traces/{run_type}/'
                    #     )
                    operations_path = data_path / f"ops/"
                    operations_path.parent.mkdir(exist_ok=True, parents=True)
                    print(
                        f"Copying {ycsb_dir}/client_traces_seq/{run_type}/ to {operations_path}"
                    )
                    # await asyncssh.scp(
                    #     (conn, f"{ycsb_dir}/traces/{run_type}/"),
                    #     str(operations_path),
                    #     recurse=True,
                    # )
                    print(
                        f"Finished copying {ycsb_dir}/client_traces_seq/{run_type}/ to {operations_path}"
                    )
                    print(f"Finished generating YCSB workload")
                    print(f"plotting cdf graph")
                    # check_freq.plot_cdf_graph(f'{data_path}/ops')
                    print(f"Finished plotting cdf graph")

                    print(f"generating config")
                    config_file = generate_config(
                        ops_size,
                        true_block_size,
                        cache_size,
                        baseline,
                        num_clients,
                        num_servers,
                        one_sided_rdma_enabled,
                        disk_async,
                        use_cache_indexing,
                        policy_type,
                        # TONY
                        craq_enabled,
                        access_rate=args.access_rate,
                        access_per_itr=args.access_per_itr,
                        shared_log=shared_log,
                    )
                    print(f"generating ops config")
                    ops_config_file = generate_ops_config(
                        dataset_size,
                        ops_size,
                        value_size,
                        workload,
                        hot_threads,
                        rdma_threads,
                        total_runtime_in_seconds,
                        rdma_async,
                        disk_async,
                        operations_pollute_cache,
                        enable_fallback_rpc=enable_fallback_rpc,
                        fallback_rpc_broadcast=fallback_rpc_broadcast,
                        write_percent=write_percent,
                        cache_logs_push_entries=cache_logs_push_entries,
                        write_policy=write_policy,
                        writes_linearizable=writes_linearizable,
                    )
                    await asyncssh.scp(f"{config_file}", (conn, config_path))
                    await asyncssh.scp(f"{ops_config_file}", (conn, ops_config_path))

                    await run_command(
                        f"""
cd {build_dir}
./bin/ldc -config {config_path} -dataset_config {ops_config_path} -machine_index {i} -threads {num_threads} -dump_operations true
"""
                    )

                    for j in range(1, total_servers):
                        username, hostname = clients[j]
                        await run_command(
                            f'rsync -avP -e "ssh -oStrictHostKeyChecking=no -i {args.identity_file}" {ldc_dir}/build/dataset.txt {username}@{hostname}:{ldc_dir}/build/dataset.txt'
                        )
                        await run_command(
                            f'rsync -avP -e "ssh -oStrictHostKeyChecking=no -i {args.identity_file}" {ldc_dir}/build/operations.txt {username}@{hostname}:{ldc_dir}/build/operations.txt'
                        )

                    # divide operations.txt into num_clients
                    await run_command(
                        f"""
cd {build_dir}
rm -f
mv operations.txt operations_full.txt
split -n l/{num_clients} -d operations_full.txt operations_
"""
                    )
                    for j in range(num_clients):
                        username, hostname = clients[j]
                        await run_command(
                            f'rsync -avP -e "ssh -oStrictHostKeyChecking=no -i {args.identity_file}" {ldc_dir}/build/operations_{j:02d} {username}@{hostname}:{ldc_dir}/build/operations.txt'
                        )

                if i < num_clients:
                    run_type = f"run{YCSB_RUN}"
                    await run_command(
                        f"""
cd {ycsb_dir}
# python3 gen-trace.py load -r {dataset_size}
# python3 gen-trace.py run {YCSB_RUN} -d {distribution} -r {dataset_size} -o {ops_size} -t {num_threads * num_clients * num_clients_per_thread}
# python3 convert_keys_to_sequential.py -t {num_threads} -c {num_clients} -ct {num_clients_per_thread} -rtype {run_type}
# cp -r {ycsb_dir}/traces/{run_type}/* {build_dir}
"""
                    )

                # Write to datapath
                data_path = Path(
                    f"results/{policy_type}::{system_type}_{distribution}_{workload}_{cache_size}_ns{num_servers}_nc{num_clients}_ncpt{num_clients_per_thread}_nt{num_threads}_rd{rdma_threads}_ht{hot_threads}_rdma{one_sided_rdma_enabled}_async{rdma_async}_disk{disk_async}_pollute{operations_pollute_cache}_access_rate{args.access_rate}"
                )

                data_path = Path(f"results/{write_policy}_{YCSB_RUN}")
                if i == 0:
                    shutil.rmtree(data_path, ignore_errors=True)
                    data_path.mkdir(exist_ok=True, parents=True)

                    # shutil.rmtree(plot_path, ignore_errors=True)
                    # plot_path.mkdir(exist_ok=True, parents=True)

                # Generate the dataset.txt and operations.txt and transfer to remote machines
                if i < total_servers:
                    config_file = generate_config(
                        ops_size,
                        true_block_size,
                        cache_size,
                        baseline,
                        num_clients,
                        num_servers,
                        one_sided_rdma_enabled,
                        disk_async,
                        use_cache_indexing,
                        policy_type,
                        # TONY
                        craq_enabled,
                        access_rate=args.access_rate,
                        access_per_itr=args.access_per_itr,
                        shared_log=shared_log,
                    )
                    ops_config_file = generate_ops_config(
                        dataset_size,
                        ops_size,
                        value_size,
                        workload,
                        hot_threads,
                        rdma_threads,
                        total_runtime_in_seconds,
                        rdma_async,
                        disk_async,
                        operations_pollute_cache,
                        enable_fallback_rpc=enable_fallback_rpc,
                        fallback_rpc_broadcast=fallback_rpc_broadcast,
                        write_percent=write_percent,
                        cache_logs_push_entries=cache_logs_push_entries,
                        write_policy=write_policy,
                        writes_linearizable=writes_linearizable,
                    )
                    await asyncssh.scp(f"{config_file}", (conn, config_path))
                    await asyncssh.scp(f"{ops_config_file}", (conn, ops_config_path))

                # kill machnet and restart
                if i < total_servers:
                    await run_command(
                        f"""
cd {machnet_dir}
sudo pkill -9 machnet || true
sudo pkill -9 ldc || true
#pm2 del all
#pm2 start 'echo "y" | ./machnet.sh' || true
"""
                    )
                if i >= num_clients:
                    # wait for dataset.txt and operations.txt
                    stdout = "no"
                    while stdout == "no":
                        # check if dataset.txt exists and if so return 'yes' to break the loop
                        stdout = await run_command(
                            f"ls {ldc_dir}/build/operations.txt || echo no"
                        )
                elif i < num_clients:
                    # wait for servers to start (ldc)
                    stdout = "no"
                    while stdout == "no":
                        # check if ldc exists in first server and if so return 'yes' to break the loop
                        username, hostname = clients[num_clients]
                        stdout = await run_command(
                            f"""
ssh -t -oStrictHostKeyChecking=no -i {args.identity_file} {username}@{hostname} "pgrep ldc || echo no"
"""
                        )
                    if i == 0:
                        global done_sync
                        done_sync = True
                    while not done_sync:
                        await asyncio.sleep(1)

                await run_command(f"pkill ldc || true")
                if i < num_clients:
                    # wait for servers to boot
                    # await asyncio.sleep(5)

                    FAILED = "machnet_connect"
                    stdout = FAILED
                    await run_command(
                        f"""
cd {build_dir}
rm -rf ./latency_data_client_*
rm -rf ./r_latency_data_client_*
rm -rf ./w_latency_data_client_*
./bin/ldc -config {config_path} -dataset_config {ops_config_path} -machine_index {i} -threads {num_threads} -clients_per_threads {num_clients_per_thread} &> run.log
"""
                    )

                    # check if failed to run ldc
                    stdout = await run_command(
                        f'cat {build_dir}/run.log | grep "{FAILED}" || echo done'
                    )
                    if stdout == FAILED:
                        sys.exit(-1)

                # After the clients are done, kill the servers
                if i == 0:
                    for j in range(num_clients, total_servers):
                        username, hostname = clients[j]
                        await run_command(
                            f"""
ssh -t -oStrictHostKeyChecking=no -i {args.identity_file} {username}@{hostname} "sudo pkill -9 machnet || true"
ssh -t -oStrictHostKeyChecking=no -i {args.identity_file} {username}@{hostname} "pkill -SIGINT -f ldc || true"
"""
                        )

                # Run servers
                elif i >= num_clients and i < total_servers:
                    await run_command(
                        f"""
cd {build_dir}
./bin/ldc -config {config_path} -dataset_config {ops_config_path} -machine_index {i} -threads {num_threads} -clients_per_threads {num_clients_per_thread} &> run.log
"""
                    )

                await asyncio.sleep(5)
                if i >= num_clients and i < total_servers_not_shared:
                    cache_dump_path = data_path / f"cache_dump_{i}.txt"
                    await asyncssh.scp(
                        (conn, f"{build_dir}/cache_dump.txt"), cache_dump_path
                    )

                    cache_metrics_path = data_path / f"cache_metrics_{i}.json"
                    await asyncssh.scp(
                        (conn, f"{build_dir}/cache_metrics.json"), cache_metrics_path
                    )

                if i < num_clients:
                    dataset_path = data_path / f"{i}" / f"dataset.txt"
                    dataset_path.parent.mkdir(exist_ok=True, parents=True)
                    operations_path = data_path / f"{i}" / f"operations.txt"
                    operations_path.parent.mkdir(exist_ok=True, parents=True)

                    # await asyncssh.scp((conn, f"{build_dir}/dataset.txt"), dataset_path)
                    # await asyncssh.scp(
                    #     (conn, f"{build_dir}/operations.txt"), operations_path
                    # )

                    await run_command(
                        f"""
cd {build_dir}
python {cal_latency_path} ./ >> run.log
"""
                    )
                    metrics_path = data_path / f"latency_results_{i}.json"
                    await asyncssh.scp(
                        (conn, f"{build_dir}/latency_results.json"), metrics_path
                    )
                    metrics_path = data_path / f"r_latency_results_{i}.json"
                    await asyncssh.scp(
                        (conn, f"{build_dir}/r_latency_results.json"), metrics_path
                    )
                    metrics_path = data_path / f"w_latency_results_{i}.json"
                    await asyncssh.scp(
                        (conn, f"{build_dir}/w_latency_results.json"), metrics_path
                    )
                    metrics_path = data_path / f"config_{i}.json"
                    await asyncssh.scp((conn, f"{config_path}"), metrics_path)
                    metrics_path = data_path / f"ops_config_{i}.json"
                    await asyncssh.scp((conn, f"{ops_config_path}"), metrics_path)

                if i < total_servers_not_shared:
                    metrics_path = data_path / f"run_{i}.log"
                    await asyncssh.scp((conn, f"{build_dir}/run.log"), metrics_path)

                if i == 0:
                    # make sure we have all jsons
                    while True:
                        latency_results_jsons = sorted(
                            data_path.glob("latency_results*.json")
                        )
                        cache_metrics_jsons = sorted(
                            data_path.glob("cache_metrics*.json")
                        )

                        if len(latency_results_jsons) == num_clients:
                            break
                        else:
                            print(
                                f"Waiting for all jsons to be generated. Currently have {len(latency_results_jsons)} out of {num_clients} | {len(cache_metrics_jsons)} out of {num_servers}"
                            )
                            await asyncio.sleep(1)

            return True

        interfaces = list(soup.find_all("login"))

        def get_ssh_endpoint(interface):
            ssh_endpoint = f'{interface["username"]}@{interface["hostname"]}'
            return ssh_endpoint

        ssh_endpoints = [
            get_ssh_endpoint(interface)
            for interface in interfaces
            if args.username in interface["username"]
        ]

        def get_username_hostname(interface):
            username = interface["username"]
            hostname = interface["hostname"]
            if args.username != username:
                return None
            return username, hostname

        username_hostname = [
            get_username_hostname(interface) for interface in interfaces
        ]
        cloudlab_password = ""
        p_key = asyncssh.read_private_key(identity_file, cloudlab_password)

        async def run_client(
            i, username_hostname, clients
        ) -> asyncssh.SSHCompletedProcess:
            username = username_hostname[0]
            hostname = username_hostname[1]
            async with asyncssh.connect(
                host=hostname,
                username=username,
                port=22,
                client_keys=[p_key],
                known_hosts=None,
            ) as conn:
                r = await run_on_node(i, conn, clients)
                return r

        async def run_multiple_clients() -> None:
            # Put your lists of hosts here
            clients = [u for u in username_hostname if u is not None]
            tasks = (run_client(i, c, clients) for i, c in enumerate(clients))
            # tasks = (run_client(u) for u in username_hostname[:5])
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results, 1):
                if isinstance(result, Exception):
                    print("Task %d failed: %s" % (i, str(result)))
                else:
                    print(f"Task {i} Success")

        asyncio.run(run_multiple_clients())
