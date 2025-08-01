# CRAQ Key-Value Store

## Overview

CRAQ (Chain Replication with Apportioned Queries) is a strongly consistent key-value store that implements chain replication for fault tolerance. In CRAQ, replicas are arranged in a chain where writes traverse from head to tail to ensure consistency, while reads can be served from any replica with version checks to maintain linearizability.


## CloudLab Setup

**⚠️ This system is designed specifically for CloudLab and requires proper cluster configuration.**

### Prerequisites
- CloudLab cluster with **minimum 6 nodes** of type **xl170** (3 nodes for clients, 3 nodes for servers)
- Ubuntu 22.04 LTS (UBUNTU22-64-STD image)
- SSH key access configured

### Setup Process

1. **Configure CloudLab Cluster**
   ```bash
   # Store your CloudLab manifest file
   cp your_cloudlab_manifest.xml config/cloudlab_machines.xml
   ```

2. **Compile and Deploy**
   ```bash
   # This script compiles code and deploys to all nodes
   cd setup/
   ./run_compile.sh
   ```
   - Builds binary at `/mnt/sda4/LDC/build` on all nodes
   - Sets up dependencies and environment

3. **Generate YCSB Workloads**
   ```bash
   # Generate workload traces
   ./create_ycsb_workload.sh
   ```
   - Creates traces in `/mydata/ycsb_traces/` and `/mydata/ycsb/`
   - Supports: uniform, hotspot, zipfian distributions

4. **Run Experiments**
   ```bash
   # Run the distributed system
   ./run.sh
   ```

## Configuration Options

### System Types
- **A**: OrigCache
- **C**: RDMA enabled system

### Cache Policies
- `thread_safe_lru`: Thread-safe LRU eviction (Static-NoAdmit)
- `nchance`: N-chance forwarding policy (CC)
- `access_rate`: Access rate-based policy (Admit-Cons(10), Admit-Opt(1))
- `access_rate_dynamic`: Dynamic access rate-based policy(LUC)

### Workload Distributions
- `uniform`: Uniform key access pattern
- `hotspot`: Configurable hotspot pattern (default: 80% accesses to 20% keys)
- `zipfian`: Zipfian distribution with configurable skew (default: 0.99)

### Configurable Parameters
- **Cache sizes**: 0.10, 0.15, 0.20, 0.25, 0.30, 0.334 (fraction of dataset)
- **Access rates**: #requests/second 
- **Number of servers**: 3-7 nodes
- **Threads per server**: 1-16 threads
- **Clients per thread**: 1-16 clients

## Key Files

- `config/cloudlab_machines.xml`: CloudLab cluster configuration
- `setup/run_compile.sh`: Build and deployment script
- `setup/create_ycsb_workload.sh`: Workload generation
- `setup/run.sh`: Experiment execution
- `src/`: Core CRAQ implementation with RDMA support
