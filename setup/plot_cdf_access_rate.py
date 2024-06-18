import os
import numpy as np
import matplotlib.pyplot as plt
from multiprocessing import Pool
from tqdm import tqdm
import argparse

def plot_cdf_graph(directory):
    freq_begin = 40
    freq = 4000
    with open("/mydata/ycsb/zipfian_0.99/unique_counts_reversed.txt", "r") as file:
    # with open("/mnt/sda4/LDC/src/cdf_graph_.txt", "r") as file:
        unique_sorted = []
        counts_sorted = []
        for line in file:
            key, count = line.split(':')
            unique_sorted.append(int(key))
            counts_sorted.append(int(count))
            # if(int(count) >= 1000):
    with open("/mnt/sda4/LDC/setup/results/access_rate_dynamic::C_zipfian_YCSB_3340000_ns3_nc3_ncpt2_nt8_rd0_ht48_rdmaTrue_asyncTrue_diskTrue_polluteTrue_access_rate30000000/shadow_freq_4.txt", "r") as file:
        unique_sorted_ = []
        counts_sorted_ = []
        for line_ in file:
            key_, count_ = line_.split(' ')
            unique_sorted_.append(int(key_))
            counts_sorted_.append(int(count_))
            # if(int(count_) >= 1000):
    
    # counts_sorted = counts_sorted/np.sum(counts_sorted)
    print(np.sum(counts_sorted_))
    # counts_sorted_ = counts_sorted_/np.sum(counts_sorted_)
    # counts_sorted_ = counts_sorted_[freq_begin:freq]
    fig = plt.figure(figsize=(12, 6))
    ax1 = fig.add_subplot(111)
    ax1.plot(counts_sorted, label='zipfian_0.99')
    # ax1.plot(counts_sorted_, label='cdf')
    ax1.set_xlabel('$p$')
    ax1.set_ylabel('$x$')
    ax1.set_title('CDF of Keys')
    plt.tight_layout()
    plt.savefig(f'cdf_graph.png')

if __name__ == "__main__":
    # plot_cdf_graph('/mydata/ycsb_workloads/hotspot_95_5/')
    parser = argparse.ArgumentParser(description='Get file directory')
    parser.add_argument('-d', '--directory', type=str, required=True)
    args = parser.parse_args()
    # plot_cdf_graph('/mydata/workload/zipfian_default')
    plot_cdf_graph(args.directory)