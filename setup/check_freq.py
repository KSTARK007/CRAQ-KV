import os
import numpy as np
import matplotlib.pyplot as plt
from multiprocessing import Pool
from tqdm import tqdm
import argparse

def process_file(filepath):
    keys = []
    with open(filepath, 'r') as file:
        for line in file:
            key, _, _ = line.split()
            keys.append(int(key))
    unique, counts = np.unique(keys, return_counts=True)
    return unique, counts

def update_unique_counts(unique_counts, new_unique, new_counts):
    unique, counts = unique_counts
    unique_combined = np.union1d(unique, new_unique)
    counts_combined = np.zeros_like(unique_combined)
    
    unique_index = np.searchsorted(unique_combined, unique)
    counts_combined[unique_index] = counts
    
    new_unique_index = np.searchsorted(unique_combined, new_unique)
    counts_combined[new_unique_index] += new_counts
    
    return unique_combined, counts_combined

def plot_cdf_graph(directory):
    pool = Pool()
    file_list = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.startswith("client_") and filename.endswith(".txt")]
    unique_counts = (np.array([]), np.array([]))
    
    with tqdm(total=len(file_list), desc="Processing files", unit="file") as pbar:
        for result in pool.imap_unordered(process_file, file_list):
            new_unique, new_counts = result
            unique_counts = update_unique_counts(unique_counts, new_unique, new_counts)
            pbar.update(1)
    
    pool.close()
    pool.join()

    unique_sorted, counts_sorted = unique_counts
    sorted_indices = np.argsort(-counts_sorted)
    unique_sorted = unique_sorted[sorted_indices]
    counts_sorted = counts_sorted[sorted_indices]

    with open(f'{directory}/unique_counts_reversed.txt', "w") as file:
        for u, c in zip(unique_sorted, counts_sorted):
            file.write(f"{int(u)}:{int(c)}\n")

    cumsum = np.cumsum(counts_sorted)
    c = cumsum / cumsum[-1]

    fig = plt.figure(figsize=(12, 6))
    ax1 = fig.add_subplot(121)
    ax1.plot(c)
    ax1.set_xlabel('$p$')
    ax1.set_ylabel('$x$')
    ax1.set_title('CDF of Keys')
    plt.tight_layout()
    plt.savefig(f'{directory}/cdf_graph.png')

if __name__ == "__main__":
    # plot_cdf_graph('/mydata/ycsb_workloads/hotspot_95_5/')
    parser = argparse.ArgumentParser(description='Get file directory')
    parser.add_argument('-d', '--directory', type=str, required=True)
    args = parser.parse_args()
    # plot_cdf_graph('/mydata/workload/zipfian_default')
    plot_cdf_graph(args.directory)