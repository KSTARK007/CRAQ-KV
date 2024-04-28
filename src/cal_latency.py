import os
import numpy as np
import json
import sys

def process_file(file_path):
    with open(file_path, 'r') as file:
        latencies = [int(line.strip()) for line in file.readlines()]
        total_time_ns = sum(latencies)
        total_time_s = total_time_ns / 1e9
        throughput = len(latencies) / total_time_s

        average_latency_us = total_time_ns / len(latencies) / 1000
        tail_latency_us = np.percentile(latencies, 99) / 1000
        p50 = np.percentile(latencies, 50) / 1000

        
        return {
            'file': os.path.basename(file_path),
            'average_latency_us': average_latency_us,
            'throughput': throughput,
            'p99': tail_latency_us,
            'p50': p50
        }

def process_directory(directory_path):
    results = []
    total_throughput = 0
    median_latency_list = []
    tail_latency_list = []
    p50_latency_list = []
    
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.startswith("latency_data_client"):
                file_path = os.path.join(root, file)
                result = process_file(file_path)
                results.append(result)
                
                # Accumulate metrics
                total_throughput += result['throughput']
                median_latency_list.append(result['average_latency_us'])
                tail_latency_list.append(result['p99'])
                p50_latency_list.append(result['p50'])
    
    # Calculate averages of the 50th and 99th percentiles
    total_median_latency_us = np.mean(median_latency_list) if median_latency_list else 0
    total_tail_latency_us = np.mean(tail_latency_list) if tail_latency_list else 0

    # Add total metrics to the results
    total_metrics = {
        'total_throughput': total_throughput,
        'average_latency_us': total_median_latency_us,
        'p99': total_tail_latency_us,
        'p50_latency_us': np.mean(p50_latency_list) if p50_latency_list else 0
    }
    
    return results, total_metrics

def dump_results_to_json(results, total_metrics, output_file_path):
    output_data = {
        'results': results,
        'total': total_metrics
    }
    with open(output_file_path, 'w') as json_file:
        json.dump(output_data, json_file, indent=4)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <path_to_directory>")
    else:
        directory_path = sys.argv[1]
        results, total_metrics = process_directory(directory_path)
        output_file_path = os.path.join(directory_path, 'latency_results.json')
        dump_results_to_json(results, total_metrics, output_file_path)
        print(f"Processed latency data and results are saved to {output_file_path}")
