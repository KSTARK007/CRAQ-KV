#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <string>
#include <dlib/optimization.h>



std::vector<uint64_t> extract_keys(const std::string& filename) {
    std::vector<uint64_t> keys;
    auto total_keys = 10000000;
    std::ifstream file(filename);
    uint64_t key;
    char node;
    char op;

    while (file >> key >> node >> op && keys.size() < total_keys){
        keys.push_back(key);
    }

    file.close();
    std::cout << "Total keys: " << keys.size() << std::endl;
    return keys;
}

std::vector<std::pair<uint64_t, std::string>> sort_keys_by_frequency(const std::vector<uint64_t>& keys) {
    std::map<uint64_t, uint64_t> frequency;
    for (uint64_t key : keys) {
        frequency[key]++;
    }

    std::vector<std::pair<uint64_t, std::string>> freq_vector;
    for (auto& kv : frequency) {
        freq_vector.emplace_back(static_cast<uint64_t>(kv.second), std::to_string(kv.first));
    }

    std::sort(freq_vector.begin(), freq_vector.end(), [](const std::pair<uint64_t, std::string>& a, const std::pair<uint64_t, std::string>& b) {
        return a.first > b.first; // Sort in descending order by frequency
    });

    return freq_vector;
}

void write_cdf_to_file(const std::vector<std::pair<uint64_t, std::string>>& freq_vector, const std::string& filename) {
    std::ofstream file(filename);
    uint64_t total = 0;
    for (const auto& kv : freq_vector) {
        total += kv.first;
    }

    uint64_t sum = 0;
    for (const auto& kv : freq_vector) {
        sum += kv.first;
        file << kv.first << " " << kv.second << " " << sum << " " << total << std::endl;
    }

    file.close();
}

std::vector<std::pair<uint64_t, std::string>> zipfian_workload() {
    auto keys = extract_keys("/mydata/ycsb_traces/zipfian/all_data.txt");
    std::cout << "Total keys: " << keys.size() << std::endl;
    auto freq_vector = sort_keys_by_frequency(keys);
    write_cdf_to_file(freq_vector, "zipfian_cdf.txt");
    return freq_vector;
}

std::vector<std::pair<uint64_t, std::string>> uniform_workload() {
    auto keys = extract_keys("/mydata/ycsb_traces/uniform/all_data.txt");
    std::cout << "Total keys: " << keys.size() << std::endl;
    auto freq_vector = sort_keys_by_frequency(keys);
    write_cdf_to_file(freq_vector, "uniform_cdf.txt");
    return freq_vector;
}

std::vector<std::pair<uint64_t, std::string>> hotspot_workload() {
    auto keys = extract_keys("/mydata/ycsb_traces/hotspot/all_data.txt");
    std::cout << "Total keys: " << keys.size() << std::endl;
    auto freq_vector = sort_keys_by_frequency(keys);
    write_cdf_to_file(freq_vector, "hotspot_cdf.txt");
    return freq_vector;
}

uint64_t get_sum_freq_till_index(std::vector<std::pair<uint64_t,std::string>> cdf, uint64_t start, uint64_t end)
{
    uint64_t sum = 0;
    for (uint64_t i = start; i < end; i++)
    {
        sum += cdf[i].first;
    }
    return sum;
}

uint64_t calculate_performance(std::vector<std::pair<uint64_t,std::string>> cdf, uint64_t water_mark_local, uint64_t water_mark_remote, uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg)
{
    uint64_t total_keys = cdf.size();
    uint64_t total_local_accesses = get_sum_freq_till_index(cdf, 0, water_mark_local);
    uint64_t total_remote_accesses = get_sum_freq_till_index(cdf, water_mark_local, water_mark_local + water_mark_remote);
    uint64_t total_disk_accesses = get_sum_freq_till_index(cdf, water_mark_local + water_mark_remote, total_keys);
    uint64_t local_latency = total_local_accesses * cache_ns_avg;
    // uint64_t remote_latency = (((2 / 3) * total_remote_accesses) * rdma_ns_avg) + (((1/3)*(total_remote_accesses)) * local_latency);
    uint64_t remote_latency = total_remote_accesses * rdma_ns_avg;
    uint64_t disk_latency = total_disk_accesses * disk_ns_avg;
    uint64_t perf_mul = -1;
    uint64_t performance = perf_mul / (local_latency + remote_latency + disk_latency);
    std::cout << "Local latency: " << local_latency << ", Remote latency: " << remote_latency << ", Disk latency: " << disk_latency << ", Performance: " << performance << std::endl;
    std::cout << "Total keys: " << total_keys << ", Total local accesses: " << total_local_accesses << ", Total remote accesses: " << total_remote_accesses << ", Total disk accesses: " << total_disk_accesses << std::endl<< std::endl<< std::endl;
    return performance;
}

size_t percentage_to_index(size_t total_size, float percent) {
    return static_cast<size_t>(total_size * (percent / 100.0));
}

void log_performance_state(uint64_t iteration, uint64_t L, uint64_t remote, uint64_t performance, const std::string& message) {
    std::cout << "Iteration: " << iteration
              << ", L: " << L
              << ", Remote: " << remote
              << ", Performance: " << performance
              << ", Message: " << message << std::endl;
}

void get_best_access_rates(std::vector<std::pair<uint64_t,std::string>>& cdf, uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg) {
   
    uint64_t iteration = 0;
    auto initial_water_mark_local = 0.0;
    auto initial_water_mark_remote = 0.6 * cdf.size();
    uint64_t best_performance = calculate_performance(cdf, initial_water_mark_local, initial_water_mark_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
    
    uint64_t best_local = initial_water_mark_local;
    uint64_t best_remote = initial_water_mark_remote;
    
    log_performance_state(iteration++, best_local, best_remote, best_performance, "Initial configuration");

    // Adjust L dynamically
    bool improved_increasing;
    bool improved_decreasing;
    bool reduced_perf_increasing;
    bool reduced_perf_decreasing;

    float performance__delta_threshold = 0.005; // 0.5%
    
    do {
        improved_increasing = false;
        reduced_perf_increasing = false;

        improved_decreasing = false;
        reduced_perf_decreasing = false;
        
        // Test increasing L
        for (int i = 1; i <= 3; i++) {
            uint64_t new_local = best_local + percentage_to_index(cdf.size(), 0.1 * i);
            uint64_t new_remote = cdf.size() - (new_local * 3);
            
            if (new_remote >= cdf.size()) break;
            
            uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
            std::string message = "Increased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
            log_performance_state(iteration++, new_local, new_remote, new_performance, message);
            
            if (new_performance > best_performance * (1 + performance__delta_threshold)) {
                std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                std::cout << "old performance: " << best_performance << std::endl;
                best_performance = new_performance;
                best_local = new_local;
                best_remote = new_remote;
                improved_increasing = true;
                std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                std::cout << "best performance: " << best_performance << std::endl;
            }
            else {
                if(new_performance < best_performance * (1 - performance__delta_threshold)){
                    reduced_perf_increasing = true;
                }
            }

        }
        
        // Test decreasing L
        if(!improved_increasing)
        {
            for (int i = 1; i <= 3; i++) {
                uint64_t new_local = best_local - percentage_to_index(cdf.size(), 0.1 * i);
                
                if (new_local == 0) break;
                
                uint64_t new_remote = cdf.size() - (new_local * 3);;
                uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
                std::string message = "Decreased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
                log_performance_state(iteration++, new_local, new_remote, new_performance, message);

                if (new_performance > best_performance * (1 + performance__delta_threshold) ) {
                    std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                    std::cout << "old performance: " << best_performance << std::endl;
                    best_performance = new_performance;
                    best_local = new_local;
                    best_remote = new_remote;
                    improved_increasing = true;
                    std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                    std::cout << "best performance: " << best_performance << std::endl;
                }
                else {
                    if(new_performance < best_performance * (1 - performance__delta_threshold) ) {
                        reduced_perf_decreasing = true;
                    }
                }
            }
        }
        
    } while (improved_increasing || improved_decreasing);
    
    // If no improvement in either round, move by 1%
    if (!improved_increasing && !improved_decreasing) {
        uint64_t new_remote = cdf.size() - (best_local * 3);
        uint64_t new_local = best_local;
        
        bool improved_increasing = false;
        bool reduced_perf_increasing = false;
        bool improved_decreasing = false;
        bool reduced_perf_decreasing = false;

        while (new_remote > 0 && !reduced_perf_increasing)
        {
            new_local = best_local + percentage_to_index(cdf.size(), 1.0);
            new_remote = cdf.size() - (new_local * 3);;
            
            if (new_remote < cdf.size()) {
                uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
                std::string message = "Increased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
                log_performance_state(iteration++, new_local, new_remote, new_performance, message);
                if (new_performance > best_performance * (1 + performance__delta_threshold) ) {
                    std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                    std::cout << "old performance: " << best_performance << std::endl;
                    best_performance = new_performance;
                    best_local = new_local;
                    best_remote = new_remote;
                    improved_increasing = true;
                    std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                    std::cout << "best performance: " << best_performance << std::endl;
                } else {
                    if(new_performance < best_performance * (1 - performance__delta_threshold)) {
                        reduced_perf_increasing = true;
                    }
                }
            }
        }
        
        // If no improvement, test decreasing L by 1%
        if (best_local == initial_water_mark_local) {
            new_local = best_local - percentage_to_index(cdf.size(), 1.0);
            
            while (new_local > 0 && reduced_perf_decreasing) {
                new_remote = cdf.size() - (new_local * 3);;
                uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
                std::string message = "Decreased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
                log_performance_state(iteration++, new_local, new_remote, new_performance, message);

                if (new_performance > best_performance * (1 + performance__delta_threshold)) {
                    std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                    std::cout << "old performance: " << best_performance << std::endl;
                    best_performance = new_performance;
                    best_local = new_local;
                    best_remote = new_remote;
                    improved_increasing = true;
                    std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                    std::cout << "best performance: " << best_performance << std::endl;
                    break;
                } else {
                    if(new_performance < best_performance * (1 - performance__delta_threshold)){
                        reduced_perf_decreasing = true;
                    }
                }
                
                new_local -= percentage_to_index(cdf.size(), 1.0);
            }
        }
    }
    std::cout << "Best local: " << best_local << ", Best remote: " << best_remote << ", Best performance: " << best_performance << std::endl;
    // Set new optimized water marks and access rate
    // set_water_marks(cache, best_local, best_remote);
    // uint64_t best_access_rate = (cdf[best_local].first) * 0.90;
    // cache->get_cache()->set_access_rate(best_access_rate);
    
    // // Get and set keys under L
    // std::vector<std::string> keys_under_l = get_keys_under_l(cdf, best_local);
    // cache->get_cache()->set_keys_under_l(keys_under_l);
}

double objective_function(const std::vector<std::pair<uint64_t, std::string>>& cdf, const uint64_t& cache_ns_avg, const uint64_t& disk_ns_avg, const uint64_t& rdma_ns_avg, const dlib::matrix<double, 2, 1>& params) {
    double L_percent = params(0);
    double remote_percent = params(1);

    size_t L_index = static_cast<size_t>(L_percent * cdf.size());
    size_t remote_index = static_cast<size_t>(remote_percent * cdf.size());

    uint64_t performance = calculate_performance(cdf, L_index, remote_index, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
    return -static_cast<double>(performance);  // Minimize negative performance
}

void get_best_access_rates_dlib(std::vector<std::pair<uint64_t, std::string>>& cdf, uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg, uint64_t cache_size) {
    dlib::matrix<double, 2, 1> params;
    params = 0.0, 0.6; // Initial guesses for L_percent and remote_percent

    dlib::matrix<double, 2, 1> lower_bounds = {0, 0};
    dlib::matrix<double, 2, 1> upper_bounds = {0.334, 0.6};

    // Adjusting npt within valid range for a 2-dimensional problem
    int n = 2;  // number of dimensions
    long npt = n + 2;  // Minimum valid value for npt

    dlib::find_min_bobyqa(
        [&](const dlib::matrix<double, 2, 1>& params) { return objective_function(cdf, cache_ns_avg, disk_ns_avg, rdma_ns_avg, params); },
        params,
        npt,  // Number of interpolation points, must be in range [n+2, (n+1)(n+2)/2]
        lower_bounds,  // Lower bounds for L_percent and remote_percent
        upper_bounds,  // Upper bounds
        0.1,  // Initial trust region radius, must satisfy: min(x_upper - x_lower) > 2*rho_begin
        1e-6,  // Stopping trust region radius
        10000    // Max number of function evaluations
    );

    double L_percent = params(0);
    double remote_percent = params(1);
    uint64_t L = static_cast<uint64_t>(L_percent * cdf.size());
    uint64_t remote = static_cast<uint64_t>(remote_percent * cdf.size());
    uint64_t performance = -static_cast<uint64_t>(objective_function(cdf, cache_ns_avg, disk_ns_avg, rdma_ns_avg, params));  // Convert back to positive performance

    std::cout << "Optimized Performance: " << performance << std::endl;
    std::cout << "L Index: " << L << ", Remote Index: " << remote << std::endl;
}

int main() {
    auto zipfian_cdf = zipfian_workload();
    auto disk_latency = 100 * 1000;  // Example latencies
    auto cache_latency = 1 * 1000;
    auto rdma_latency = 500 * 1000;
    auto cache_size = 60000;

    std::cout << "Zipfian" << std::endl;
    get_best_access_rates_dlib(zipfian_cdf, cache_latency, disk_latency, rdma_latency, cache_size);

    return 0;
}