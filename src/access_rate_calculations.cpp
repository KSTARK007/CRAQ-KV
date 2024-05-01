#include "access_rate_calculations.h"

std::vector<std::pair<uint64_t,std::string>> get_and_sort_freq(std::shared_ptr<BlockCache<std::string, std::string>> cache)
{
    std::vector<std::pair<std::string, uint64_t>> key_freq = cache->get_cache()->get_key_freq_map();
    uint64_t total_keys = cache->get_cache()->get_block_db_num_entries();
    std::vector<std::pair<uint64_t,std::string>> sorted_key_freq;

    // Insert key frequency pairs, swapping key and value for sorting purposes.
    for (auto &it : key_freq)
    {
        sorted_key_freq.push_back(std::make_pair(it.second, it.first));
    }

    // Sort by frequency (first element of pair), as per std::pair's default sort behavior
    std::sort(sorted_key_freq.begin(), sorted_key_freq.end());

    // Resize the vector if it has more entries than total_keys
    if (sorted_key_freq.size() > total_keys)
    {
        sorted_key_freq.resize(total_keys);
    }

    // Add missing keys with frequency 0 if the vector has fewer entries than total_keys
    if (sorted_key_freq.size() < total_keys)
    {
        std::set<std::string> existing_keys;
        for (auto &it : sorted_key_freq)
        {
            existing_keys.insert(it.second);
        }

        for (uint64_t i = 1; i <= total_keys; i++)
        {
            std::string key = std::to_string(i);
            if (existing_keys.find(key) == existing_keys.end())
            {
                sorted_key_freq.push_back(std::make_pair(0, key));
            }
        }

        // Sort in decending order
        std::sort(sorted_key_freq.begin(), sorted_key_freq.end(), std::greater<std::pair<uint64_t, std::string>>());
        // std::sort(sorted_key_freq.begin(), sorted_key_freq.end());
    }

    return sorted_key_freq;
}

// Function to get and maintain keys under L
std::vector<std::string> get_keys_under_l(const std::vector<std::pair<uint64_t, std::string>>& cdf, uint64_t L) {
    std::vector<std::string> keys;
    for (size_t i = 0; i < L && i < cdf.size(); i++) {
        keys.push_back(cdf[i].second);
    }
    return keys;
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

void set_water_marks(std::shared_ptr<BlockCache<std::string, std::string>> cache, uint64_t water_mark_local, uint64_t water_mark_remote)
{
    cache->get_cache()->set_water_marks(water_mark_local, water_mark_remote);
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
    uint64_t performance = 0;
    if (local_latency + remote_latency + disk_latency != 0)
    {
        performance = total_keys / (local_latency + remote_latency + disk_latency);
    }
    std::cout << "Local latency: " << local_latency << ", Remote latency: " << remote_latency << ", Disk latency: " << disk_latency << ", Performance: " << performance << std::endl;
    std::cout << "Total keys: " << total_keys << ", Total local accesses: " << total_local_accesses << ", Total remote accesses: " << total_remote_accesses << ", Total disk accesses: " << total_disk_accesses << std::endl;
    return performance;
}

size_t percentage_to_index(size_t total_size, float percent) {
    return static_cast<size_t>(total_size * (percent / 100.0));
}

void log_performance_state(uint64_t iteration, uint64_t L, uint64_t remote, uint64_t performance, const std::string& message) {
    // std::cout << "Iteration: " << iteration
    //           << ", L: " << L
    //           << ", Remote: " << remote
    //           << ", Performance: " << performance
    //           << ", Message: " << message << std::endl << std::endl;
    info("Iteration: {}, L: {}, Remote: {}, Performance: {}, Message: {}", std::to_string(iteration), std::to_string(L), std::to_string(remote), std::to_string(performance), message);
}

void itr_through_all_the_perf_values_to_find_optimal(std::shared_ptr<BlockCache<std::string, std::string>> cache, std::vector<std::pair<uint64_t,std::string>> cdf, uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg)
{
    std::tuple<uint64_t, uint64_t, uint64_t> water_marks = cache->get_cache()->get_water_marks();
    uint64_t cache_size = cache->get_cache()->get_cache_size();
    uint64_t water_mark_local = std::get<0>(water_marks);
    uint64_t water_mark_remote = std::get<1>(water_marks);
    uint64_t performance = calculate_performance(cdf, water_mark_local, water_mark_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
    uint64_t best_performance = performance;
    uint64_t best_water_mark_local = water_mark_local;
    uint64_t best_water_mark_remote = water_mark_remote;
    int remote = cache_size - (3 * water_mark_local);

    for (uint64_t i = 0; i < cdf.size(); i+=10)
    {
        uint64_t local = i;
        remote = cache_size - (3 * local);
        if (remote < 0)
        {
            break;
        }
        if(local > cache_size/3)
        {
            break;
        }
        uint64_t new_performance = calculate_performance(cdf, local, remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
        if (new_performance > best_performance)
        {
            best_performance = new_performance;
            best_water_mark_local = local;
            best_water_mark_remote = remote;
        }
        log_performance_state(i, local, remote, new_performance, "");
    }
    uint64_t best_access_rate = (cdf[best_water_mark_local].first) * 0.90;
    std::cout << "Best local: " << best_water_mark_local << ", Best remote: " << best_water_mark_remote << ", Best performance: " << best_performance << std::endl;
    set_water_marks(cache, best_water_mark_local, best_water_mark_remote);
    cache->get_cache()->set_access_rate(best_access_rate);
}

void get_best_access_rates(std::shared_ptr<BlockCache<std::string, std::string>> cache, std::vector<std::pair<uint64_t,std::string>>& cdf, uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg) {
    info("Calculating best access rates");
    auto [initial_water_mark_local, initial_water_mark_remote, _] = cache->get_cache()->get_water_marks();
    info("Initial water mark local: {}, Initial water mark remote: {}", std::to_string(initial_water_mark_local), std::to_string(initial_water_mark_remote));
    uint64_t cache_size = cache->get_cache()->get_cache_size();
    info("Cache size: {}", std::to_string(cache_size));
    uint64_t best_performance = calculate_performance(cdf, initial_water_mark_local, initial_water_mark_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
    info("Initial performance: {}", std::to_string(best_performance));
    uint64_t iteration = 0;
    uint64_t best_local = initial_water_mark_local;
    uint64_t best_remote = initial_water_mark_remote;
    log_performance_state(iteration++, best_local, best_remote, best_performance, "Initial configuration");

    // Adjust L dynamically
    bool improved_increasing;
    bool improved_decreasing;
    bool reduced_perf_increasing;
    bool reduced_perf_decreasing;
    bool improved = false;
    float performance__delta_threshold = 0.000; // 0.5%
    
    do {
        improved_increasing = false;
        reduced_perf_increasing = false;

        improved_decreasing = false;
        reduced_perf_decreasing = false;
        
        // Test increasing L
        for (int i = 1; i <= 3; i++) {
            int new_local = best_local + percentage_to_index(cdf.size(), 0.01 * i);
            int new_remote = cache_size - (new_local * 3);
            if(new_remote < 0) break;
            if(new_local > cache_size/3) break;
            if (new_remote >= cache_size) break;
            
            uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
            std::string message = "Increased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
            log_performance_state(iteration++, new_local, new_remote, new_performance, message);
            
            if (new_performance > best_performance * (1 + performance__delta_threshold)) {
                // std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                // std::cout << "old performance: " << best_performance << std::endl;
                best_performance = new_performance;
                best_local = new_local;
                best_remote = new_remote;
                improved_increasing = true;
                improved = true;
                // std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                // std::cout << "best performance: " << best_performance << std::endl;
            }
            else {
                if(new_performance < best_performance * (1 - performance__delta_threshold)){
                    reduced_perf_increasing = true;
                }
            }

        }
        int new_local = best_local;
        // Test decreasing L
        if(!improved_increasing)
        {
            while (new_local > 0 && !reduced_perf_decreasing) {
                new_local = new_local - percentage_to_index(cdf.size(), 0.01);
                int new_remote = cache_size - (new_local * 3);
                if (new_local < 0) break;
                if (new_remote >= cache_size) break;
                uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
                std::string message = "Decreased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
                log_performance_state(iteration++, new_local, new_remote, new_performance, message);

                if (new_performance > best_performance * (1 + performance__delta_threshold)) {
                    // std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                    // std::cout << "old performance: " << best_performance << std::endl;
                    best_performance = new_performance;
                    best_local = new_local;
                    best_remote = new_remote;
                    improved_decreasing = true;
                    improved = true;
                    // std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                    // std::cout << "best performance: " << best_performance << std::endl;
                }
                else {
                    if(new_performance < best_performance * (1 - performance__delta_threshold)){
                        reduced_perf_decreasing = true;
                    }
                }
            }
        }
        // std::cout<<"improved_increasing: "<<improved_increasing<<std::endl;
        // std::cout<<"reduced_perf_increasing: "<<reduced_perf_increasing<<std::endl;
    } while (improved_increasing || improved_decreasing);
    // If no improvement in either round, move by 1%
    // std::cout<<"improved_increasing: "<<improved_increasing<<std::endl;
    // std::cout<<"reduced_perf_increasing: "<<improved_decreasing<<std::endl;
    if (!improved) {
        // std::cout << "No improvement in either round" << std::endl;
        info("No improvement in either round");
        uint64_t new_remote = cache_size - (best_local * 3);
        uint64_t new_local = best_local;
        
        bool improved_increasing = false;
        bool reduced_perf_increasing = false;
        bool improved_decreasing = false;
        bool reduced_perf_decreasing = false;
        new_local = best_local + percentage_to_index(cdf.size(), 1.0);

        while (new_remote < cache_size && !reduced_perf_increasing)
        {
            new_local = new_local + percentage_to_index(cdf.size(), 1.0);
            new_remote = cache_size - (new_local * 3);;
            if (new_remote >= cache_size) break;
            uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
            std::string message = "Increased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
            log_performance_state(iteration++, new_local, new_remote, new_performance, message);
            if (new_performance > best_performance * (1 + performance__delta_threshold) ) {
                // std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                // std::cout << "old performance: " << best_performance << std::endl;
                best_performance = new_performance;
                best_local = new_local;
                best_remote = new_remote;
                improved_increasing = true;
                // std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                // std::cout << "best performance: " << best_performance << std::endl;
            } else {
                if(new_performance < best_performance * (1 - performance__delta_threshold)) {
                    reduced_perf_increasing = true;
                }
            }
            // std::cout<<"new_remote: "<<new_remote<<std::endl;
            info("new_local: {} , new_remote: {} , best_performance: {}", std::to_string(new_local), std::to_string(new_remote), std::to_string(best_performance));
        }
        
        // If no improvement, test decreasing L by 1%
        if (best_local == initial_water_mark_local) {
            new_local = best_local - percentage_to_index(cdf.size(), 1.0);
            
            while (new_local > 0 && reduced_perf_decreasing) {
                new_remote = cache_size - (new_local * 3);;
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
            // std::cout<<"new_local: "<<new_local<<std::endl;
            info("new_local: {}, new_remote: {}, best_performance: {} ", std::to_string(new_local), std::to_string(new_remote), std::to_string(best_performance));
        }
    }
    // std::cout << "Best local: " << best_local << ", Best remote: " << best_remote << ", Best performance: " << best_performance << std::endl;
    info("Best local: {} , Best remote: {} , Best performance: {}", std::to_string(best_local), std::to_string(best_remote), std::to_string(best_performance));
    
    // Set new optimized water marks and access rate
    // set_water_marks(cache, best_local, best_remote);
    // uint64_t best_access_rate = (cdf[best_local].first) * 0.90;
    // cache->get_cache()->set_access_rate(best_access_rate);
    
    // // Get and set keys under L
    // std::vector<std::string> keys_under_l = get_keys_under_l(cdf, best_local);
    // cache->get_cache()->set_keys_under_l(keys_under_l);
}