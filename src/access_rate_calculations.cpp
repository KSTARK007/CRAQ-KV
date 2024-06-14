#include "access_rate_calculations.h"

#include <map>
/*
std::vector<std::pair<uint64_t,std::string>> get_and_sort_freq(std::shared_ptr<BlockCache<std::string, std::string>>
cache)
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
   std::sort(sorted_key_freq.begin(), sorted_key_freq.end(), std::greater<std::pair<uint64_t, std::string>>());

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
*/

static bool *key_map = NULL;

void get_and_sort_freq(std::shared_ptr<BlockCache<std::string, std::string>> cache, CDFType& cdf_result) {
    auto get_time = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<std::string, uint64_t>> &key_freq = cache->get_cache()->get_key_freq_map();
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - get_time).count();

    auto get_num = std::chrono::high_resolution_clock::now();
    uint64_t total_keys = cache->get_cache()->get_block_db_num_entries();
    auto get_num_end_time = std::chrono::high_resolution_clock::now();
    auto get_num_duration = std::chrono::duration_cast<std::chrono::microseconds>(get_num_end_time - get_num).count();
    if (key_map == NULL) {
        key_map = new bool[total_keys + 1];
    }

    std::vector<std::pair<uint64_t, std::string>> sorted_key_freq;
    std::map<uint64_t, uint64_t> bucket_cumilative_freq;

    // Insert key frequency pairs, swapping key and value for sorting purposes.
    auto insert_time = std::chrono::high_resolution_clock::now();
    for (auto& it : key_freq) {
        sorted_key_freq.push_back(std::make_pair(it.second, it.first));
    }
    auto insert_end_time = std::chrono::high_resolution_clock::now();
    auto insert_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(insert_end_time - insert_time).count();

    // Sort by frequency (first element of pair), as per std::pair's default sort behavior
    auto sort_start = std::chrono::high_resolution_clock::now();
    std::sort(sorted_key_freq.begin(), sorted_key_freq.end(), std::greater<std::pair<uint64_t, std::string>>());
    auto sort_end = std::chrono::high_resolution_clock::now();
    auto sort_duration = std::chrono::duration_cast<std::chrono::microseconds>(sort_end - sort_start).count();

    auto total_freq_start = std::chrono::high_resolution_clock::now();
    // Resize the vector if it has more entries than total_keys
    if (sorted_key_freq.size() > total_keys) {
        sorted_key_freq.resize(total_keys);
    }
    auto total_freq_end = std::chrono::high_resolution_clock::now();
    auto total_freq_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(total_freq_end - total_freq_start).count();

    auto missing_keys_start = std::chrono::high_resolution_clock::now();
    // // Add missing keys with frequency 0 if the vector has fewer entries than total_keys
    // if (sorted_key_freq.size() < total_keys) {
    //     std::set<std::string> existing_keys;
    //     for (auto& it : sorted_key_freq) {
    //         existing_keys.insert(it.second);
    //     }

    //     for (uint64_t i = 1; i <= total_keys; i++) {
    //         std::string key = std::to_string(i);
    //         if (existing_keys.find(key) == existing_keys.end()) {
    //             sorted_key_freq.push_back(std::make_pair(0, key));
    //         }
    //     }

    //     // Sort in decending order
    //     auto missing_keys_sort_start = std::chrono::high_resolution_clock::now();
    //     std::sort(sorted_key_freq.begin(), sorted_key_freq.end(), std::greater<std::pair<uint64_t, std::string>>());
    //     auto missing_keys_sort_end = std::chrono::high_resolution_clock::now();
    //     auto missing_keys_sort_duration =
    //         std::chrono::duration_cast<std::chrono::microseconds>(missing_keys_sort_end - missing_keys_sort_start).count();
    // }
    for (uint64_t i = 1; i <= total_keys; i++) {
        key_map[i] = false;
    }
    for (auto &e: sorted_key_freq) {
        key_map[std::stoi(e.second)] = true;
    }
    for (uint64_t i = 1; i <= total_keys; i++) {
        if (!key_map[i]) {
            sorted_key_freq.push_back(std::make_pair(1, std::to_string(i)));
        }
    }
    auto missing_keys_end = std::chrono::high_resolution_clock::now();
    auto missing_keys_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(missing_keys_end - missing_keys_start).count();

    std::vector<std::pair<uint64_t, std::string>> sorted_key_freq_with_buckets;
    std::vector<std::tuple<uint64_t, std::string, uint64_t>> sorted_key_freqs;
    std::map<uint64_t, std::vector<std::pair<uint64_t, std::string>>> cdf_buckets;
    std::map<std::string, std::pair<uint64_t, uint64_t>> key_freq_bucket_map;
    uint64_t total_freq = 0;

    for (const auto& kv : sorted_key_freq) {
        total_freq += kv.first;
    }

    uint64_t cumulative_freq = 0;
    for (const auto& kv : sorted_key_freq) {
        cumulative_freq += kv.first;
        uint64_t percentile = (cumulative_freq * 100) / total_freq;
        cdf_buckets[percentile].push_back(kv);
    }

    // Sort keys within each bucket in descending order
    auto total_cum_sum_start = std::chrono::high_resolution_clock::now();
    uint64_t total_cum_sum = 0;
    for (auto& bucket : cdf_buckets) {
        uint64_t bucket_freq_sum = 0;
        auto& bucket_keys = bucket.second;
        std::sort(bucket_keys.begin(), bucket_keys.end(),
                  [](const auto& a, const auto& b) { return std::stoull(a.second) > std::stoull(b.second); });

        for (const auto& it : bucket_keys) {
            sorted_key_freq_with_buckets.push_back(it);
            sorted_key_freqs.push_back(std::make_tuple(it.first, it.second, bucket.first));
            total_cum_sum += it.first;
            key_freq_bucket_map[it.second] = std::make_pair(total_cum_sum, bucket.first);
        }
    }
    auto total_cum_sum_end = std::chrono::high_resolution_clock::now();
    auto total_cum_sum_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(total_cum_sum_end - total_cum_sum_start).count();
    auto make_pair_time = std::chrono::high_resolution_clock::now();
    cdf_result = std::make_pair(sorted_key_freqs, key_freq_bucket_map);
    auto make_pair_end_time = std::chrono::high_resolution_clock::now();
    auto make_pair_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(make_pair_end_time - make_pair_time).count();
    
    info("Get key freq map time: {} microseconds", duration);
    info("Get num entries time: {} microseconds", get_num_duration);
    info("Insert time: {} microseconds", insert_duration);
    info("Sort time: {} microseconds", sort_duration);
    info("Total freq time: {} microseconds", total_freq_duration);
    info("Missing keys time: {} microseconds", missing_keys_duration);
    info("Total cum sum time: {} microseconds", total_cum_sum_duration);
    info("Make pair time: {} microseconds", make_pair_duration);
}

/*
std::vector<std::pair<uint64_t,std::string>> get_and_sort_freq(std::shared_ptr<BlockCache<std::string, std::string>>
cache)
{
    std::vector<std::pair<std::string, uint64_t>> key_freq = cache->get_cache()->get_key_freq_map();
    uint64_t total_keys = cache->get_cache()->get_block_db_num_entries();
    uint64_t total_freq = 0;

    // Calculate the total frequency
    for (auto &it : key_freq)
    {
        total_freq += it.second;
    }

    hdr_histogram *histogram;
    hdr_init(1, total_freq, 3, &histogram);

    // Insert key frequency pairs, swapping key and value for sorting purposes.
    for (auto &it : key_freq)
    {
        hdr_record_values(histogram, it.second, 1);
    }


    // Sort by frequency in descending order
    std::sort(key_freq.begin(), key_freq.end(), [](const auto &a, const auto &b) {
        return b.second < a.second;
    });

    // Calculate cumulative frequency and create CDF buckets
    std::vector<std::pair<uint64_t, std::string>> sorted_key_freq;
    std::map<uint64_t, std::vector<std::pair<uint64_t, std::string>>> cdf_buckets;
    uint64_t cumulative_freq = 0;

    for (auto &it : key_freq)
    {
        cumulative_freq += it.second;
        cdf_buckets[cumulative_freq].push_back(std::make_pair(it.second, it.first));
    }

    // Sort keys within each bucket in the desired order (alphabetically here as an example)
    for (auto &bucket : cdf_buckets)
    {
        auto &bucket_keys = bucket.second;
        std::sort(bucket_keys.begin(), bucket_keys.end(), [](const auto &a, const auto &b) {
            return a.second < b.second; // Change this to any desired sorting order
        });

        for (auto &it : bucket_keys)
        {
            sorted_key_freq.push_back(it);
        }
    }

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

        // Sort in descending order again to maintain order after adding missing keys
        std::sort(sorted_key_freq.begin(), sorted_key_freq.end(), std::greater<std::pair<uint64_t, std::string>>());
    }

    return sorted_key_freq;
}
*/
// Function to get and maintain keys under L
std::vector<std::string> get_keys_under_l(const std::vector<std::pair<uint64_t, std::string>>& cdf, uint64_t L) {
    std::vector<std::string> keys;
    for (size_t i = 0; i < L && i < cdf.size(); i++) {
        keys.push_back(cdf[i].second);
    }
    return keys;
}

uint64_t get_sum_freq_till_index(CDFType& cdf, uint64_t start, uint64_t end,
                                 std::map<uint64_t, uint64_t>& bucket_cumilative_freq) {
    uint64_t sum = 0;
    std::tuple<uint64_t, std::string, uint64_t> tmp;
    std::map<std::string, std::pair<uint64_t, uint64_t>> *key_freq_bucket_map = &std::get<1>(cdf);
    if (start >= std::get<0>(cdf).size()) {
        // info("Start index is greater than the size of CDF");
        start = std::get<0>(cdf).size() - 1;
    }
    if (end >= std::get<0>(cdf).size()) {
        // info("End index is greater than the size of CDF");
        end = std::get<0>(cdf).size() - 1;
    }

    uint64_t start_key_freq_sum = (*key_freq_bucket_map)[std::get<1>(std::get<0>(cdf)[start])].first;
    uint64_t end_key_freq_sum = (*key_freq_bucket_map)[std::get<1>(std::get<0>(cdf)[end])].first;
    // for (uint64_t i = start; i < end; i++)
    // {
    //     tmp = std::get<0>(cdf)[i];
    //     sum += std::get<0>(tmp);
    // }
    // return sum;
    return end_key_freq_sum - start_key_freq_sum;
}

void set_water_marks(std::shared_ptr<BlockCache<std::string, std::string>> cache, uint64_t water_mark_local,
                     uint64_t water_mark_remote) {
    cache->get_cache()->set_water_marks(water_mark_local, water_mark_remote);
}

uint64_t calculate_performance(CDFType& cdf, uint64_t water_mark_local, uint64_t water_mark_remote,
                               uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg,
                               std::map<uint64_t, uint64_t>& bucket_cumilative_freq) {
    uint64_t total_keys = std::get<0>(cdf).size();
    uint64_t total_local_accesses = get_sum_freq_till_index(cdf, 0, water_mark_local, bucket_cumilative_freq);
    uint64_t total_remote_accesses =
        get_sum_freq_till_index(cdf, water_mark_local, water_mark_remote, bucket_cumilative_freq);
    uint64_t total_disk_accesses =
        get_sum_freq_till_index(cdf, water_mark_remote, total_keys, bucket_cumilative_freq);
    uint64_t local_latency = total_local_accesses * cache_ns_avg;
    // uint64_t remote_latency = (((2 / 3) * total_remote_accesses) * rdma_ns_avg) + (((1/3)*(total_remote_accesses)) *
    // local_latency);
    uint64_t remote_latency = total_remote_accesses * rdma_ns_avg;
    uint64_t disk_latency = total_disk_accesses * disk_ns_avg;
    uint64_t perf_mul = -1;
    uint64_t performance = 0;
    if (local_latency + remote_latency + disk_latency != 0) {
        performance = perf_mul / (local_latency + remote_latency + disk_latency);
    }
    // std::cout << "Local latency: " << local_latency << ", Remote latency: " << remote_latency
    //           << ", Disk latency: " << disk_latency << ", Performance: " << performance << std::endl;
    // std::cout << "Total keys: " << total_keys << ", Total local accesses: " << total_local_accesses
    //           << ", Total remote accesses: " << total_remote_accesses
    //           << ", Total disk accesses: " << total_disk_accesses << std::endl;
    return performance;
}

size_t percentage_to_index(size_t total_size, float percent) {
    return static_cast<size_t>(total_size * (percent / 100.0));
}

void log_performance_state(uint64_t iteration, uint64_t L, uint64_t remote, uint64_t performance,
                           const std::string& message) {
    // std::cout << "Iteration: " << iteration
    //           << ", L: " << L
    //           << ", Remote: " << remote
    //           << ", Performance: " << performance
    //           << ", Message: " << message << std::endl << std::endl;
    info("Iteration: {}, L: {}, Remote: {}, Performance: {}, Message: {}", std::to_string(iteration), std::to_string(L),
         std::to_string(remote), std::to_string(performance), message);
}

void itr_through_all_the_perf_values_to_find_optimal(std::shared_ptr<BlockCache<std::string, std::string>> cache,
                                                     CDFType& cdf, uint64_t cache_ns_avg, uint64_t disk_ns_avg,
                                                     uint64_t rdma_ns_avg) {
    std::cout << "Calculating best access rates" << std::endl;
    std::tuple<uint64_t, uint64_t, uint64_t> water_marks = cache->get_cache()->get_water_marks();
    uint64_t cache_size = cache->get_cache()->get_cache_size();
    std::map<uint64_t, uint64_t> bucket_cumilative_freq = cache->get_cache()->get_bucket_cumulative_sum();
    uint64_t water_mark_local = std::get<0>(water_marks);
    uint64_t water_mark_remote = std::get<1>(water_marks);
    uint64_t performance = calculate_performance(cdf, water_mark_local, water_mark_remote, cache_ns_avg, disk_ns_avg,
                                                 rdma_ns_avg, bucket_cumilative_freq);
    uint64_t best_performance = performance;
    uint64_t best_water_mark_local = water_mark_local;
    uint64_t best_water_mark_remote = water_mark_remote;
    int remote = cache_size - (3 * water_mark_local);

    for (uint64_t i = 0; i < std::get<0>(cdf).size(); i++) {
        uint64_t local = i;
        remote = cache_size - (3 * local);
        if (remote < 0) {
            break;
        }
        if (local > cache_size / 3) {
            break;
        }
        uint64_t new_performance =
            calculate_performance(cdf, local, remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg, bucket_cumilative_freq);
        if (new_performance > best_performance) {
            best_performance = new_performance;
            best_water_mark_local = local;
            best_water_mark_remote = remote;
        }
        // log_performance_state(i, local, remote, new_performance, "");
    }
    // uint64_t best_access_rate = (std::get<0>(std::get<0>(cdf)[best_water_mark_local])) * 0.90;
    std::cout << "Best local: " << best_water_mark_local << ", Best remote: " << best_water_mark_remote
              << ", Best performance: " << best_performance << std::endl;
    
    // set_water_marks(cache, best_water_mark_local, best_water_mark_remote);
    // cache->get_cache()->set_access_rate(best_access_rate);
    set_water_marks(cache, best_water_mark_local, best_water_mark_remote);
    uint64_t best_access_rate = -1;
    uint64_t best_bucket = -1;
    uint64_t cutoff_key_id = -1;

    if (best_water_mark_local != 0) {
        best_access_rate = std::get<0>(std::get<0>(cdf)[best_water_mark_local]);
        best_bucket = std::get<2>(std::get<0>(cdf)[best_water_mark_local]);
        cutoff_key_id = std::stoi(std::get<1>(std::get<0>(cdf)[best_water_mark_local]));
    }
    cache->get_cache()->check_and_set_total_cache_duplication();
    cache->get_cache()->set_access_rate(best_access_rate);
    cache->get_cache()->set_bucket_id(best_bucket);
    cache->get_cache()->set_key_id_cutoff(cutoff_key_id);
    cache->get_cache()->set_perf_stats(best_water_mark_local, best_water_mark_remote, best_performance);
    cache->get_cache()->set_keys_from_past(std::get<0>(cdf));
    cache->get_cache()->print_all_stats();
}

void print_cdf(CDFType& cdf) {
    std::ofstream file;
    file.open("cdf.txt");
    for (auto& it : std::get<0>(cdf)) {
        file << std::get<0>(it) << " " << std::get<1>(it) << " " << std::get<2>(it) << std::endl;
    }
}

void write_latency_to_file(std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> latencies) {
    std::ofstream file;
    file.open("latencies.txt");
    for (auto& it : latencies) {
        file << std::get<0>(it) << " " << std::get<1>(it) << " " << std::get<2>(it) << std::endl;
    }
}

void get_best_access_rates(std::shared_ptr<BlockCache<std::string, std::string>> cache, CDFType& cdf,
                           uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg) {
    info("Calculating best access rates");
    print_cdf(cdf);
    info("cache_ns_avg: {}, disk_ns_avg: {}, rdma_ns_avg: {}", std::to_string(cache_ns_avg),
         std::to_string(disk_ns_avg), std::to_string(rdma_ns_avg));
    auto [initial_water_mark_local, initial_water_mark_remote, _] = cache->get_cache()->get_water_marks();
    info("Initial water mark local: {}, Initial water mark remote: {}", std::to_string(initial_water_mark_local),
         std::to_string(initial_water_mark_remote));
    uint64_t cache_size = cache->get_cache()->get_cache_size();
    std::map<uint64_t, uint64_t> bucket_cumilative_freq = cache->get_cache()->get_bucket_cumulative_sum();
    info("Cache size: {}", std::to_string(cache_size));
    uint64_t best_performance = calculate_performance(cdf, initial_water_mark_local, initial_water_mark_remote,
                                                      cache_ns_avg, disk_ns_avg, rdma_ns_avg, bucket_cumilative_freq);
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
    float performance__delta_threshold = 0.00;  // 0.5%

    do {
        improved_increasing = false;
        reduced_perf_increasing = false;

        improved_decreasing = false;
        reduced_perf_decreasing = false;

        // Test increasing L
        for (int i = 1; i <= 3; i++) {
            int new_local = best_local + percentage_to_index(std::get<0>(cdf).size(), 0.01 * i);
            int new_remote = cache_size - (new_local * 3);
            if (new_remote < 0) break;
            if (new_local > cache_size / 3) break;
            if (new_remote >= cache_size) break;

            uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg,
                                                             rdma_ns_avg, bucket_cumilative_freq);
            std::string message =
                "Increased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
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
            } else {
                if (new_performance < best_performance * (1 - performance__delta_threshold)) {
                    reduced_perf_increasing = true;
                }
            }
        }
        int new_local = best_local;
        // Test decreasing L
        if (!improved_increasing) {
            while (new_local > 0 && !reduced_perf_decreasing) {
                new_local = new_local - percentage_to_index(std::get<0>(cdf).size(), 0.01);
                int new_remote = cache_size - (new_local * 3);
                if (new_local < 0) break;
                if (new_remote >= cache_size) break;
                uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg,
                                                                 rdma_ns_avg, bucket_cumilative_freq);
                std::string message = "Decreased L to " + std::to_string(new_local) +
                                      ", new performance: " + std::to_string(new_performance);
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
                } else {
                    if (new_performance < best_performance * (1 - performance__delta_threshold)) {
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
        new_local = best_local + percentage_to_index(std::get<0>(cdf).size(), 1.0);

        while (new_remote < cache_size && !reduced_perf_increasing) {
            new_local = new_local + percentage_to_index(std::get<0>(cdf).size(), 1.0);
            new_remote = cache_size - (new_local * 3);
            ;
            if (new_remote >= cache_size) break;
            uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg,
                                                             rdma_ns_avg, bucket_cumilative_freq);
            std::string message = "increasing 1% Increased L to " + std::to_string(new_local) +
                                  ", new performance: " + std::to_string(new_performance);
            log_performance_state(iteration++, new_local, new_remote, new_performance, message);
            if (new_performance > best_performance * (1 + performance__delta_threshold)) {
                // std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                // std::cout << "old performance: " << best_performance << std::endl;
                best_performance = new_performance;
                best_local = new_local;
                best_remote = new_remote;
                improved_increasing = true;
                // std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                // std::cout << "best performance: " << best_performance << std::endl;
            } else {
                if (new_performance < best_performance * (1 - performance__delta_threshold)) {
                    reduced_perf_increasing = true;
                }
            }
            // std::cout<<"new_remote: "<<new_remote<<std::endl;
            info("new_local: {} , new_remote: {} , best_performance: {}", std::to_string(new_local),
                 std::to_string(new_remote), std::to_string(best_performance));
        }

        // If no improvement, test decreasing L by 1%
        if (best_local == initial_water_mark_local) {
            new_local = best_local - percentage_to_index(std::get<0>(cdf).size(), 1.0);

            while (new_local > 0 && reduced_perf_decreasing) {
                new_remote = cache_size - (new_local * 3);
                ;
                uint64_t new_performance = calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg,
                                                                 rdma_ns_avg, bucket_cumilative_freq);
                std::string message = "Decreased L to " + std::to_string(new_local) +
                                      ", new performance: " + std::to_string(new_performance);
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
                    if (new_performance < best_performance * (1 - performance__delta_threshold)) {
                        reduced_perf_decreasing = true;
                    }
                }
                new_local -= percentage_to_index(std::get<0>(cdf).size(), 1.0);
            }
            // std::cout<<"new_local: "<<new_local<<std::endl;
            info("new_local: {}, new_remote: {}, best_performance: {} ", std::to_string(new_local),
                 std::to_string(new_remote), std::to_string(best_performance));
        }
    }
    // std::cout << "Best local: " << best_local << ", Best remote: " << best_remote << ", Best performance: " <<
    // best_performance << std::endl;
    info("Best local: {} , Best remote: {} , Best performance: {}", std::to_string(best_local),
         std::to_string(best_remote), std::to_string(best_performance));

    // Set new optimized water marks and access rate
    set_water_marks(cache, best_local, best_remote);
    uint64_t best_access_rate = -1;
    uint64_t best_bucket = -1;
    uint64_t cutoff_key_id = -1;

    if (best_local != 0) {
        best_access_rate = std::get<0>(std::get<0>(cdf)[best_local]);
        best_bucket = std::get<2>(std::get<0>(cdf)[best_local]);
        cutoff_key_id = std::stoi(std::get<1>(std::get<0>(cdf)[best_local]));
    }
    cache->get_cache()->check_and_set_total_cache_duplication();
    cache->get_cache()->set_access_rate(best_access_rate);
    cache->get_cache()->set_bucket_id(best_bucket);
    cache->get_cache()->set_key_id_cutoff(cutoff_key_id);
    cache->get_cache()->set_perf_stats(best_local, best_remote, best_performance);
    cache->get_cache()->set_keys_from_past(std::get<0>(cdf));
    cache->get_cache()->print_all_stats();

    // // Get and set keys under L
    // std::vector<std::string> keys_under_l = get_keys_under_l(cdf, best_local);
    // cache->get_cache()->set_keys_under_l(keys_under_l);
}