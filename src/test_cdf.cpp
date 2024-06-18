#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <string>
#include <vector>

std::vector<uint64_t> extract_keys(const std::string &filename) {
    std::vector<uint64_t> keys;
    auto total_keys = 100000000;
    std::ifstream file(filename);
    uint64_t key;
    char node;
    char op;

    while (file >> key >> node >> op && keys.size() < total_keys) {
        keys.push_back(key);
    }

    file.close();
    std::cout << "Total keys: " << keys.size() << std::endl;
    return keys;
}

// std::vector<std::pair<uint64_t, std::string>> sort_keys_by_frequency(const std::vector<uint64_t>& keys) {
//     std::map<uint64_t, uint64_t> frequency;
//     for (uint64_t key : keys) {
//         frequency[key]++;
//     }

//     std::vector<std::pair<uint64_t, std::string>> freq_vector;
//     for (auto& kv : frequency) {
//         freq_vector.emplace_back(static_cast<uint64_t>(kv.second), std::to_string(kv.first));
//     }

//     std::sort(freq_vector.begin(), freq_vector.end(), [](const std::pair<uint64_t, std::string>& a, const
//     std::pair<uint64_t, std::string>& b) {
//         return a.first > b.first; // Sort in descending order by frequency
//     });

//     return freq_vector;
// }

void write_vector_to_file(const std::vector<std::pair<uint64_t, std::string> > &vec, const std::string &filename) {
    std::ofstream file(filename);
    for (const auto &kv : vec) {
        file << kv.first << " " << kv.second << std::endl;
    }
    file.close();
}

// Function to write buckets to a file for debugging
void write_buckets_to_file(const std::map<uint64_t, std::vector<std::pair<uint64_t, std::string> > > &buckets,
                           const std::string &filename) {
    std::ofstream file(filename);
    for (const auto &bucket : buckets) {
        file << "Percentile: " << bucket.first << std::endl;
        for (const auto &kv : bucket.second) {
            file << kv.first << " " << kv.second << std::endl;
        }
        file << std::endl;
    }
    file.close();
}

void write_vectors_to_file(const std::vector<std::tuple<uint64_t, std::string, uint64_t> > &vec,
                           const std::string &filename) {
    std::ofstream file(filename);
    for (const auto &kv : vec) {
        file << std::get<0>(kv) << " " << std::get<1>(kv) << " " << std::get<2>(kv) << std::endl;
    }
    file.close();
}

std::vector<std::pair<uint64_t, std::string> > sort_keys_by_frequency(const std::vector<uint64_t> &keys) {
    // Calculate the frequency of each key
    std::map<uint64_t, uint64_t> frequency;
    for (uint64_t key : keys) {
        frequency[key]++;
    }

    // Create a vector of frequencies
    std::vector<std::pair<uint64_t, std::string> > freq_vector;
    for (auto &kv : frequency) {
        freq_vector.emplace_back(static_cast<uint64_t>(kv.second), std::to_string(kv.first));
    }

    // Sort the frequencies in descending order
    std::sort(freq_vector.begin(), freq_vector.end(),
              [](const std::pair<uint64_t, std::string> &a, const std::pair<uint64_t, std::string> &b) {
                  return a.first > b.first;  // Sort in descending order by frequency
              });

    // Create buckets at each percentile
    std::vector<std::pair<uint64_t, std::string> > sorted_key_freq;
    std::vector<std::tuple<uint64_t, std::string, uint64_t> > sorted_key_freqs;
    std::map<uint64_t, std::vector<std::pair<uint64_t, std::string> > > cdf_buckets;
    uint64_t total_freq = 0;

    for (const auto &kv : freq_vector) {
        total_freq += kv.first;
    }

    uint64_t cumulative_freq = 0;
    for (const auto &kv : freq_vector) {
        cumulative_freq += kv.first;
        uint64_t percentile = (cumulative_freq * 100) / total_freq;
        cdf_buckets[percentile].push_back(kv);
    }

    // Sort keys within each bucket in descending order
    for (auto &bucket : cdf_buckets) {
        auto &bucket_keys = bucket.second;
        std::sort(bucket_keys.begin(), bucket_keys.end(),
                  [](const auto &a, const auto &b) { return std::stoull(a.second) > std::stoull(b.second); });

        for (const auto &it : bucket_keys) {
            sorted_key_freq.push_back(it);
            sorted_key_freqs.push_back(std::make_tuple(it.first, it.second, bucket.first));
        }
    }

    // Write the frequency vector and buckets to files for debugging
    write_vector_to_file(freq_vector, "freq_vector.txt");
    write_buckets_to_file(cdf_buckets, "buckets.txt");
    write_vector_to_file(sorted_key_freq, "sorted_key_freq.txt");
    write_vectors_to_file(sorted_key_freqs, "sorted_key_freqs.txt");

    return sorted_key_freq;
}

void write_cdf_to_file(const std::vector<std::pair<uint64_t, std::string> > &freq_vector, const std::string &filename) {
    std::ofstream file(filename);
    uint64_t total = 0;
    for (const auto &kv : freq_vector) {
        total += kv.first;
    }

    uint64_t sum = 0;
    for (const auto &kv : freq_vector) {
        sum += kv.first;
        file << kv.first << " " << kv.second << " " << sum << " " << total << std::endl;
    }

    file.close();
}

std::vector<std::pair<uint64_t, std::string> > zipfian_workload() {
    auto keys = extract_keys("/mydata/ycsb_traces/zipfian/all_data.txt");
    std::cout << "Total keys: " << keys.size() << std::endl;
    auto freq_vector = sort_keys_by_frequency(keys);
    write_cdf_to_file(freq_vector, "zipfian_cdf.txt");
    return freq_vector;
}

std::vector<std::pair<uint64_t, std::string> > uniform_workload() {
    auto keys = extract_keys("/mydata/ycsb_traces/uniform/all_data.txt");
    std::cout << "Total keys: " << keys.size() << std::endl;
    auto freq_vector = sort_keys_by_frequency(keys);
    write_cdf_to_file(freq_vector, "uniform_cdf.txt");
    return freq_vector;
}

std::vector<std::pair<uint64_t, std::string> > hotspot_workload() {
    auto keys = extract_keys("/mydata/ycsb/hotspot_80_20/all_data.txt");
    std::cout << "Total keys: " << keys.size() << std::endl;
    auto freq_vector = sort_keys_by_frequency(keys);
    write_cdf_to_file(freq_vector, "hotspot_cdf.txt");
    return freq_vector;
}

uint64_t get_sum_freq_till_index(std::vector<std::pair<uint64_t, std::string> > &cdf, uint64_t start, uint64_t end) {
    static bool first = true;
    static std::vector<uint64_t> sum_freq;
    if (first) {
        std::cout << "Calculating sum_freq" << std::endl;
        sum_freq.resize(cdf.size());
        sum_freq[0] = cdf[0].first;
        for (uint64_t i = 1; i < cdf.size(); i++) {
            sum_freq[i] = sum_freq[i - 1] + cdf[i].first;
        }
        first = false;
    }

    // return sum without including end
    if (start == 0) {
        return sum_freq[end - 1];
    }
    return sum_freq[end - 1] - sum_freq[start - 1];
}

uint64_t calculate_latency(std::vector<std::pair<uint64_t, std::string> > &cdf, uint64_t water_mark_local,
                           uint64_t water_mark_remote, uint64_t cache_ns_avg, uint64_t disk_ns_avg,
                           uint64_t rdma_ns_avg) {
    uint64_t total_keys = cdf.size();
    uint64_t total_local_accesses = get_sum_freq_till_index(cdf, 0, water_mark_local);
    uint64_t total_remote_accesses =
        get_sum_freq_till_index(cdf, water_mark_local, water_mark_remote);
    uint64_t total_disk_accesses = get_sum_freq_till_index(cdf, water_mark_remote, total_keys);
    uint64_t local_latency = total_local_accesses * cache_ns_avg;
    uint64_t remote_latency = ((((2 * total_remote_accesses) / 3) * rdma_ns_avg) + ((total_remote_accesses / 3) *
    cache_ns_avg));
    // uint64_t remote_latency = total_remote_accesses * rdma_ns_avg;
    uint64_t disk_latency = total_disk_accesses * disk_ns_avg;
    return local_latency + remote_latency + disk_latency;
}

uint64_t calculate_performance(std::vector<std::pair<uint64_t, std::string> > &cdf, uint64_t water_mark_local,
                               uint64_t water_mark_remote, uint64_t cache_ns_avg, uint64_t disk_ns_avg,
                               uint64_t rdma_ns_avg) {
    uint64_t total_keys = cdf.size();
    uint64_t total_local_accesses = get_sum_freq_till_index(cdf, 0, water_mark_local);
    uint64_t total_remote_accesses =
        get_sum_freq_till_index(cdf, water_mark_local, water_mark_remote);
    uint64_t total_disk_accesses = get_sum_freq_till_index(cdf, water_mark_remote, total_keys);
    uint64_t local_latency = total_local_accesses * cache_ns_avg;
    uint64_t remote_latency = ((((2 * total_remote_accesses) / 3) * rdma_ns_avg) + ((total_remote_accesses / 3) *
    cache_ns_avg));
    // uint64_t remote_latency = total_remote_accesses * rdma_ns_avg;
    uint64_t disk_latency = total_disk_accesses * disk_ns_avg;
    uint64_t perf_mul = -1;
    uint64_t performance = perf_mul / (local_latency + remote_latency + disk_latency);
    // std::cout << "Local latency: " << local_latency << ", Remote latency: " << remote_latency
    // << ", Disk latency: " << disk_latency << ", Performance: " << performance << std::endl;
    // std::cout << "Total keys: " << total_keys << ", Total local accesses: " << total_local_accesses
    // << ", Total remote accesses: " << total_remote_accesses << ", Total disk accesses: " << total_disk_accesses
    // << std::endl;
    return performance;
}

size_t percentage_to_index(size_t total_size, float percent) {
    return static_cast<size_t>(total_size * (percent / 100.0));
}

void log_performance_state(uint64_t iteration, uint64_t L, uint64_t remote, uint64_t performance,
                           const std::string &message) {
    std::cout << "Iteration: " << iteration << ", L: " << L << ", Remote: " << remote
              << ", Performance: " << performance << ", Message: " << message << std::endl
              << std::endl;
}

// INCORRECT
void get_best_access_rate_faster(std::vector<std::pair<uint64_t, std::string> > &cdf, uint64_t cache_ns_avg,
                                 uint64_t disk_ns_avg, uint64_t rdma_ns_avg, uint64_t cache_size) {
    uint64_t best_local, best_remote, best_performance;

    // ensure that L is less than R
    uint64_t lim = std::min(cache_size / 4, static_cast<uint64_t>(cdf.size()));
    long double min = std::numeric_limits<long double>::max();

    // iterate over cdf
    for (uint64_t i = 0; i <= lim; i++) {
        auto value = std::abs(static_cast<long double>(3 * (disk_ns_avg - rdma_ns_avg) * cdf[cache_size - 3 * i].first -
                                                       (rdma_ns_avg - cache_ns_avg) * cdf[i].first));
        if (value < min) {
            min = value;
            best_local = i;
        }
    }

    best_remote = cache_size - 3 * best_local;
    best_performance = calculate_performance(cdf, best_local, best_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
    std::cout << get_sum_freq_till_index(cdf, best_local, best_remote) << std::endl;
    std::cout << get_sum_freq_till_index(cdf, best_remote, cdf.size()) << std::endl;
    std::cout << get_sum_freq_till_index(cdf, 0, best_local) << std::endl;
    
    std::cout << "Best local: " << best_local << ", Best remote: " << best_remote
              << ", Best performance: " << best_performance << std::endl;
}

void get_best_access_rate_even_faster(std::vector<std::pair<uint64_t, std::string> > &cdf, uint64_t cache_ns_avg,
                                      uint64_t disk_ns_avg, uint64_t rdma_ns_avg, uint64_t cache_size) {
    uint64_t best_local, best_remote, best_performance;

    // ensure that L is less than R
    uint64_t lim = std::min(cache_size / 4, static_cast<uint64_t>(cdf.size()));
    uint64_t min = std::numeric_limits<uint64_t>::max();

    std::fstream file;
    std::fstream file_;
    file.open("cdf_graph.txt", std::ios::out);
    file_.open("cdf_graph_.txt", std::ios::out);

    // iterate over cdf
    for (uint64_t i = 0; i <= lim; i++) {
        uint64_t value = calculate_latency(cdf, i, cache_size - 3 * i, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
        if (value < min) {
            min = value;
            best_local = i;
        }
        file << value << std::endl;
        uint64_t value_ = calculate_performance(cdf, i, cache_size - 3 * i, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
        file_ << value_ << std::endl;
    }

    best_remote = cache_size - 3 * best_local;
    best_performance = calculate_performance(cdf, best_local, best_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
    std::cout << "Best local: " << best_local << ", Best remote: " << best_remote
              << ", Best performance: " << best_performance << std::endl;
    std::cout << get_sum_freq_till_index(cdf, best_local, best_remote) << std::endl;
    std::cout << get_sum_freq_till_index(cdf, best_remote, cdf.size()) << std::endl;
    std::cout << get_sum_freq_till_index(cdf, 0, best_local) << std::endl;
    file.close();
    file_.close();
}

void get_best_access_rates(std::vector<std::pair<uint64_t, std::string> > &cdf, uint64_t cache_ns_avg,
                           uint64_t disk_ns_avg, uint64_t rdma_ns_avg, uint64_t cache_size) {
    uint64_t iteration = 0;
    auto initial_water_mark_local = 0.0;
    auto initial_water_mark_remote = cache_size;
    uint64_t best_performance = calculate_performance(cdf, initial_water_mark_local, initial_water_mark_remote,
                                                      cache_ns_avg, disk_ns_avg, rdma_ns_avg);

    uint64_t best_local = initial_water_mark_local;
    uint64_t best_remote = initial_water_mark_remote;

    // log_performance_state(iteration++, best_local, best_remote, best_performance, "Initial configuration");

    // Adjust L dynamically
    bool improved_increasing;
    bool improved_decreasing;
    bool reduced_perf_increasing;
    bool reduced_perf_decreasing;
    bool improved = false;
    float performance__delta_threshold = 0.000;  // 0.5%

    do {
        improved_increasing = false;
        reduced_perf_increasing = false;

        improved_decreasing = false;
        reduced_perf_decreasing = false;

        // Test increasing L
        for (int i = 1; i <= 3; i++) {
            int new_local = best_local + percentage_to_index(cdf.size(), 0.01 * i);
            int new_remote = cache_size - (new_local * 3);
            if (new_remote < 0) break;
            if (new_local > cache_size / 3) break;
            if (new_remote >= cache_size) break;

            uint64_t new_performance =
                calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
            std::string message =
                "Increased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
            // log_performance_state(iteration++, new_local, new_remote, new_performance, message);

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
                new_local = new_local - percentage_to_index(cdf.size(), 0.01);
                int new_remote = cache_size - (new_local * 3);
                if (new_local < 0) break;
                if (new_remote >= cache_size) break;
                uint64_t new_performance =
                    calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
                std::string message = "Decreased L to " + std::to_string(new_local) +
                                      ", new performance: " + std::to_string(new_performance);
                // log_performance_state(iteration++, new_local, new_remote, new_performance, message);

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
        uint64_t new_remote = cache_size - (best_local * 3);
        uint64_t new_local = best_local;

        bool improved_increasing = false;
        bool reduced_perf_increasing = false;
        bool improved_decreasing = false;
        bool reduced_perf_decreasing = false;
        new_local = best_local + percentage_to_index(cdf.size(), 1.0);

        while (new_remote < cache_size && !reduced_perf_increasing) {
            new_local = new_local + percentage_to_index(cdf.size(), 1.0);
            new_remote = cache_size - (new_local * 3);
            ;
            if (new_remote >= cache_size) break;
            uint64_t new_performance =
                calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
            std::string message =
                "Increased L to " + std::to_string(new_local) + ", new performance: " + std::to_string(new_performance);
            // log_performance_state(iteration++, new_local, new_remote, new_performance, message);
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
            // std::cout << "new_remote: " << new_remote << std::endl;
        }

        // If no improvement, test decreasing L by 1%
        if (best_local == initial_water_mark_local) {
            new_local = best_local - percentage_to_index(cdf.size(), 1.0);

            while (new_local > 0 && reduced_perf_decreasing) {
                new_remote = cache_size - (new_local * 3);
                ;
                uint64_t new_performance =
                    calculate_performance(cdf, new_local, new_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
                std::string message = "Decreased L to " + std::to_string(new_local) +
                                      ", new performance: " + std::to_string(new_performance);
                // log_performance_state(iteration++, new_local, new_remote, new_performance, message);

                if (new_performance > best_performance * (1 + performance__delta_threshold)) {
                    // std::cout << "old local: " << best_local << ", old remote: " << best_remote << std::endl;
                    // std::cout << "old performance: " << best_performance << std::endl;
                    best_performance = new_performance;
                    best_local = new_local;
                    best_remote = new_remote;
                    improved_increasing = true;
                    // std::cout << "best local: " << best_local << ", best remote: " << best_remote << std::endl;
                    // std::cout << "best performance: " << best_performance << std::endl;
                    break;
                } else {
                    if (new_performance < best_performance * (1 - performance__delta_threshold)) {
                        reduced_perf_decreasing = true;
                    }
                }

                new_local -= percentage_to_index(cdf.size(), 1.0);
            }
            // std::cout << "new_local: " << new_local << std::endl;
        }
    }
    std::cout << "Best local: " << best_local << ", Best remote: " << best_remote
              << ", Best performance: " << best_performance << std::endl;
    // Set new optimized water marks and access rate
    // set_water_marks(cache, best_local, best_remote);
    // uint64_t best_access_rate = (cdf[best_local].first) * 0.90;
    // cache->get_cache()->set_access_rate(best_access_rate);

    // // Get and set keys under L
    // std::vector<std::string> keys_under_l = get_keys_under_l(cdf, best_local);
    // cache->get_cache()->set_keys_under_l(keys_under_l);
}

void itr_through_all_the_perf_values_to_find_optimal(std::vector<std::pair<uint64_t, std::string> > cdf,
                                                     uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg,
                                                     uint64_t cache_size) {
    uint64_t water_mark_local = 0;
    uint64_t water_mark_remote = cache_size;
    uint64_t performance =
        calculate_performance(cdf, water_mark_local, water_mark_remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
    uint64_t best_performance = performance;
    uint64_t best_water_mark_local = water_mark_local;
    uint64_t best_water_mark_remote = water_mark_remote;
    int remote = cache_size - (3 * water_mark_local);

    for (uint64_t i = 0; i < cdf.size(); i += 10) {
        uint64_t local = i;
        remote = cache_size - (3 * local);
        if (remote < 0) {
            break;
        }
        if (local > cache_size / 3) {
            break;
        }
        uint64_t new_performance = calculate_performance(cdf, local, remote, cache_ns_avg, disk_ns_avg, rdma_ns_avg);
        if (new_performance > best_performance) {
            best_performance = new_performance;
            best_water_mark_local = local;
            best_water_mark_remote = remote;
        }
        log_performance_state(i, local, remote, new_performance, "");
    }
    uint64_t best_access_rate = (cdf[best_water_mark_local].first) * 0.90;
    std::cout << "Best local: " << best_water_mark_local << ", Best remote: " << best_water_mark_remote
              << ", Best performance: " << best_performance << std::endl;

    // set_water_marks(cache, best_water_mark_local, best_water_mark_remote);
    // cache->get_cache()->set_access_rate(best_access_rate);
}

int main() {
    auto disk_latency = 20000;
    auto cache_latency = 1;
    auto rdma_latency = 8;
    auto cache_size = 10000000;

    // auto zipfian_cdf = zipfian_workload();
    // std::ifstream filez("zipfian_cdf.txt");
    uint64_t freq;
    uint64_t key;
    uint64_t sum;
    uint64_t data_size;
    // std::vector<std::pair<uint64_t, std::string>> zipfian_cdf;

    // while (filez >> freq >> key >> sum >> data_size){
    //     zipfian_cdf.push_back(std::make_pair(freq, std::to_string(key)));
    // }
    // filez.close();

    // std::cout << "Zipfian" << std::endl<<std::endl;
    // // get_best_access_rates(zipfian_cdf, cache_latency, disk_latency, rdma_latency, cache_size);
    // // itr_through_all_the_perf_values_to_find_optimal(zipfian_cdf, cache_latency, disk_latency, rdma_latency,
    // cache_size);
    // // std::cout << get_sum_freq_till_index(zipfian_cdf, 14400, 16800) << std::endl;
    // // for( auto i = 0; i < 100; i++)
    // // {
    // //     std::cout << zipfian_cdf[i].first << " " << zipfian_cdf[i].second << std::endl;
    // // }

    // std::ifstream fileu("uniform_cdf.txt");
    // std::vector<std::pair<uint64_t, std::string>> uniform_cdf;

    // while (fileu >> freq >> key >> sum >> data_size){
    //     uniform_cdf.push_back(std::make_pair(freq, std::to_string(key)));
    // }
    // fileu.close();

    // std::cout << "Uniform" << std::endl<<std::endl;
    // get_best_access_rates(uniform_cdf, cache_latency, disk_latency, rdma_latency, cache_size);
    // itr_through_all_the_perf_values_to_find_optimal(uniform_cdf, cache_latency, disk_latency, rdma_latency,
    // cache_size);

    // auto hotspot_cdfs = hotspot_workload();
    // std::ifstream fileh("hotspot_cdf.txt");

    // std::ifstream fileh("/mydata/ycsb/zipfian_0.99/unique_counts_reversed.txt");
    std::ifstream fileh("/mnt/sda4/LDC/setup/results/access_rate_dynamic::C_zipfian_YCSB_3340000_ns3_nc3_ncpt2_nt8_rd0_ht48_rdmaTrue_asyncTrue_diskTrue_polluteTrue_access_rate30000000/shadow_freq_3.txt");
    std::vector<std::pair<uint64_t, std::string> > cdf;

    // while (fileh >> freq >> key >> sum >> data_size){
    //     hotspot_cdf.push_back(std::make_pair(freq, std::to_string(key)));
    // }
    std::string line;
    std::cout << "Reading file" << std::endl;
    while (std::getline(fileh, line)) {
        // std::string key = line.substr(0, line.find(":"));
        // std::string freq = line.substr(line.find(":") + 1);
        std::string key = line.substr(0, line.find(" "));
        std::string freq = line.substr(line.find(" ") + 1);
        cdf.push_back(std::make_pair(std::stoull(freq), key));
    }

    std::cout << "Zipfian" << std::endl << std::endl;
    auto t1 = std::chrono::high_resolution_clock::now();
    get_best_access_rates(cdf, cache_latency, disk_latency, rdma_latency, cache_size);
    // auto t2 = std::chrono::high_resolution_clock::now();
    // get_best_access_rate_faster(cdf, cache_latency, disk_latency, rdma_latency, cache_size);
    auto t3 = std::chrono::high_resolution_clock::now();
    get_best_access_rate_even_faster(cdf, cache_latency, disk_latency, rdma_latency, cache_size);
    auto t4 = std::chrono::high_resolution_clock::now();
    std::cout << "Time taken for get_best_access_rates: "
              << std::chrono::duration_cast<std::chrono::microseconds>(t3 - t1).count() << " microseconds" << std::endl;
    // std::cout << "Time taken for get_best_access_rate_faster: "
    //           << std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count() << " microseconds" <<
    //           std::endl;
    std::cout << "Time taken for get_best_access_rate_even_faster: "
              << std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count() << " microseconds" << std::endl;
    // itr_through_all_the_perf_values_to_find_optimal(hotspot_cdf, cache_latency, disk_latency, rdma_latency,
    // cache_size);
    return 0;
}