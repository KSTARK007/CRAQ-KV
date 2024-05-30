#include "ldc.h"

#include <set>

// std::vector<std::pair<uint64_t,std::string>> get_and_sort_freq(std::shared_ptr<BlockCache<std::string, std::string>> cache);
// std::vector<std::tuple<uint64_t, std::string, uint64_t>> get_and_sort_freq(std::shared_ptr<BlockCache<std::string, std::string>> cache);
// std::map<std::string, std::pair<uint64_t, u_int64_t>> get_and_sort_freq(std::shared_ptr<BlockCache<std::string, std::string>> cache);
CDFType get_and_sort_freq(std::shared_ptr<BlockCache<std::string, std::string>> cache);

// void get_best_access_rates(std::shared_ptr<BlockCache<std::string, std::string>> cache, std::vector<std::tuple<uint64_t, std::string, uint64_t>>& cdf, uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg);
void get_best_access_rates(std::shared_ptr<BlockCache<std::string, std::string>> cache, CDFType& cdf, uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg);
// void get_best_access_rates(std::shared_ptr<BlockCache<std::string, std::string>> cache, std::vector<std::pair<uint64_t,std::string>>& cdf, uint64_t cache_ns_avg, uint64_t disk_ns_avg, uint64_t rdma_ns_avg);