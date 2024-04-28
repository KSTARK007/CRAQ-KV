#pragma once

// Includes
#include "globals.h"

// Third party
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include "concurrentqueue.h"
#include "unordered_dense.h"
#include <gflags/gflags.h>
#include <nlohmann/json.hpp>
#include "infinity/infinity.h"
#include <hdr/hdr_histogram.h>
#include <infiniband/verbs.h>

// Generated
#include "packet.capnp.h"

// Depends
#include <arpa/inet.h>
#include <netinet/in.h>

// Stdlib
#include <csignal>
#include <array>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <future>
#include <cstring>
#include <span>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <memory_resource>
#include <string_view>
#include <charconv>

// Alias
template <typename T, typename T2>
using HashMap = ankerl::unordered_dense::map<T, T2>;

template <typename T>
using MPMCQueue = moodycamel::ConcurrentQueue<T>;

using json = nlohmann::json;

inline void assert_with_msg(bool cond, const char *msg)
{
    if (!cond)
    {
        printf("%s\n", msg);
        exit(-1);
    }
}

[[maybe_unused]] inline bool bind_this_thread_to_core(uint8_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);       // Clear all CPUs
  CPU_SET(core, &cpuset);  // Set the requested core

  pthread_t current_thread = pthread_self();
  if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
    perror("Could not set thread to specified core");
    return false;
  }
  return true;
}

template<typename T>
inline T convert_string(std::string_view sv)
{
  T t{};
  auto result = std::from_chars(sv.data(), sv.data() + sv.size(), t);
  if (result.ec == std::errc::invalid_argument) {
    std::cout << "Could not convert " << sv << " to value" << std::endl;
  }
  return t;
}

struct KeyValueEntry
{
  std::string key;
  std::string value;
};