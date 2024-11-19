#pragma once

// Includes
#include "globals.h"

// Third party
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include "concurrentqueue.h"
#include "readerwriterqueue.h"
#include "unordered_dense.h"
#include <gflags/gflags.h>
#include <nlohmann/json.hpp>
#include "infinity/infinity.h"
#include <hdr/hdr_histogram.h>
#include <infiniband/verbs.h>
#include <parallel_hashmap/phmap.h>

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
#include <execution>

using namespace std::chrono_literals;

// Alias
template <typename T, typename T2>
using HashMap = ankerl::unordered_dense::map<T, T2>;

template <typename T>
using HashSet = ankerl::unordered_dense::set<T>;

template <typename T, typename T2>
using ParallelFlatHashMap = phmap::parallel_flat_hash_map<T, T2>;

template <typename T>
using MPMCQueue = moodycamel::ConcurrentQueue<T>;

template <typename T>
using SPSCQueue = moodycamel::ReaderWriterQueue<T>;

template<typename T>
using SingleProducerSingleConsumerQueue = moodycamel::ReaderWriterQueue<T>;

template<typename T>
using MultiProducerSingleConsumerQueue = moodycamel::ConcurrentQueue<T>;

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

template<typename T>
inline std::optional<T> convert_to_string_opt(std::string_view sv)
{
  T t{};
  auto result = std::from_chars(sv.data(), sv.data() + sv.size(), t);
  if (result.ec == std::errc::invalid_argument) {
    return std::nullopt;
  }
  return t;
}

struct KeyValueEntry
{
  std::string key;
  std::string value;
};


struct RotatingVectorHandle
{
    std::size_t index;
};

struct RowColumnIndex
{
    std::size_t row;
    std::size_t column;
};

template<typename T>
struct RotatingVectorEntry
{
    T t;
    bool valid = false;
};

template<typename T, std::size_t N = 1, std::size_t ROWS = 4, bool INITIALIZE = false>
class RotatingVector2
{
public:
    explicit RotatingVector2()
    {
        static_assert(N > 0, "N must be greater than 0");
        static_assert(ROWS > 0, "ROWS must be greater than 0");

        if constexpr(INITIALIZE)
        {
            std::for_each(std::execution::par_unseq, std::begin(data), std::end(data), [](auto& row)
            {
                std::for_each(std::execution::par_unseq, std::begin(row), std::end(row), [](auto& entry)
                {
                    entry.t = T{};
                    entry.valid = false;
                });
            });
        }
        else
        {
            std::for_each(std::execution::par_unseq, std::begin(data), std::end(data), [](auto& row)
            {
                std::for_each(std::execution::par_unseq, std::begin(row), std::end(row), [](auto& entry)
                {
                    entry.t = std::nullopt;
                    entry.valid = false;
                });
            });
        }
    }

    inline RowColumnIndex GetRowColumnIndex(std::size_t index) const
    {
        return RowColumnIndex{(index / N) % ROWS, index % N};
    }

    bool Insert(std::size_t index, T&& value)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        if (entry || valid)
        {
            return false;
        }
        entry = std::forward<T>(value);
        valid = true;
        return true;
    }

    void InsertNoCheck(std::size_t index, T&& value)
    {
        if (!Insert(index, std::forward<T>(value)))
        {
            std::cerr << ("RotatingVector: Insertstd::cerr <<  failed");
        }
    }

    T& InsertNoAllocation(std::size_t index)
    {
        // Retry...
        while (true)
        {
            auto [row, column] = GetRowColumnIndex(index);
            auto& [entry, valid] = data[row][column];
            if (valid)
            {
                // warning("RotatingVector: InsertNoAllocation failed");
                std::this_thread::sleep_for(10ms);
                continue;
            }
            if (!entry)
            {
                // warning("RotatingVector: InsertNoAllocation failed -- entry invalid");
                std::this_thread::sleep_for(10ms);
                continue;
            }
            valid = true;
            return *entry;
        }

        // auto [row, column] = GetRowColumnIndex(index);
        // auto& [entry, valid] = data[row][column];
        // if (valid)
        // {
        //     std::cerr << ("RotatingVector: InsertNoAllocation failed");
        // }
        // if (!entry)
        // {
        //     std::cerr << ("RotatingVector: InsertNoAllocation failed -- entry invalid");
        // }
        // valid = true;
        // return *entry;
    }

    bool InsertUnsafe(std::size_t index, const T& value)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        if (entry || valid)
        {
            return false;
        }
        entry = std::move(value);
        valid = true;
        return true;
    }

    RotatingVectorEntry<std::optional<T>>& GetEntry2(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& entry = data[row][column];
        return entry;
    }

    const RotatingVectorEntry<std::optional<T>>& GetEntry2(std::size_t index) const
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& entry = data[row][column];
        return entry;
    }

    const std::optional<T>& GetEntry(std::size_t index) const
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        return entry;
    }

    std::optional<T>& GetEntry(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        return entry;
    }

    T& Get(std::size_t index)
    {
        auto& entry = GetEntry(index);
        if (!entry)
        {
            std::cerr << ("RotatingVector: Get failed");
        }
        return *entry;
    }

    const T& Get(std::size_t index) const
    {
        const auto& entry = GetEntry(index);
        if (!entry)
        {
            std::cerr << ("RotatingVector: Get failed");
        }
        return *entry;
    }

    T& WaitGet(std::size_t index)
    {
        auto& entry = GetEntry(index);
        while (!entry)
        {
          std::cerr << ("RotatingVector: Get failed");
        }
        return *entry;
    }

    void Delete(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        entry = std::nullopt;
        valid = false;
    }

    void DeleteWithoutDeallocating(std::size_t index)
    {
        auto [row, column] = GetRowColumnIndex(index);
        auto& [entry, valid] = data[row][column];
        // entry = std::nullopt;
        valid = false;
    }

    bool empty() const
    {
        return false;
    }

private:
    std::array<std::array<RotatingVectorEntry<std::optional<T>>, N>, ROWS> data;
};

template<typename T, typename QueueT = SingleProducerSingleConsumerQueue<T>>
struct ExecutionQueue
{
    void set_num_queues(std::size_t num)
    {
        execution_queues.resize(num);
        num_queues = num;
    }

    std::size_t get_num_queues() const
    {
        return num_queues;
    }

    void send_data_to_queue(std::size_t index, T data)
    {
        execution_queues[index].enqueue(data);
    }

    std::optional<T> pull_data_from_queue(std::size_t index)
    {
        T data;
        if (execution_queues[index].try_dequeue(data))
        {
            return data;
        }
        else
        {
            return std::nullopt;
        }
    }

    T pull_data_from_queue_blocking(std::size_t index)
    {
        while (true)
        {
            std::optional<T> data = pull_data_from_queue(index);
            if (data)
            {
                return *data;
            }
            else
            {
                std::this_thread::yield();
            }
        }
    }
    
    void send_data_to_queue(std::string_view key, T data)
    {
        auto index = hash_to_queue(key);
        send_data_to_queue(index, data);
    }

    uint64_t hash_to_queue(std::string_view key)
    {
        auto queueIndex = 0;
        constexpr uint64_t hash = 5381;
        for (const auto& c : key) {
            queueIndex = ((queueIndex << 5) + queueIndex) + c;
        }
        queueIndex %= num_queues;
        return queueIndex;
    }

private:
    std::size_t num_queues = 0;
    std::vector<QueueT> execution_queues;    
};

using DurIndex = uint64_t;

template<typename T>
struct DurIndexAndMessage
{
    DurIndex index;
    T message;
};

constexpr auto BACKGROUND_HANDLE_MESSAGE_LIMIT = 128;
template<typename T, std::size_t DURABILITY_MSG_QUEUE_PREALLOCATION_COLUMNS = 50, std::size_t DURABILITY_MSG_QUEUE_PREALLOCATION_ROWS = 1000>
class DurabilityAndExecutionContext
{
public:
    explicit DurabilityAndExecutionContext()
    {
        durable_background_consuming_thread = std::thread([&]()
        {
            while (!stop)
            {
                auto i = 0;
                const auto message_limit = BACKGROUND_HANDLE_MESSAGE_LIMIT;
                while (i < message_limit)
                {
                    const auto& [entry, valid] = message_vector.GetEntry2(message_index);
                    if (!entry.has_value() || !valid)
                    {
                        break;
                    }
                    const auto& [index, message] = *entry;
                    if (index != message_index)
                    {
                        // panic("Indices not matching - %d != %d", index, message_index);
                    }
                    // Handle function here
                    // HandleRequestBg(message);
                    message_vector.Delete(message_index);
                    message_index += 1;
                }
            }
        });
    }

    ~DurabilityAndExecutionContext()
    {
        stop = true;
        durable_background_consuming_thread.join();
    }

    void durable_insert(DurIndex dur_index, T message)
    {
        auto dur_index_and_message = DurIndexAndMessage{dur_index, message};
        while (!message_vector.InsertUnsafe(dur_index, dur_index_and_message))
        {
            // info("Message vector is full... waiting for background thread to consume messages");
            std::this_thread::yield();
        }
    }

private:
    bool stop = false;
    RotatingVector2<DurIndexAndMessage<T>, DURABILITY_MSG_QUEUE_PREALLOCATION_COLUMNS, DURABILITY_MSG_QUEUE_PREALLOCATION_ROWS> message_vector;
    std::thread durable_background_consuming_thread;

	// To keep track of where end starts, so when popping from message queue adds to this end
	DurIndex messages_end = 0;

	// Keep track of what message index are we at (keeps track of what message index to expect next)
	DurIndex message_index = 0;
};
