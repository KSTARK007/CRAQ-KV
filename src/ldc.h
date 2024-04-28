#pragma once

#include "defines.h"

#include "operations.h"
#include "heap.h"
#include "async_rdma.h"

struct SnapshotEntry
{
  uint64_t key_index;
  uint64_t total_accesses;
  uint64_t cache_hits;
  uint64_t cache_miss;
  uint64_t evicted;
  uint64_t disk_access;
  uint64_t local_disk_access;
  uint64_t remote_disk_access;
  uint64_t access_rate;
};

inline void to_json(json& j, const SnapshotEntry& e)
{
  j = json{
    { "key_index", e.key_index },
    { "total_accesses", e.total_accesses },
    { "cache_hits", e.cache_hits },
    { "cache_miss", e.cache_miss },
    { "evicted", e.evicted },
    { "disk_access", e.disk_access },
    { "local_disk_access", e.local_disk_access },
    { "remote_disk_access", e.remote_disk_access },
    { "access_rate", e.access_rate }
  };
}

struct Snapshot
{
  explicit Snapshot(BlockCacheConfig block_cache_config_, Configuration ops_config_) :
    block_cache_config(block_cache_config_), ops_config(ops_config_)
  {
    if (!enabled())
    {
      return;
    }
    entries.resize(ops_config.NUM_KEY_VALUE_PAIRS);
    for (auto i = 0; i < entries.size(); i++)
    {
      entries[i].key_index = i;
    }

    // Background thread
    static std::thread background_thread([this]()
    {
      while (!g_stop)
      {
        this->dump();
        std::this_thread::sleep_for(std::chrono::milliseconds(ops_config.dump_snapshot_period_ms));        
      }
    });
    background_thread.detach();
  }

  bool enabled() const
  {
    return ops_config.dump_snapshot_period_ms != 0;
  }

  SnapshotEntry& get_entry(uint64_t key)
  {
    auto i = key;
    if (i > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] Getting entry out of bounds {} > {}", i, ops_config.NUM_KEY_VALUE_PAIRS);
    }
    return entries[i];
  }

  void update_total_access(uint64_t key)
  {
    if (!enabled()) return;
    if (key > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] [update_total_access] Getting entry out of bounds {} > {}", key, ops_config.NUM_KEY_VALUE_PAIRS);
    }
    auto& e = get_entry(key);
    e.total_accesses++;
  }

  void update_cache_hits(uint64_t key)
  {
    if (!enabled()) return;
    if (key > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] [update_cache_hits] Getting entry out of bounds {} > {}", key, ops_config.NUM_KEY_VALUE_PAIRS);
    }

    auto& e = get_entry(key);
    e.cache_hits++;
  }

  void update_cache_miss(uint64_t key)
  {
    if (!enabled()) return;
    if (key > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] [update_cache_miss] Getting entry out of bounds {} > {}", key, ops_config.NUM_KEY_VALUE_PAIRS);
    }

    auto& e = get_entry(key);
    e.cache_miss++;
  }

  void update_evicted(uint64_t key)
  {
    if (!enabled()) return;
    if (key > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] [update_evicted] Getting entry out of bounds {} > {}", key, ops_config.NUM_KEY_VALUE_PAIRS);
    }

    auto& e = get_entry(key);
    e.evicted++;
  }

  void update_disk_access(uint64_t key)
  {
    if (!enabled()) return;
    if (key > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] [update_disk_access] Getting entry out of bounds {} > {}", key, ops_config.NUM_KEY_VALUE_PAIRS);
    }

    auto& e = get_entry(key);
    e.disk_access++;
  }

  void update_local_disk_access(uint64_t key)
  {
    if (!enabled()) return;
    if (key > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] [update_local_disk_access] Getting entry out of bounds {} > {}", key, ops_config.NUM_KEY_VALUE_PAIRS);
    }

    auto& e = get_entry(key);
    e.local_disk_access++;
  }

  void update_remote_disk_access(uint64_t key)
  {
    if (!enabled()) return;
    if (key > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] [update_remote_disk_access] Getting entry out of bounds {} > {}", key, ops_config.NUM_KEY_VALUE_PAIRS);
    }

    auto& e = get_entry(key);
    e.remote_disk_access++;
  }

  void update_access_rate(uint64_t key)
  {
    if (!enabled()) return;
    if (key > ops_config.NUM_KEY_VALUE_PAIRS)
    {
      panic("[SnapshotEntry] [update_access_rate] Getting entry out of bounds {} > {}", key, ops_config.NUM_KEY_VALUE_PAIRS);
    }

    auto& e = get_entry(key);
    e.access_rate++;
  }

  void dump()
  {
    auto p = ops_config.dump_snapshot_file + std::to_string(index);
    std::ofstream o(p, std::ios::trunc);
    if (!o)
    {
      panic("Cannot open path to {}", p);
    }

    index++;
    json j;
    for (const auto& e : entries)
    {
      j.push_back(e);
    }
    o << j << std::endl;
  }

private:
  BlockCacheConfig block_cache_config;
  Configuration ops_config;
  std::vector<SnapshotEntry> entries;

  CopyableAtomic<uint64_t> index;
};

extern std::shared_ptr<Snapshot> snapshot;

#define CACHE_INDEX_LOG_PORT 50100
#define KEY_VALUE_STORAGE_PORT 50200
#define CACHE_INDEXES_PORT 50300

constexpr auto RDMA_CACHE_INDEX_KEY_VALUE_SIZE = 100;
constexpr auto FreeRequestTokenQueueSize = 50000;
constexpr auto CacheIndexValueQueueSize = 50000;
constexpr auto RDMA_SETUP_SYNC_SLEEP_MS = 1000;

struct RDMABufferAndToken
{
  infinity::memory::Buffer* buffer;
  infinity::memory::RegionToken* region_token;
};

struct RDMAData
{
  RDMAData(BlockCacheConfig block_cache_config_, Configuration ops_config_, int machine_index_, infinity::core::Context *context_, infinity::queues::QueuePairFactory* qp_factory_) :
    block_cache_config(block_cache_config_), ops_config(ops_config_), machine_index(machine_index_), context(context_), qp_factory(qp_factory_)
  {
    for (auto i = 0; i < block_cache_config.remote_machine_configs.size(); i++)
    {
      auto remote_machine_config = block_cache_config.remote_machine_configs[i];
      if (remote_machine_config.server)
      {
        server_configs.push_back(remote_machine_config);
      }
    }

    for (auto i = 0; i < server_configs.size(); i++)
    {
      if (i == machine_index)
      {
        my_server_config = server_configs[i];
        break;
      }
    }

    for (auto i = 0; i < FreeRequestTokenQueueSize; i++)
    {
      auto request_token = std::make_shared<infinity::requests::RequestToken>(context);
      free_request_token_queue.enqueue(request_token);
    }
  }

  void listen(int port, void* buffer, uint64_t size)
  {
    auto& [read_write_buffer, region_token] = get_buffer(buffer, size);
		region_token = read_write_buffer->createRegionToken();

    LOG_RDMA_DATA("[RDMAData] Listening on port [{}:{}]", my_server_config.ip, port);
    infinity::queues::QueuePairFactory* qpf = new infinity::queues::QueuePairFactory(context);
    qpfs.emplace_back(qpf);
    qpf->bindToPort(port);
    LOG_RDMA_DATA("[RDMAData] Listening on port [{}:{}] bound", my_server_config.ip, port);
    for (int i = 0; i < server_configs.size(); i++)
    {
      LOG_RDMA_DATA("[RDMAData] Accepting incoming connection on port [{}]", port);
      start_accepting_connections = true;
  		infinity::queues::QueuePair* qp = qpf->acceptIncomingConnection(region_token, sizeof(infinity::memory::RegionToken));
      LOG_RDMA_DATA("[RDMAData] Accepted incoming connection on port [{}:{}]", my_server_config.ip, port);
      listen_qps.emplace_back(qp);
    }
    is_server = true;
  }

  void connect(int port)
  {
    for (auto i = 0; i < server_configs.size(); i++)
    {
      // infinity::queues::QueuePairFactory* qpf = new infinity::queues::QueuePairFactory(context);
      // qpfs.emplace_back(qpf);

      auto server_config = server_configs[i];
      LOG_RDMA_DATA("[RDMAData] Connecting to remote machine [{}:{}]", server_config.ip, port);
      infinity::queues::QueuePair* qp = qp_factory->connectToRemoteHost(server_config.ip.c_str(), port);
      LOG_RDMA_DATA("[RDMAData] Connected to remote machine [{}:{}]", server_config.ip, port);
      connect_qps.emplace_back(qp);
    }
    is_server = false;
  }

  std::shared_ptr<infinity::requests::RequestToken> get_request_token()
  {
    std::shared_ptr<infinity::requests::RequestToken> request_token;
    while (!free_request_token_queue.try_dequeue(request_token))
    {
      panic("Free request token queue out of tokens!");
    }
    request_token->reset();
    return request_token;
  }

  void free_request_token(std::shared_ptr<infinity::requests::RequestToken> request_token)
  {
    free_request_token_queue.enqueue(request_token);
  }

  std::shared_ptr<infinity::requests::RequestToken> read(int remote_index, void* buffer, uint64_t buffer_size, uint64_t local_offset, uint64_t remote_offset, uint64_t size_in_bytes)
  {
    auto& [read_write_buffer, _] = get_buffer(buffer, buffer_size);

    auto& qps = connect_qps;
    LOG_RDMA_DATA("[RDMAData] Read from remote machine [{}/{}] Local {} Remote {} Size {}", remote_index, qps.size(), local_offset, remote_offset, size_in_bytes);
    auto qp = qps[remote_index];
    auto region_token = (infinity::memory::RegionToken *)qp->getUserData();
    std::shared_ptr<infinity::requests::RequestToken> request_token = get_request_token();
    qp->read(read_write_buffer, local_offset, region_token, remote_offset, size_in_bytes, infinity::queues::OperationFlags(), request_token.get());
    return request_token;
  }

  std::shared_ptr<infinity::requests::RequestToken> write(int remote_index, void* buffer, uint64_t buffer_size, uint64_t local_offset, uint64_t remote_offset, uint64_t size_in_bytes)
  {
    auto& [read_write_buffer, _] = get_buffer(buffer, buffer_size);

    auto& qps = connect_qps;
    LOG_RDMA_DATA("[RDMAData] Writing to remote machine [{}/{}] Local {} Remote {} Size {}", remote_index, qps.size(), local_offset, remote_offset, size_in_bytes);
    auto qp = qps[remote_index];
    auto region_token = (infinity::memory::RegionToken *)qp->getUserData();
    std::shared_ptr<infinity::requests::RequestToken> request_token = get_request_token();
    qp->write(read_write_buffer, local_offset, region_token, remote_offset, size_in_bytes, infinity::queues::OperationFlags(), request_token.get());
    return request_token;
  }

  RDMABufferAndToken& get_buffer(void* buffer, uint64_t size)
  {
    auto rdma_buffer_and_token = buffer_map.find(buffer);
    if (rdma_buffer_and_token != std::end(buffer_map))
    {
      return rdma_buffer_and_token->second;
    }
    else
    {
      auto read_write_buffer = new infinity::memory::Buffer(context, buffer, size);
      auto [new_buffer, inserted] = buffer_map.insert({buffer, {read_write_buffer, nullptr}});
      return new_buffer->second;
    }
  }

  BlockCacheConfig block_cache_config;
  Configuration ops_config;
  int machine_index;
  infinity::core::Context *context;
  infinity::queues::QueuePairFactory *qp_factory;
  std::vector<RemoteMachineConfig> server_configs;
  RemoteMachineConfig my_server_config;
  void* buffer{};
  uint64_t size{};
  
  HashMap<void*, RDMABufferAndToken> buffer_map;
  std::vector<infinity::queues::QueuePairFactory*> qpfs;
  std::vector<infinity::queues::QueuePair*> listen_qps;
  std::vector<infinity::queues::QueuePair*> connect_qps;
  std::vector<infinity::queues::QueuePair*> qps;
  bool is_server;
  bool start_accepting_connections = false;
  MPMCQueue<std::shared_ptr<infinity::requests::RequestToken>> free_request_token_queue;
};

template<typename T>
using DataWithRequestCallback = std::function<void(int, const T&)>;

template<typename T>
struct RDMADataWithQueue : public RDMAData
{
  RDMADataWithQueue(BlockCacheConfig block_cache_config, Configuration ops_config, int machine_index, infinity::core::Context *context, infinity::queues::QueuePairFactory* qp_factory, uint64_t queue_size) :
    RDMAData(block_cache_config, ops_config, machine_index, context, qp_factory)
  {
    for (auto i = 0; i < queue_size; i++)
    {
      auto data = std::make_shared<T>();
      DataWithRequestToken data_with_token{std::move(data), nullptr};
      get_buffer(data_with_token.data.get(), sizeof(T));
      free_queue.enqueue(data_with_token);
    }
  }

  void read(int remote_index, DataWithRequestCallback<T> callback, const T& read_data, uint64_t local_offset = 0, uint64_t remote_offset = 0, uint64_t size_in_bytes = sizeof(T))
  {
    DataWithRequestToken data_with_request_token;
    while (!free_queue.try_dequeue(data_with_request_token))
    {
      LOG_RDMA_DATA("Cannot queue read operation");
    }
    *data_with_request_token.data = read_data;
    data_with_request_token.remote_index = remote_index;
    data_with_request_token.callback = std::move(callback);

    LOG_RDMA_DATA("[RDMADataWithQueue] Index [{}] Request data [{}:{}:{}]", remote_index, local_offset, remote_offset, size_in_bytes);
    data_with_request_token.token = RDMAData::read(remote_index, data_with_request_token.data.get(), sizeof(T), local_offset, remote_offset, size_in_bytes);
    pending_read_queue.enqueue(std::move(data_with_request_token));

    // data_with_request_token.token->waitUntilCompleted();
    // LOG_RDMA_DATA("[RDMADataWithQueue] Index [{}] Got data [{}:{}]", remote_index, data_with_request_token.data->key_index, (char*)data_with_request_token.data->data);
    // free_queue.enqueue(std::move(data_with_request_token));
  }

  template<typename F>
  void execute_pending(F&& f)
  {
    DataWithRequestToken data_with_request_token;
    while (pending_read_queue.try_dequeue(data_with_request_token))
    {
      data_with_request_token.token->waitUntilCompleted();
      f(data_with_request_token);
      if (data_with_request_token.callback)
      {
        data_with_request_token.callback(data_with_request_token.remote_index, *data_with_request_token.data);
      }
      free_request_token(std::move(data_with_request_token.token));
      free_queue.enqueue(std::move(data_with_request_token));
    }
  }

public:
  struct DataWithRequestToken
  {
    std::shared_ptr<T> data;
    std::shared_ptr<infinity::requests::RequestToken> token;
    int remote_index;
    DataWithRequestCallback<T> callback;
  };

protected:
  MPMCQueue<DataWithRequestToken> pending_read_queue;
  MPMCQueue<DataWithRequestToken> free_queue;
};

struct RDMACacheIndex2
{
  RDMACacheIndex* cache_index;
  bool is_local = false;
};

struct RDMACacheIndexKeyValue
{
  uint64_t key_index;
  uint8_t data[RDMA_CACHE_INDEX_KEY_VALUE_SIZE];
};

// This index storage maintains a cache index for each remote machine
// This index storage is mapped as rdma to each of the machines, this means we can read directly into the key value storage using the key_value_offset
// Three RDMA points
// [Local CacheIndex] has key_value_offset --> RDMA read --> [KeyValueStore]
// [UpdateCacheIndexLog] has batch updates --> RDMA write --> [Local CacheIndexLog] --> apply local --> [Local CacheIndex]
// Every server -> CacheIndexLog * N servers
// Every server -> KeyValueStore

struct KeyValueStorage : public RDMADataWithQueue<RDMACacheIndexKeyValue>
{
  KeyValueStorage(BlockCacheConfig block_cache_config, Configuration ops_config, int machine_index,
    infinity::core::Context *context,
    infinity::queues::QueuePairFactory* qp_factory, RDMAKeyValueStorage* rdma_kv_storage_ = nullptr) :
    RDMADataWithQueue(block_cache_config, ops_config, machine_index, context, qp_factory, CacheIndexValueQueueSize), rdma_kv_storage(rdma_kv_storage_)
  {
    LOG_RDMA_DATA("[KeyValueStorage] Initializing");

    auto key_value_buffer = rdma_kv_storage->get_key_value_buffer();
    auto key_value_buffer_size = rdma_kv_storage->get_key_value_buffer_size();

    auto done_connect = std::async(std::launch::async, [&] {
      while(!start_accepting_connections)
      {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(RDMA_SETUP_SYNC_SLEEP_MS));
      connect(KEY_VALUE_STORAGE_PORT);
    });
    listen(KEY_VALUE_STORAGE_PORT, key_value_buffer, key_value_buffer_size);
    done_connect.wait();

    LOG_RDMA_DATA("[KeyValueStorage] Initialized");
  }

  void read(int remote_index, DataWithRequestCallback<RDMACacheIndexKeyValue> callback, RDMACacheIndex rdma_cache_index)
  {
    LOG_RDMA_DATA("[KeyValueStorage] Read machine {} with offset {}", remote_index, rdma_cache_index.key_value_ptr_offset);
    RDMACacheIndexKeyValue read_data;
    auto remote_offset = rdma_cache_index.key_value_ptr_offset;

    RDMADataWithQueue::read(remote_index, callback, read_data, 0, remote_offset);
  }

  RDMAKeyValueStorage* rdma_kv_storage{};
};

struct CacheIndexes : public RDMAData
{
  CacheIndexes(BlockCacheConfig block_cache_config, Configuration ops_config, int machine_index,
    infinity::core::Context *context,
    infinity::queues::QueuePairFactory* qp_factory,
    RDMAKeyValueStorage* rdma_kv_storage_) :
    RDMAData(block_cache_config, ops_config, machine_index, context, qp_factory), rdma_kv_storage(rdma_kv_storage_)
  {
    LOG_RDMA_DATA("[CacheIndexes] Initializing");
    rdma_kv_storage->set_my_cache_index(machine_index);
    rdma_cache_indexes.resize(server_configs.size());
    for (auto i = 0; i < server_configs.size(); i++)
    {
      auto server_config = server_configs[i];
      RDMACacheIndex* cache_index;
      if (i == machine_index)
      {
        LOG_RDMA_DATA("[CacheIndexes] Local cache index buffer {}", i);
        cache_index = rdma_kv_storage->get_cache_index_buffer();
      }
      else
      {
        LOG_RDMA_DATA("[CacheIndexes] Remote cache index buffer {}", i);
        cache_index = rdma_kv_storage->allocate_cache_index();
      }
      rdma_kv_storage->set_cache_index(i, cache_index);
      rdma_cache_indexes[i] = cache_index;

      auto done_connect = std::async(std::launch::async, [&] {
        while(!start_accepting_connections)
        {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(RDMA_SETUP_SYNC_SLEEP_MS));
        RDMAData::connect(CACHE_INDEXES_PORT + i);
      });
      auto size = rdma_kv_storage->get_allocated_cache_index_size();
      RDMAData::listen(CACHE_INDEXES_PORT + i, cache_index, size);
      done_connect.wait();
    }
    LOG_RDMA_DATA("[CacheIndexes] Initialized");
  }

  void write_remote(const std::string& key, const std::string& value)
  {
    auto key_index = convert_string<uint64_t>(key);
    for (auto i = 0; i < server_configs.size(); i++)
    {
      const auto& server_config = server_configs[i];
      if (machine_index == i)
      {
        continue;
      }
      auto size = rdma_kv_storage->get_allocated_cache_index_size();
      auto offset = key_index * sizeof(RDMACacheIndex);
      const auto& rdma_cache_index = rdma_cache_indexes[machine_index];
      auto rdma_index = (machine_index * server_configs.size()) + i;
      if (rdma_cache_index[key_index].key_value_ptr_offset == KEY_VALUE_PTR_INVALID)
      {
        panic("[{}-{}] Invalid key value ptr offset - key {} offset {}", machine_index, rdma_index, key_index, rdma_cache_index[key_index].key_value_ptr_offset);
      }
      LOG_RDMA_DATA("[CacheIndexes] Writing remote {} - [{}] key {} offset {} {}", i, rdma_index, key_index, (void*)&rdma_cache_index[key_index], rdma_cache_index[key_index].key_value_ptr_offset);
      auto request_token = RDMAData::write(rdma_index, rdma_cache_index, size, offset, offset, sizeof(RDMACacheIndex));
      pending_write_queue.enqueue(request_token);
    }
  }

  void update_local_key(uint64_t expected_key, uint64_t key, const std::string& value)
  {
    auto expected_key_index = expected_key;
    auto key_index = key;

    if (key_index == KEY_VALUE_PTR_INVALID)
    {
      return;
    }

    const auto& my_rdma_cache_index = rdma_cache_indexes[machine_index];
    for (auto i = 0; i < server_configs.size(); i++)
    {
      const auto& server_config = server_configs[i];
      if (machine_index == i)
      {
        continue;
      }
      auto& rdma_cache_index = rdma_cache_indexes[i];
      rdma_cache_index[key_index] = my_rdma_cache_index[expected_key_index]; 
    }
  }

  void dealloc_remote(const std::string& key)
  {
    auto key_index = convert_string<uint64_t>(key);
    for (auto i = 0; i < server_configs.size(); i++)
    {
      const auto& server_config = server_configs[i];
      if (machine_index == i)
      {
        continue;
      }
      auto size = rdma_kv_storage->get_allocated_cache_index_size();
      auto offset = key_index * sizeof(RDMACacheIndex);
      auto& rdma_cache_index = rdma_cache_indexes[machine_index];
      rdma_cache_index[key_index] = InvalidRDMACacheIndex;
      auto rdma_index = (machine_index * server_configs.size()) + i;
      LOG_RDMA_DATA("[CacheIndexes] Dealloc remote {} - [{}] key {} offset {} {}", i, rdma_index, key_index, (void*)&rdma_cache_index[key_index], rdma_cache_index[key_index].key_value_ptr_offset);
      auto request_token = RDMAData::write(rdma_index, rdma_cache_index, size, offset, offset, sizeof(RDMACacheIndex));
      pending_write_queue.enqueue(request_token);
    }
  }

  auto& get_cache_index(int index) { return rdma_cache_indexes[index]; }
  auto& get_cache_indexes() { return rdma_cache_indexes; }

  template<typename F>
  void execute_pending(F&& f)
  {
    std::shared_ptr<infinity::requests::RequestToken> token;
    while (pending_write_queue.try_dequeue(token))
    {
      token->waitUntilCompleted();
      f();
      free_request_token(std::move(token));
    }
  }

  std::vector<RDMACacheIndex*> rdma_cache_indexes;
  MPMCQueue<std::shared_ptr<infinity::requests::RequestToken>> pending_write_queue;
  RDMAKeyValueStorage* rdma_kv_storage{};
};

struct CacheIndexLogEntry
{
  uint64_t key = -1;
  RDMACacheIndex cache_index = InvalidRDMACacheIndex;
  bool filled = false;
};

using CacheIndexLogEntries = std::vector<CacheIndexLogEntry>;

struct MachineCacheIndexLog
{
  CacheIndexLogEntries cache_index_log_entries;
  CopyableAtomic<uint64_t> index;
};

struct CacheIndexLogs : public RDMAData
{
  CacheIndexLogs(BlockCacheConfig block_cache_config, Configuration ops_config, int machine_index,
    infinity::core::Context *context,
    infinity::queues::QueuePairFactory* qp_factory, std::shared_ptr<CacheIndexes> cache_indexes_) :
    RDMAData(block_cache_config, ops_config, machine_index, context, qp_factory), cache_indexes(cache_indexes_)
  {
    if (!ops_config.use_cache_logs)
    {
      return;
    }
    max_cache_log_index = ops_config.cache_log_sync_every_x_operations;
    LOG_RDMA_DATA("[CacheIndexLogs] Initializing");
    machine_cache_index_logs.resize(server_configs.size());
    for (auto i = 0; i < server_configs.size(); i++)
    {
      auto server_config = server_configs[i];
      // TODO: add to config
      auto cache_index_log_size = max_cache_log_index;
      auto& cache_index_log_entries = machine_cache_index_logs[i].cache_index_log_entries;
      cache_index_log_entries.resize(cache_index_log_size + 1);
      auto done_connect = std::async(std::launch::async, [&] {
        while(!start_accepting_connections)
        {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(RDMA_SETUP_SYNC_SLEEP_MS));
        RDMAData::connect(CACHE_INDEX_LOG_PORT + i);
      });
      cache_index_log_entries_size = cache_index_log_entries.size() * sizeof(CacheIndexLogEntry);
      RDMAData::listen(CACHE_INDEX_LOG_PORT + i, cache_index_log_entries.data(), cache_index_log_entries_size);
      done_connect.wait();
    }
    LOG_RDMA_DATA("[CacheIndexLogs] Initialized");

    if (ops_config.cache_logs_push_entries)
    {
      static std::thread background_worker([this, machine_index]()
      {
        while (!g_stop)
        {
          // Apply to states
          for (auto i = 0; i < server_configs.size(); i++)
          {
            const auto& server_config = server_configs[i];
            if (machine_index == i)
            {
              continue;
            }
            auto& cache_indexes = this->cache_indexes->get_cache_index(i);
            auto& [cache_index_log_entries, log_index] = machine_cache_index_logs[i];
            for (auto j = 0; j < max_cache_log_index; j++)
            {
              auto& cache_index_log_entry = cache_index_log_entries[j];
              if (!cache_index_log_entry.filled)
              {
                continue;
              }
              auto key_index = cache_index_log_entry.key;
              auto cache_index = cache_index_log_entry.cache_index;
              cache_index_log_entry.filled = false;
              if (key_index == KEY_VALUE_PTR_INVALID)
              {
                continue;
              }
              LOG_RDMA_DATA("[CacheIndexLogs] [{}] Applied key {} with ptr {}", i, key_index, cache_index.key_value_ptr_offset);
              cache_indexes[key_index] = cache_index;
            }
            cache_index_log_entries[max_cache_log_index].filled = false;
          }
        }
      });
      background_worker.detach();
    }
    else
    {
      static std::thread background_worker([this, machine_index]()
      {
        uint64_t NUM_CACHE_INDEXES = 1000;
        struct ServerInfo
        {
          uint64_t counter = 0;
          uint64_t index = 0;
        };
        std::vector<ServerInfo> infos(server_configs.size());
        while (!g_stop)
        {
          // Apply to states
          for (auto i = 0; i < server_configs.size(); i++)
          {
            const auto& server_config = server_configs[i];
            if (machine_index == i)
            {
              continue;
            }
            auto& cache_indexes = this->cache_indexes->get_cache_index(i);
            auto& [cache_index_log_entries, log_index] = machine_cache_index_logs[i];

            auto rdma_index = (i * server_configs.size()) + machine_index;

            auto& [counter, index] = infos[i];

            // Check if first entry is not set
            auto local_offset = 0;
            auto remote_offset = (counter % max_cache_log_index) * NUM_CACHE_INDEXES * sizeof(CacheIndexLogEntry);
            auto token = RDMAData::read(rdma_index, cache_index_log_entries.data(), cache_index_log_entries_size, local_offset, remote_offset, NUM_CACHE_INDEXES * sizeof(CacheIndexLogEntry));
            token->waitUntilCompleted();
            free_request_token(token);

            bool finished_block = true;
            for (; index < NUM_CACHE_INDEXES; index++)
            {
              auto& cache_index_log_entry = cache_index_log_entries[index];
              if (!cache_index_log_entry.filled)
              {
                finished_block = false;
                break;
              }
              auto key_index = cache_index_log_entry.key;
              auto cache_index = cache_index_log_entry.cache_index;
              cache_index_log_entry.filled = false;
              if (key_index == KEY_VALUE_PTR_INVALID)
              {
                continue;
              }
              LOG_RDMA_DATA("[CacheIndexLogs] [{}] Applied key {} with ptr {}", i, key_index, cache_index.key_value_ptr_offset);
              cache_indexes[key_index] = cache_index;
            }
            if (finished_block)
            {
              index = 0;
              counter++;
            }
          }
        }
      });
      background_worker.detach();
    }
  }

  void append_entry(CacheIndexLogEntry entry)
  {
    entry.filled = true;

    auto& [cache_index_log_entries, log_index] = machine_cache_index_logs[machine_index];
    auto current_log_index = log_index.fetch_add(1, std::memory_order_relaxed) % max_cache_log_index;
    auto& cache_index_log_entry = cache_index_log_entries[current_log_index];
    cache_index_log_entry = entry;
    LOG_RDMA_DATA("[CacheIndexLogs] {} Writing keys to {}", current_log_index, entry.key);

    if (ops_config.cache_logs_push_entries)
    {
      if (current_log_index == 0)
      {
        bool done = false;
        while (!done)
        {
          auto machines_ready = 0;
          for (auto i = 0; i < server_configs.size(); i++)
          {
            const auto& server_config = server_configs[i];
            if (machine_index == i)
            {
              continue;
            }
            auto rdma_index = (i * server_configs.size()) + machine_index;

            // Check if first entry is not set
            LOG_RDMA_DATA("[CacheIndexLogs] Checking if remote [{}] is done {}", i, current_log_index);
            auto local_offset = max_cache_log_index * sizeof(CacheIndexLogEntry);
            auto token = RDMAData::read(rdma_index, cache_index_log_entries.data(), cache_index_log_entries_size, local_offset, 0, sizeof(CacheIndexLogEntry));
            token->waitUntilCompleted();
            free_request_token(token);

            auto& last_entry = cache_index_log_entries[max_cache_log_index];
            LOG_RDMA_DATA("[CacheIndexLogs] Checked if remote [{}] is done {} | {}", i, current_log_index, last_entry.filled);
            if (!last_entry.filled)
            {
              machines_ready++;
            }
          }

          if (machines_ready == server_configs.size() - 1)
          {
            auto& last_entry = cache_index_log_entries[max_cache_log_index];
            last_entry.filled = true;

            // Write to remote machines
            for (auto i = 0; i < server_configs.size(); i++)
            {
              const auto& server_config = server_configs[i];
              if (machine_index == i)
              {
                continue;
              }
              auto rdma_index = (machine_index * server_configs.size()) + i;
              LOG_RDMA_DATA("[CacheIndexLogs] Wrote all keys to {}", i);
              auto request_token = RDMAData::write(rdma_index, cache_index_log_entries.data(), cache_index_log_entries_size, 0, 0, cache_index_log_entries_size);
              request_token->waitUntilCompleted();
              free_request_token(std::move(request_token));
              // pending_write_queue.enqueue(request_token);
            }
            done = true;
          }
          else
          {
            info("[CacheIndexLogs] Unable to sync! Other nodes are not ready");
          }
        }
      }
    }
    else
    {
      
    }
  }

  void append_entry_k(const std::string& key)
  {
    auto key_index = convert_string<uint64_t>(key);
    const auto& rdma_cache_index = cache_indexes->get_cache_index(machine_index);
    auto cache_index = rdma_cache_index[key_index];
    CacheIndexLogEntry entry{key_index, cache_index, true};
    append_entry(entry);
  }

  template<typename F>
  void execute_pending(F&& f)
  {
    std::shared_ptr<infinity::requests::RequestToken> token;
    while (pending_write_queue.try_dequeue(token))
    {
      token->waitUntilCompleted();
      f();
      free_request_token(std::move(token));
    }
  }

  uint64_t max_cache_log_index = 0;
  uint64_t cache_index_log_entries_size = 0;
  std::vector<MachineCacheIndexLog> machine_cache_index_logs;
  MPMCQueue<std::shared_ptr<infinity::requests::RequestToken>> pending_write_queue;
  std::shared_ptr<CacheIndexes> cache_indexes;
};

struct RDMAKeyValueCache : public RDMAData
{
  RDMAKeyValueCache(BlockCacheConfig block_cache_config, Configuration ops_config, int machine_index, infinity::core::Context *context,
    infinity::queues::QueuePairFactory* qp_factory,
    RDMAKeyValueStorage* kv_storage_,
    std::shared_ptr<BlockCache<std::string, std::string>> block_cache_) :
    RDMAData(block_cache_config, ops_config, machine_index, context, qp_factory),
    kv_storage(kv_storage_),
    block_cache(block_cache_),
    cache_indexes(std::make_shared<CacheIndexes>(block_cache_config, ops_config, machine_index, context, qp_factory, kv_storage)),
    cache_index_logs(std::make_unique<CacheIndexLogs>(block_cache_config, ops_config, machine_index, context, qp_factory, cache_indexes)),
    key_value_storage(std::make_unique<KeyValueStorage>(block_cache_config, ops_config, machine_index, context, qp_factory, kv_storage))
  {
    LOG_RDMA_DATA("[RDMAKeyValueCache] Initializing machine index {}", machine_index);
    auto cache = block_cache->get_cache();
    cache->add_callback_on_write([this, ops_config](const std::string& key, const std::string& value){
      // Update the cache_indexes on remote nodes
      LOG_RDMA_DATA("[RDMAKeyValueCache] Writing callback on cache index {} {}", key, value);
      // if (ops_config.use_cache_logs)
      // {
      //   cache_index_logs->append_entry_k(key);
      // }
      // else
      // {
      //   cache_indexes->write_remote(key, value);
      // }
      EvictionCallbackData<std::string, std::string> data{key, value};
      cache_index_write_queue.enqueue(data);
      write_cache_index_cv.notify_one();
      writes.fetch_add(1, std::memory_order::relaxed);
    });
    cache->add_callback_on_eviction([this, ops_config](EvictionCallbackData<std::string, std::string> data){
      LOG_RDMA_DATA("Evicted {}", data.key);
      snapshot->update_evicted(std::stoi(data.key));
      // if (ops_config.use_cache_logs)
      // {
      //   cache_index_logs->append_entry_k(data.key);
      // }
      // else
      // {
      //   cache_indexes->dealloc_remote(data.key);
      // }
      cache_index_eviction_queue.enqueue(data);
      write_cache_index_cv.notify_one();
      writes.fetch_add(1, std::memory_order::relaxed);
    });
    LOG_RDMA_DATA("[RDMAKeyValueCache] Initialized");

    static std::thread background_worker([this, ops_config]()
    {
      while (!g_stop)
      {
        std::unique_lock lk(write_cache_index_m);
        write_cache_index_cv.wait_for(lk, std::chrono::milliseconds(1));
        write_cache_index_m.unlock();
        EvictionCallbackData<std::string, std::string> data;
        while (cache_index_write_queue.try_dequeue(data))
        {
          if (ops_config.use_cache_logs)
          {
            cache_index_logs->append_entry_k(data.key);
          }
          else
          {
            cache_indexes->write_remote(data.key, data.value);
          }
        }

        while (cache_index_eviction_queue.try_dequeue(data))
        {
          if (ops_config.use_cache_logs)
          {
            cache_index_logs->append_entry_k(data.key);
          }
          else
          {
            cache_indexes->dealloc_remote(data.key);
          }
        }
      }
    });
    background_worker.detach();
  }

  void read(int remote_index, uint64_t key_index)
  {
    RDMACacheIndex* cache_index = cache_indexes->get_cache_index(remote_index);
    // auto rdma_index = (machine_index * server_configs.size()) + remote_index;
    auto rdma_index = remote_index;
    const auto& ci = cache_index[key_index];
    LOG_RDMA_DATA("[RDMAKeyValueCache] Reading cache index {} key {} key_value_offset {}", rdma_index, key_index, (uint64_t)ci.key_value_ptr_offset);
    key_value_storage->read(rdma_index, {}, ci);
  }

  bool read_callback(uint64_t key_index, DataWithRequestCallback<RDMACacheIndexKeyValue> callback)
  {
    bool found_remote_machine_with_possible_value = false;
    for (auto i = 0; i < server_configs.size(); i++)
    {
      auto rdma_index = i;
      if (rdma_index == machine_index)
      {
        continue;
      }
      RDMACacheIndex* cache_index = cache_indexes->get_cache_index(rdma_index);
      const auto& ci = cache_index[key_index];
      if (ci.key_value_ptr_offset == KEY_VALUE_PTR_INVALID)
      {
        continue;
      }
      // auto rdma_index = (machine_index * server_configs.size()) + remote_index;
      LOG_RDMA_DATA("[RDMAKeyValueCache] Reading cache index {} key {} key_value_offset {}", rdma_index, key_index, (uint64_t)ci.key_value_ptr_offset);
      key_value_storage->read(rdma_index, callback, ci);
      found_remote_machine_with_possible_value = true;
      break;
    }
    return found_remote_machine_with_possible_value;
  }

  inline static void default_function()
  {
  }

  void update_local_key(uint64_t expected_key, uint64_t key, const std::string& value)
  {
    cache_indexes->update_local_key(expected_key, key, value);
  }

  template<typename F, typename FF>
  void execute_pending(F&& on_key_value_storage_read, FF&& on_cache_index_write = default_function)
  {
    cache_index_logs->execute_pending(on_cache_index_write);
    cache_indexes->execute_pending(std::forward<FF>(on_cache_index_write));
    key_value_storage->execute_pending(std::forward<F>(on_key_value_storage_read));
  }

  uint64_t get_writes() const { return writes.load(std::memory_order::relaxed); }
  auto get_cache_indexes() { return cache_indexes; }

private:
  std::shared_ptr<BlockCache<std::string, std::string>> block_cache;
  RDMAKeyValueStorage* kv_storage;
  std::shared_ptr<CacheIndexes> cache_indexes;
  std::unique_ptr<CacheIndexLogs> cache_index_logs;
  std::unique_ptr<KeyValueStorage> key_value_storage;
  CopyableAtomic<uint64_t> writes;

  std::mutex write_cache_index_m;
  std::condition_variable write_cache_index_cv;
  MPMCQueue<EvictionCallbackData<std::string, std::string>> cache_index_write_queue;
  MPMCQueue<EvictionCallbackData<std::string, std::string>> cache_index_eviction_queue;
};

struct RDMA_connect
{
  std::string ip;
  u_int64_t index;
  infinity::core::Context *context;
  infinity::queues::QueuePair *qp;
  infinity::queues::QueuePairFactory *qp_factory;
  infinity::memory::RegionToken *remote_buffer_token;
  infinity::memory::Buffer *buffer;
  void *tmp_buffer;
  u_int64_t free_blocks;
  bool isLocal;
  void *local_memory_region;
  AsyncRdmaOpCircularBuffer *circularBuffer;
  std::shared_ptr<BlockCache<std::string, std::string>> block_cache{};
  std::shared_ptr<RDMAKeyValueCache> rdma_key_value_cache = nullptr;
};

inline std::optional<std::string> find_nic_containing(std::string_view s)
{
    int32_t numberOfInstalledDevices = 0;
    ibv_device **ibvDeviceList = ibv_get_device_list(&numberOfInstalledDevices);

    int dev_i = 0;
    for (int dev_i = 0; dev_i < numberOfInstalledDevices; dev_i++) {
        ibv_device *dev = ibvDeviceList[dev_i];
        const char *name = ibv_get_device_name(dev);
        if (std::string_view(name).find(s) != std::string::npos)
        {
            return name;
        }
    }
    return std::nullopt;
}

std::vector<std::string> load_database(Configuration &ops_config,
                                       Client &client);
int generateDatabaseAndOperationSet(Configuration &ops_config);
void executeOperations(
    const Operations &operationSet, int client_start_index,
    BlockCacheConfig config, Configuration &ops_config, Client &client, int client_index_per_thread, int machine_index);

void dump_per_thread_latency_to_file(const std::vector<long long> &timestamps, int client_index_per_thread, int machine_index, int thread_index);
void dump_latency_to_file(const std::string &filename, const std::vector<long long> &timestamps);

int issueOps(BlockCacheConfig config, Configuration &ops_config,
             std::vector<std::string> &keys, Client client);

HashMap<uint64_t, RDMA_connect> connect_to_servers(
    BlockCacheConfig config, int machine_index, int value_size, Configuration ops_config, std::shared_ptr<BlockCache<std::string, std::string>> block_cache);

void printRDMAConnect(const RDMA_connect &conn);

void *RDMA_Server_Init(int serverport, uint64_t buffer_size, int machine_index, Configuration ops_config);

void rdma_writer_thread(RDMA_connect &node, uint64_t offset, std::span<uint8_t> buffer_data);

void rdma_reader_thread(RDMA_connect &node, uint64_t offset, void *buffer_data, Server *server, int remoteIndex, int remote_port);

int calculateNodeAndOffset(Configuration &ops_config, uint64_t key, int &nodeIndex, uint64_t &offset);

void write_correct_node(Configuration &ops_config, HashMap<uint64_t, RDMA_connect> &rdma_nodes, int node, uint64_t key, std::span<uint8_t> buffer_data);

void read_correct_node(Configuration &ops_config, HashMap<uint64_t, RDMA_connect> &rdma_nodes, int node, uint64_t key, void *buffer_data, Server *server, int remoteIndex, int remote_port);

void printBuffer(const std::array<uint8_t, BLKSZ> &buffer);

inline void printBlockDBConfig(const BlockDBConfig &config)
{
  std::cout << "BlockDBConfig: { filename: " << config.filename
            << ", num_entries: " << config.num_entries
            << ", block_size: " << config.block_size << " }" << std::endl;
}

inline void printDBConfig(const DBConfig &config)
{
  std::cout << "DBConfig: { block_db: " << std::endl;
  printBlockDBConfig(config.block_db);
  std::cout << "}" << std::endl;
}

inline void printLRUConfig(const LRUConfig &config)
{
  std::cout << "LRUConfig: { cache_size: " << config.cache_size << " }" << std::endl;
}

inline void printRandomCacheConfig(const RandomCacheConfig &config)
{
  std::cout << "RandomCacheConfig: { cache_size: " << config.cache_size << " }" << std::endl;
}

inline void printSplitCacheConfig(const SplitCacheConfig &config)
{
  std::cout << "SplitCacheConfig: { cache_size: " << config.cache_size
            << ", owning_ratio: " << config.owning_ratio
            << ", nonowning_ratio: " << config.nonowning_ratio
            << ", owning_cache_type: " << config.owning_cache_type
            << ", nonowning_cache_type: " << config.nonowning_cache_type << " }" << std::endl;
}

inline void printThreadSafeLRUConfig(const ThreadSafeLRUConfig &config)
{
  std::cout << "ThreadSafeLRUConfig: { cache_size: " << config.cache_size << " }" << std::endl;
}

inline void printRdmaConfig(const RdmaConfig &config)
{
  std::cout << "RdmaConfig: { context_index: " << config.context_index << " }" << std::endl;
}

inline void printCacheConfig(const CacheConfig &config)
{
  std::cout << "CacheConfig: {" << std::endl;
  std::cout << "  LRU: " << std::endl;
  printLRUConfig(config.lru);
  std::cout << "  Random: " << std::endl;
  printRandomCacheConfig(config.random);
  std::cout << "  Split: " << std::endl;
  printSplitCacheConfig(config.split);
  std::cout << "  ThreadSafeLRU: " << std::endl;
  printThreadSafeLRUConfig(config.thread_safe_lru);
  std::cout << "  paged: " << (config.paged ? "true" : "false") << std::endl;
  std::cout << "  RDMA: " << std::endl;
  printRdmaConfig(config.rdma);
  std::cout << "}" << std::endl;
}

inline void printRemoteMachineConfig(const RemoteMachineConfig &config)
{
  std::cout << "RemoteMachineConfig: { index: " << config.index
            << ", ip: " << config.ip
            << ", port: " << config.port
            << ", server: " << (config.server ? "true" : "false") << " }" << std::endl;
}

inline void printBaseline(const Baseline &config)
{
  std::cout << "Baseline: { selected: " << config.selected
            << ", one_sided_rdma_enabled: " << (config.one_sided_rdma_enabled ? "true" : "false") << " }" << std::endl;
}

inline void printBlockCacheConfig(const BlockCacheConfig &config)
{
  std::cout << "BlockCacheConfig: {" << std::endl
            << "  ingest_block_index: " << (config.ingest_block_index ? "true" : "false") << std::endl
            << "  policy_type: " << config.policy_type << std::endl
            << "  rdma_port: " << config.rdma_port << std::endl
            << "  db_type: " << config.db_type << std::endl;
  std::cout << "  DBConfig: " << std::endl;
  printDBConfig(config.db);
  std::cout << "  CacheConfig: " << std::endl;
  printCacheConfig(config.cache);
  std::cout << "  Baseline: " << std::endl;
  printBaseline(config.baseline);
  std::cout << "  RemoteMachineConfigs: " << std::endl;
  for (const auto &remoteConfig : config.remote_machine_configs)
  {
    printRemoteMachineConfig(remoteConfig);
  }
  std::cout << "}" << std::endl;
  std::cout << "access_rate: " << config.access_rate << std::endl;
  std::cout << "access_per_itr: " << config.access_per_itr << std::endl;
  
}
template<typename KeyType, typename ValueType>
struct CacheLayerData
{
    KeyType key;
    ValueType value;
    bool singleton;
    uint64_t forward_count;
    int replica_count;
};

struct SimpleSharedLog
{
  SimpleSharedLog(std::size_t capacity_) : capacity(capacity_)
  {
    key_values.resize(capacity);
  }

  uint64_t append(std::string_view key, std::string_view value)
  {
    auto current_index = index.fetch_add(1, std::memory_order::relaxed);
    key_values[current_index % capacity] = KeyValueEntry{std::string(key), std::string(value)};
    return current_index;
  }

  KeyValueEntry get(uint64_t current_index)
  {
    return key_values[current_index % capacity];
  }

  uint64_t get_tail() const { return index.load(std::memory_order::relaxed); }

  std::size_t capacity;
  std::vector<KeyValueEntry> key_values;
  std::atomic<uint64_t> index;
};

class LDCTimer {
public:
  LDCTimer() {
    start();
  }

  void start() {
    start_time = std::chrono::high_resolution_clock::now();
  }

  void stop() {
    end_time = std::chrono::high_resolution_clock::now();
  }

  auto time_elapsed() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<uint64_t, std::nano>(end - start_time).count();
  }

  auto elapsed() const {
    return std::chrono::duration<uint64_t, std::nano>(end_time - start_time).count();
  }

private:
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
  std::chrono::time_point<std::chrono::high_resolution_clock> end_time;
};
