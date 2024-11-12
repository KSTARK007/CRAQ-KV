#pragma once

#include <ext/machnet.h>
#undef PAGE_SIZE // both define PAGE_SIZE

#include "defines.h"

#include "block_cache.h"
#include "operations.h"

// hash for MachnetFlow
namespace std
{
  template <>
  struct hash<MachnetFlow>
  {
    std::size_t operator()(const MachnetFlow &flow) const
    {
      return std::hash<int>{}(flow.src_ip) ^ std::hash<int>{}(flow.dst_ip) ^
             std::hash<int>{}(flow.src_port) ^ std::hash<int>{}(flow.dst_port);
    }
  };
} // namespace std

struct ConnectionData
{
  MachnetFlow flow;
};

struct MachineIndexAndPort
{
  int machine_index;
  int port;

  bool operator==(const MachineIndexAndPort &other) const
  {
    return machine_index == other.machine_index && port == other.port;
  }
};

namespace std
{
  template <>
  struct hash<MachineIndexAndPort>
  {
    std::size_t operator()(const MachineIndexAndPort &flow) const
    {
      return std::hash<int>{}(flow.machine_index) ^ std::hash<int>{}(flow.port);
    }
  };
} // namespace std

struct SharedLogPutRequestEntry
{
  std::string key;
  std::string value;
  uint64_t hash;
};

struct SharedLogPutResponseEntry
{
  uint64_t index;
  uint64_t hash;
};

struct Connection
{
  Connection(BlockCacheConfig config_, Configuration ops_config_,
             int machine_index_, int thread_index_);

  const BlockCacheConfig &get_block_cache_config() { return config; }
  const Configuration& get_ops_config() { return ops_config; }

  void connect_to_remote_machine(int remote_index);
  void listen();
  void send(int index, int port, std::string_view data);
  void send(int index, std::string_view data);
  void put(int index, int thread_index, std::string_view key, std::string_view value);
  std::string get(int index, int thread_index, std::string_view key);
  void poll_receive(auto &&handler)
  {
    bool received_data = false;
    while (!received_data)
    {
      received_data = receive(handler);
      if (g_stop)
      {
        break;
      }
    }
  }

  void loop(auto &&handler)
  {
    bool received_data = false;
    while (!received_data)
    {
      execute_pending_operations();
      received_data = receive(handler);
      if (g_stop)
      {
        break;
      }
    }
  }

   void receive_and_execute_pending(auto &&handler)
  {
    execute_pending_operations();
    receive(handler);
  }

  virtual void execute_pending_operations()
  {
    for (const auto& pending_function : pending_functions)
    {
      pending_function();
    }
  }
  
  void append_pending_function(std::function<void()> f)
  {
    pending_functions.emplace_back(f);
  }

  bool receive(auto &&handler)
  {
    std::array<char, 4096 * 2> buf;

    MachnetFlow rx_flow;

    int ret = -1;
    do
    {
      ret = machnet_recv(channel, buf.data(), buf.size(), &rx_flow);
      if (ret < 0)
      {
        info("machnet_recv() {}", ret);
      }
    }
    while (ret < 0);
    assert_with_msg(ret >= 0, "machnet_recv() failed");
    if (ret == 0)
    {
      return false;
    }

    // perf_monitor.record_receive_request(ret);

    auto *word = reinterpret_cast<capnp::word *>(buf.data());
    auto received_array = kj::ArrayPtr<capnp::word>(word, word + ret);
    capnp::FlatArrayMessageReader message(received_array);
    Packets::Reader packets = message.getRoot<Packets>();
    for (Packet::Reader packet : packets.getPackets())
    {
      auto data = packet.getData();

      MachnetFlow tx_flow;
      tx_flow.dst_ip = rx_flow.src_ip;
      tx_flow.src_ip = rx_flow.dst_ip;
      tx_flow.dst_port = rx_flow.src_port;
      tx_flow.src_port = rx_flow.dst_port;

      auto remote_index = dst_ip_to_machine_index[rx_flow.src_ip];
      auto port = rx_flow.src_port;
      machine_index_to_connection[{remote_index, port}].flow = tx_flow;

      LOG_STATE("[{}-{}] Received [{}]", machine_index, remote_index,
                kj::str(data).cStr());

      handler(remote_index, port, tx_flow, data);
    }
    return true;
  }

  // TODO: Add rpc functions for craq
  void craq_forward_propagate_request(int index, int port, std::string_view key, std::string_view value, uint64_t client_index, uint64_t client_port);
  void craq_backward_propagate_request(int index, int port, std::string_view key, int latest_clean_version, uint64_t client_index, uint64_t client_port);
  void craq_version_request(int index, int port, std::string_view key);
  void craq_version_response(int index, int port, std::string_view key, int version);

  void shared_log_forward_request(int index, int port, std::string_view key, uint64_t hash);
  void shared_log_forward_response(int index, int port, ResponseType response_type, uint64_t hash);
  void shared_log_put_request(int index, int port, std::string_view key, std::string_view value, uint64_t hash);
  void shared_log_put_response(int index, int port, uint64_t shared_log_index, uint64_t hash);
  void shared_log_put_request(int index, int port, std::vector<SharedLogPutRequestEntry> entries);
  void shared_log_put_response(int index, int port, std::vector<SharedLogPutResponseEntry> entries);
  void shared_log_get_request(int index, int port, uint64_t shared_log_index);
  void shared_log_get_response(int index, int port, uint64_t shared_log_index, uint64_t server_shared_log_index, std::vector<KeyValueEntry> entries);

  int use_next_port();

  int get_machine_index() const { return machine_index; }

protected:
  // The config
  BlockCacheConfig config;

  // This machine's index corresponding to the one in the config
  int machine_index;

  // Thread index
  int thread_index;

  // opoeration parameters
  Configuration ops_config;

  // Mapping from remote machine index to flow (for sending)
  HashMap<MachineIndexAndPort, ConnectionData> machine_index_to_connection;
  HashMap<int, int> dst_ip_to_machine_index;

  // Latest port, increments based on each connection to another machine
  int current_port;

  // Machnet channel
  void *channel;

  std::vector<std::function<void()>> pending_functions;
};

struct Client : public Connection
{
  Client(BlockCacheConfig config, Configuration ops_config, int machine_index,
         int thread_index);

  void connect_to_other_clients();
  void sync_with_other_clients();
};

struct Server : public Connection
{
  Server(BlockCacheConfig config, Configuration ops_config, int machine_index,
         int thread_index, std::shared_ptr<BlockCache<std::string, std::string>> block_cache_);

  void put_response(int index, int port, ResponseType response_type);
  void put_response(int index, ResponseType response_type);
  void get_response(int index, int port, ResponseType response_type,
                    std::string_view value);
  void get_response(int index, ResponseType response_type,
                    std::string_view value);
  void pass_write_to_server_request(int index, int port, std::string_view key);
  void rdma_setup_request(int index, int my_index, uint64_t start_address,
                          uint64_t size);
  void rdma_setup_response(int index, ResponseType response_type);
  void singleton_put_request(int index, int port, std::string_view key,
                             std::string_view value, bool singleton, uint64_t forward_count);
  void delete_request(int index, int port, std::string_view key);
  void fallback_get_request(int index, int port, std::string_view key);
  void fallback_get_response(int index, int port, std::string_view key, std::string_view value, uint64_t key_value_ptr_offset, bool singleton, uint64_t forward_count);

  void execute_pending_operations() override;
  void append_to_rdma_get_response_queue(int index, int port, ResponseType response_type,
                                         std::string_view value);
  json get_stats();
  void append_to_rdma_block_cache_request_queue(int index, int port, ResponseType response_type,
                                                std::string_view key, std::string_view value);
  void append_singleton_put_request(int index, int port, std::string_view key,
                                    std::string_view value, bool singleton, uint64_t forward_count);
  void append_fallback_get_request(int index, int port, std::string_view key);
  void append_delete_request(int index, int port, std::string_view key);
  void append_shared_log_get_request(int index, int port, uint64_t shared_log_index);
  void append_put_response(int index, int port, ResponseType response_type);

  void increment_async_disk_requests() { async_disk_requests++; }

  auto get_block_cache() { return block_cache; }

public:
  struct RDMAGetResponse
  {
    int index;
    int port;
    ResponseType response_type;
    std::string value;
  };

  struct BlockCacheRequest
  {
    int index;
    int port;
    ResponseType response_type;
    std::string key;
    std::string value;
  };

  struct AppendSingletonPutRequest
  {
    int index;
    int port;
    ResponseType response_type;
    std::string key;
    std::string value;
    bool singleton;
    uint64_t forward_count;
  };

  struct AppendFallbackGetRequest
  {
    int index;
    int port;
    std::string key;
  };

  struct AppendDeleteRequest
  {
    int index;
    int port;
    std::string key;
  };

  struct SharedLogGetRequest
  {
    int index;
    int port;
    uint64_t shared_log_index;
  };

  struct AppendPutResponse
  {
    int index;
    int port;
    ResponseType response_type;
  };

private:
  MPMCQueue<RDMAGetResponse> rdma_get_response_queue;
  uint64_t remote_rdma_cache_hits{};

  std::shared_ptr<BlockCache<std::string, std::string>> block_cache;
  MPMCQueue<BlockCacheRequest> block_cache_request_queue;
  uint64_t async_disk_requests{};

  MPMCQueue<AppendSingletonPutRequest> singleton_put_request_queue;
  MPMCQueue<AppendFallbackGetRequest> fallback_get_request_queue;
  MPMCQueue<AppendDeleteRequest> delete_request_queue;
  MPMCQueue<SharedLogGetRequest> shared_log_get_request_queue;
  MPMCQueue<AppendPutResponse> append_put_response_queue;
};
