#include "connection.h"

std::atomic<bool> g_stop;

inline std::string flow_to_string(const MachnetFlow &flow)
{
  char src_address[INET_ADDRSTRLEN];
  char dst_address[INET_ADDRSTRLEN];

  auto src_ip = htonl(flow.src_ip);
  auto dst_ip = htonl(flow.dst_ip);
  inet_ntop(AF_INET, &src_ip, src_address, INET_ADDRSTRLEN);
  inet_ntop(AF_INET, &dst_ip, dst_address, INET_ADDRSTRLEN);

  return fmt::format("[{}:{} -> {}:{}]", src_address, flow.src_port,
                     dst_address, flow.dst_port);
}

Connection::Connection(BlockCacheConfig config_, Configuration ops_config_,
                       int machine_index_, int thread_index_)
    : config(config_), machine_index(machine_index_),
      thread_index(thread_index_), ops_config(ops_config_)
{
  auto my_machine_config = config.remote_machine_configs[machine_index];
  auto ip = my_machine_config.ip;
  // Set this thread's port to be an offset to base port
  auto port = my_machine_config.port + thread_index;
  current_port = my_machine_config.port +
                 (thread_index * config.remote_machine_configs.size());

  LOG_STATE("Connection [{}] {}:{}", thread_index, ip.c_str(), port);

  for (auto i = 0; i < config.remote_machine_configs.size(); i++)
  {
    const auto &remote_machine_config = config.remote_machine_configs[i];
    dst_ip_to_machine_index[ntohl(
        inet_addr(remote_machine_config.ip.c_str()))] = i;
  }

  channel = machnet_attach();
  assert_with_msg(channel != nullptr, "machnet_attach() failed");
}

void Connection::connect_to_remote_machine(int remote_index)
{
  auto my_machine_config = config.remote_machine_configs[machine_index];
  auto ip = my_machine_config.ip;

  // Bind to any next available port
  // auto port = use_next_port();

  auto remote_machine_config = config.remote_machine_configs[remote_index];
  auto remote_port = static_cast<int>(remote_machine_config.port + thread_index);

  auto connection_data = ConnectionData{};
  auto &[flow] = connection_data;

  // LOG_STATE("[{}-{}] Listening on [{}:{}]", machine_index, remote_index, ip,
  //       port);

  // auto ret = machnet_listen(channel, ip.c_str(), port);
  // assert_with_msg(ret == 0, "machnet_listen() failed");
  int ret = 0;
  auto port = 0;

  constexpr auto MACHNET_CONNECT_RETRIES = 1000000;
  for (auto i = 0; i < MACHNET_CONNECT_RETRIES; i++)
  {
    LOG_STATE("[{}-{}] {} Connecting from [{}:{}] to [{}:{}]", machine_index,
              remote_index, flow_to_string(flow), ip, port,
              remote_machine_config.ip, remote_port);

    ret = machnet_connect(channel, ip.c_str(), remote_machine_config.ip.c_str(),
                          remote_port, &flow);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 100);
    auto val = dis(gen);
    std::this_thread::sleep_for(std::chrono::milliseconds(val));
    if (ret == 0)
    {
      break;
    }
  }
  assert_with_msg(ret == 0, fmt::format("machnet_connect() failed [{}] [{}]", remote_machine_config.ip, remote_port).c_str());

  info("[{}-{}] {} Connected from [{}:{}] to [{}:{}]", machine_index,
            remote_index, flow_to_string(flow), ip, port,
            remote_machine_config.ip, remote_port);

  machine_index_to_connection[{remote_index, remote_port}] = connection_data;
}

void Connection::listen()
{
  auto my_machine_config = config.remote_machine_configs[machine_index];
  auto ip = my_machine_config.ip;
  auto port = static_cast<int>(my_machine_config.port + thread_index);

  auto connection_data = ConnectionData{};
  auto &[flow] = connection_data;

  LOG_STATE("[{}] {} Attempting to listen on [{}:{}]", machine_index,
            flow_to_string(flow), ip, port);

  auto ret = machnet_listen(channel, ip.c_str(), port);
  assert_with_msg(ret == 0, "machnet_listen() failed");

  LOG_STATE("[{}] {} Listening on [{}:{}]", machine_index, flow_to_string(flow),
            ip, port);

  machine_index_to_connection[{machine_index, port}] = connection_data;
}

void Connection::send(int index, int port, std::string_view data)
{
  auto &connection_data = machine_index_to_connection[{index, port}];
  auto &[flow] = connection_data;
  LOG_STATE("[{}-{}:{}] {} Sending size {}", machine_index, index, port,
            flow_to_string(flow), data.size());
  auto ret = -1; 
  while (ret < 0)
  {
    ret = machnet_send(channel, flow, data.data(), data.size());
    if (ret < 0)
    {
	    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      info("machnet_send() {} SIZE {} ? MAX_SIZE {}", ret, data.size(), MACHNET_MSG_MAX_LEN);
    }
  }
  // if (ret < 0)
  // {
  //   panic("machnet_send() failed");
  // }

  LOG_STATE("[{}-{}:{}] {} Sent size {}", machine_index, index, port, flow_to_string(flow),
          data.size());
}

void Connection::send(int index, std::string_view data)
{
  auto port = config.remote_machine_configs[index].port;
  send(index, port, data);
}

void Connection::put(int index, int thread_index, std::string_view key, std::string_view value)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  PutRequest::Builder put_request = data.initPutRequest();
  put_request.setKey(std::string(key));
  put_request.setValue(std::string(value));
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] Put request [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  auto port = config.remote_machine_configs[index].port + thread_index;
  send(index, port, std::string_view(p.begin(), p.end()));

  poll_receive([&](auto remote_index, auto remote_port, MachnetFlow &tx_flow, auto &&data)
               {
    if (data.isPutResponse()) {
      auto p = data.getPutResponse();
      LOG_STATE("[{}-{}:{}] Put response", machine_index, index, remote_port);
    } else {
      info("Unexpected response");
    }
  });
}

std::string Connection::get(int index, int thread_index, std::string_view key)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  GetRequest::Builder get_request = data.initGetRequest();
  get_request.setKey(std::string(key));
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] Get request [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  auto port = config.remote_machine_configs[index].port + thread_index;
  send(index, port, std::string_view(p.begin(), p.end()));

  std::string value;
  poll_receive([&](auto remote_index, auto remote_port, MachnetFlow &tx_flow, auto &&data)
               {
    if (data.isGetResponse()) {
      auto p = data.getGetResponse();
      value = p.getValue().cStr();
      LOG_STATE("[{}-{}:{}] Get response [key = {}, value = {}]", machine_index,
            index, remote_port, key, value);
    } else {
      info("Unexpected response");
      info("Unexpected response: [{}-{}] -> [{}-{}] Get request [{}]", machine_index, index, remote_index, remote_port, kj::str(data).cStr());
    }
  });
  return value;
}

int Connection::use_next_port()
{
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0)
  {
    panic("could not open socket (%d) %s", errno, strerror(errno));
  }

  struct sockaddr_in serv_addr;
  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = 0;
  if (bind(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
  {
    panic("could not bind to process (%d) %s", errno, strerror(errno));
  }

  socklen_t len = sizeof(serv_addr);
  if (getsockname(sock, (struct sockaddr *)&serv_addr, &len) == -1)
  {
    panic("could not get socket name (%d) %s", errno, strerror(errno));
  }

  if (close(sock) < 0)
  {
    panic("could not close socket (%d) %s", errno, strerror(errno));
  }
  auto port = ntohs(serv_addr.sin_port);

  LOG_STATE("FREE PORT {}", port);
  return port;
}

Client::Client(BlockCacheConfig config, Configuration ops_config, int machine_index,
               int thread_index)
    : Connection(config, ops_config, machine_index, thread_index)
{
  for (auto i = 0; i < config.remote_machine_configs.size(); i++)
  {
    if (i != machine_index && config.remote_machine_configs[i].server)
    {
      connect_to_remote_machine(i);
    }
  }
}

void Client::connect_to_other_clients()
{
  LOG_STATE("Connecting to other clients");
  listen();
  for (auto i = 0; i < config.remote_machine_configs.size(); i++)
  {
    if (i != machine_index && !config.remote_machine_configs[i].server)
    {
      connect_to_remote_machine(i);
    }
  }
  LOG_STATE("Connecting to other clients");
}

void Client::sync_with_other_clients()
{
  auto total_other_clients = 0;
  for (auto i = 0; i < config.remote_machine_configs.size(); i++)
  {
    if (i != machine_index && !config.remote_machine_configs[i].server)
    {
      total_other_clients++;
    }
  }

  HashMap<int, bool> request_acks;
  HashMap<int, bool> response_acks;

  // Send requests
  for (auto i = 0; i < config.remote_machine_configs.size(); i++)
  {
    if (i != machine_index && !config.remote_machine_configs[i].server)
    {
      ::capnp::MallocMessageBuilder message;
      Packets::Builder packets = message.initRoot<Packets>();
      ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
      Packet::Data::Builder data = packet[0].initData();
      ClientSyncRequest::Builder client_sync_request = data.initClientSyncRequest();
      client_sync_request.setResponse(ResponseType::OK);
      client_sync_request.setIndex(machine_index);
      auto m = capnp::messageToFlatArray(message);
      auto p = m.asChars();

      info("[{}-{}] Client sync request [{}]", machine_index, i,
                kj::str(message.getRoot<Packets>()).cStr());

      send(i, std::string_view(p.begin(), p.end()));

      info("[{}-{}] Client sync request sent [{}]", machine_index, i,
                kj::str(message.getRoot<Packets>()).cStr());
    }
  }
  
  // Wait for requests and then send back responses
  for (auto i = 0; i < total_other_clients; i++)
  {
    poll_receive([&](auto remote_index, int remote_port, MachnetFlow &tx_flow, auto &&data)
    {
      if (data.isClientSyncRequest()) {
        auto p = data.getClientSyncRequest();
        auto index = p.getIndex();
        request_acks[index] = true;
      }
    });
  }

  // Check if the number of acks is equal to the number of other clients
  if (request_acks.size() != total_other_clients)
  {
    info("Client sync failed {} != {}", request_acks.size(), total_other_clients);
  }
}

Server::Server(BlockCacheConfig config, Configuration ops_config, int machine_index,
               int thread_index, std::shared_ptr<BlockCache<std::string, std::string>> block_cache_)
    : Connection(config, ops_config, machine_index, thread_index), block_cache(block_cache_)
{
  listen();
  for (auto i = 0; i < config.remote_machine_configs.size(); i++)
  {
    if (i != machine_index && config.remote_machine_configs[i].server)
    {
      connect_to_remote_machine(i);
    }
  }
}

void Server::put_response(int index, int port, ResponseType response_type)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  PutResponse::Builder put_response = data.initPutResponse();
  put_response.setResponse(response_type);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}:{}] Put response [{}]", machine_index, index, port,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end()));

  LOG_STATE("[{}-{}:{}] Put response sent [{}]", machine_index, index, port,
            kj::str(message.getRoot<Packets>()).cStr());
}

void Server::put_response(int index, ResponseType response_type)
{
  auto port = config.remote_machine_configs[index].port;
  put_response(index, port, response_type);
}

void Server::get_response(int index, int port, ResponseType response_type,
                          std::string_view value)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  GetResponse::Builder get_response = data.initGetResponse();
  get_response.setResponse(response_type);
  get_response.setValue(std::string(value));
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}:{}] Get response [{}]", machine_index, index, port,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end()));

  LOG_STATE("[{}-{}:{}] Get response sent [{}]", machine_index, index, port,
            kj::str(message.getRoot<Packets>()).cStr());
}

void Server::get_response(int index, ResponseType response_type, std::string_view value)
{
  auto port = config.remote_machine_configs[index].port;
  get_response(index, port, response_type, value);
}

void Server::pass_write_to_server_request(int index, int port, std::string_view key)
{
  // ::capnp::MallocMessageBuilder message;
  // Packets::Builder packets = message.initRoot<Packets>();
  // ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  // Packet::Data::Builder data = packet[0].initData();
  // SingletonPutRequest::Builder request = data.initSingletonPutRequest();
  // request.setKey(std::string(key));
  // request.setValue(std::string(value));
  // request.setSingleton(singleton);
  // request.setForwardCount(forward_count);
  // auto m = capnp::messageToFlatArray(message);
  // auto p = m.asChars();

  // LOG_STATE("[{}-{}] Singleton Put Request [{}]", machine_index, index,
  //           kj::str(message.getRoot<Packets>()).cStr());

  // send(index, port, std::string_view(p.begin(), p.end())); 
}

void Server::rdma_setup_request(int index, int my_index, uint64_t start_address,
                                uint64_t size)
{
  panic("Deprecated");
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  RdmaSetupRequest::Builder request = data.initRdmaSetupRequest();
  request.setMachineIndex(my_index);
  request.setStartAddress(start_address);
  request.setSize(size);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] RDMA setup request [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, std::string_view(p.begin(), p.end()));
}

void Server::rdma_setup_response(int index, ResponseType response_type)
{
  panic("Deprecated");
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  RdmaSetupResponse::Builder response = data.initRdmaSetupResponse();
  response.setResponse(response_type);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] RDMA setup response [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, std::string_view(p.begin(), p.end()));
}

void Server::singleton_put_request(int index, int port, std::string_view key,
                                   std::string_view value, bool singleton, uint64_t forward_count)
{
  ::capnp::MallocMessageBuilder message;
  // info("building packets");
  Packets::Builder packets = message.initRoot<Packets>();
  // info("building packets list");
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  // info("building data");
  Packet::Data::Builder data = packet[0].initData();
  // info("building singleton put request");
  SingletonPutRequest::Builder request = data.initSingletonPutRequest();
  // info("adding key to request");
  request.setKey(std::string(key));
  // info("adding value to request");
  request.setValue(std::string(value));
  // info("adding singleton to request");
  request.setSingleton(singleton);
  // info("adding forward_count to request");
  request.setForwardCount(forward_count);
  // info("messageToFlatArray");
  auto m = capnp::messageToFlatArray(message);
  // info("converting to chars");
  auto p = m.asChars();

  LOG_STATE("[{}-{}] Singleton Put Request [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Server::delete_request(int index, int port, std::string_view key)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  DeleteRequest::Builder request = data.initDeleteRequest();
  request.setKey(std::string(key));
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] Delete Request [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Server::fallback_get_request(int index, int port, std::string_view key)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  FallbackGetRequest::Builder request = data.initFallbackGetRequest();
  request.setKey(std::string(key));
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] FallbackGetRequest [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Server::fallback_get_response(int index, int port, std::string_view key, std::string_view value, uint64_t key_value_ptr_offset, bool singleton, uint64_t forward_count)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  FallbackGetResponse::Builder request = data.initFallbackGetResponse();
  request.setResponse(ResponseType::OK);
  request.setKey(std::string(key));
  request.setValue(std::string(value));
  request.setKeyValuePtrOffset(key_value_ptr_offset);
  request.setSingleton(singleton);
  request.setForwardCount(forward_count);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] FallbackGetResponse [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::shared_log_forward_request(int index, int port, std::string_view key, uint64_t hash)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  SharedLogForwardRequest::Builder request = data.initSharedLogForwardRequest();
  request.setKey(std::string(key));
  request.setHash(hash);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] SharedLogForwardRequest [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::shared_log_forward_response(int index, int port, ResponseType response_type, uint64_t hash)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  SharedLogForwardResponse::Builder request = data.initSharedLogForwardResponse();
  request.setResponse(response_type);
  request.setHash(hash);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] SharedLogForwardResponse [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::shared_log_put_request(int index, int port, std::string_view key, std::string_view value, uint64_t hash)
{
  SharedLogPutRequestEntry e{std::string(key), std::string(value), hash};
  shared_log_put_request(index, port, {e});
}

void Connection::shared_log_put_response(int index, int port, uint64_t shared_log_index, uint64_t hash)
{
  SharedLogPutResponseEntry e{shared_log_index, hash};
  shared_log_put_response(index, port, {e});
}

void Connection::shared_log_put_request(int index, int port, std::vector<SharedLogPutRequestEntry> entries)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  SharedLogPutRequest::Builder request = data.initSharedLogPutRequest();
  ::capnp::List<SharedLogPutRequestE>::Builder shared_log_entries = request.initE(entries.size());
  for (auto i = 0; i < entries.size(); i++)
  {
    SharedLogPutRequestE::Builder shared_log_entry = shared_log_entries[i];
    const auto& e = entries[i];

    shared_log_entry.setKey(e.key);
#ifdef COMPRESS_SHARED_LOG
    shared_log_entry.setValue("");
#else
    shared_log_entry.setValue(e.value);
#endif
    shared_log_entry.setHash(e.hash);
  }
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] SharedLogPutRequest [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::shared_log_put_response(int index, int port, std::vector<SharedLogPutResponseEntry> entries)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  SharedLogPutResponse::Builder request = data.initSharedLogPutResponse();
  ::capnp::List<SharedLogPutResponseE>::Builder shared_log_entries = request.initE(entries.size());
  for (auto i = 0; i < entries.size(); i++)
  {
    SharedLogPutResponseE::Builder shared_log_entry = shared_log_entries[i];
    const auto& e = entries[i];

    shared_log_entry.setIndex(e.index);
    shared_log_entry.setHash(e.hash);
  }
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] SharedLogPutRequest [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::shared_log_get_request(int index, int port, uint64_t shared_log_index)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  SharedLogGetRequest::Builder request = data.initSharedLogGetRequest();
  request.setIndex(shared_log_index);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] SharedLogGetRequest [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::shared_log_get_response(int index, int port, uint64_t shared_log_index, uint64_t server_shared_log_index, std::vector<KeyValueEntry> entries)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  SharedLogGetResponse::Builder request = data.initSharedLogGetResponse();
  request.setIndex(shared_log_index);
  request.setLogIndex(server_shared_log_index);

  ::capnp::List<SharedLogEntry>::Builder shared_log_entries = request.initE(entries.size());
  for (auto i = 0; i < entries.size(); i++)
  {
    SharedLogEntry::Builder shared_log_entry = shared_log_entries[i];
    const auto& e = entries[i];

    shared_log_entry.setKey(e.key);
#ifndef COMPRESS_SHARED_LOG
    shared_log_entry.setValue(e.value);
#else
    shared_log_entry.setValue("");
#endif
  }

  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] SharedLogGetResponse [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

// TODO: Add craq functions here
void Connection::craq_forward_propagate_request(int index, int port, std::string_view key, std::string_view value, uint64_t version, uint64_t client_index, uint64_t client_port)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  CraqForwardPropagateRequest::Builder request = data.initCraqForwardPropagateRequest();
  request.setKey(std::string(key));
  request.setValue(std::string(value));
  request.setVersion(version);
  request.setClientIndex(client_index);
  request.setClientPort(client_port);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] CraqForwardPropagateRequest [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::craq_backward_propagate_request(int index, int port, std::string_view key, uint64_t latest_clean_version, uint64_t client_index, uint64_t client_port)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  CraqBackwardPropagateRequest::Builder request = data.initCraqBackwardPropagateRequest();
  request.setKey(std::string(key));
  request.setLatestCleanVersion(latest_clean_version);
  request.setClientIndex(client_index);
  request.setClientPort(client_port);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] CraqBackwardPropagateRequest [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::craq_version_request(int index, int port, std::string_view key, uint64_t client_index, uint64_t client_port)
{
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  CraqVersionRequest::Builder request = data.initCraqVersionRequest();
  request.setKey(std::string(key));
  request.setClientIndex(client_index);
  request.setClientPort(client_port);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] CraqVersionRequest [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Connection::craq_version_response(int index, int port, std::string_view key, std::string_view value, uint64_t version, uint64_t client_index, uint64_t client_port) {
  ::capnp::MallocMessageBuilder message;
  Packets::Builder packets = message.initRoot<Packets>();
  ::capnp::List<Packet>::Builder packet = packets.initPackets(1);
  Packet::Data::Builder data = packet[0].initData();
  CraqVersionResponse::Builder request = data.initCraqVersionResponse();
  request.setKey(std::string(key));
  request.setValue(std::string(value));
  request.setVersion(version);
  request.setClientIndex(client_index);
  request.setClientPort(client_port);
  auto m = capnp::messageToFlatArray(message);
  auto p = m.asChars();

  LOG_STATE("[{}-{}] CraqVersionResponse [{}]", machine_index, index,
            kj::str(message.getRoot<Packets>()).cStr());

  send(index, port, std::string_view(p.begin(), p.end())); 
}

void Server::execute_pending_operations()
{
  Connection::execute_pending_operations();
  RDMAGetResponse response;
  while (rdma_get_response_queue.try_dequeue(response))
  {
    auto [index, port, response_type, value] = response;
    if (response_type != ResponseType::OK)
    {
      panic("RDMA get failed");
    }
    LOG_STATE("[{}-{}] Execute pending operation [{}]", machine_index, index, value);
    get_response(index, port, response_type, value);
  }

  if (config.db.block_db.async)
  {
    BlockCacheRequest block_cache_request;
    while (block_cache_request_queue.try_dequeue(block_cache_request))
    {
      auto [index, port, response_type, key, value] = block_cache_request;
      if (response_type != ResponseType::OK)
      {
        panic("Disk get failed");
      }
      LOG_STATE("[{}-{}] Execute pending operation [{}]", machine_index, index, value);
      get_response(index, port, response_type, value);
    }
  }

  {
    AppendSingletonPutRequest request;
    while (singleton_put_request_queue.try_dequeue(request))
    {
      auto [index, port, response_type, key, value, singleton, forward_count] = request;
      if (response_type != ResponseType::OK)
      {
        panic("Singleton put failed");
      }
      LOG_STATE("[{}-{}] Execute pending operation [{}]", machine_index, index, value);
      singleton_put_request(index, port, key, value, singleton, forward_count);
    }
  }

  {
    AppendFallbackGetRequest request;
    while (fallback_get_request_queue.try_dequeue(request))
    {
      auto [index, port, key] = request;
      fallback_get_request(index, port, key);
    }
  }

  {
    AppendDeleteRequest request;
    while (delete_request_queue.try_dequeue(request))
    {
      auto [index, port, key] = request;
      // LOG_STATE("[{}-{}] Execute pending operation [{}]", machine_index, index);
      delete_request(index, port, key);
    }
  }

  {
    SharedLogGetRequest request;
    while (shared_log_get_request_queue.try_dequeue(request))
    {
      auto [index, port, shared_log_index] = request;
      shared_log_get_request(index, port, shared_log_index);
    }
  }

  {
    AppendPutResponse request;
    while (append_put_response_queue.try_dequeue(request))
    {
      auto [index, port, response_type] = request;
      put_response(index, port, response_type);
    }
  }
}

void Server::append_to_rdma_get_response_queue(int index, int port, ResponseType response_type,
                                               std::string_view value)
{
  auto response = Server::RDMAGetResponse{index, port, response_type, std::string(value)};
  rdma_get_response_queue.enqueue(response);

  remote_rdma_cache_hits++;
}

json Server::get_stats()
{
  json j;
  j["remote_rdma_cache_hits"] = remote_rdma_cache_hits;
  j["async_disk_requests"] = async_disk_requests;
  return j;
}

void Server::append_to_rdma_block_cache_request_queue(int index, int port, ResponseType response_type, std::string_view key, std::string_view value)
{
  auto request = Server::BlockCacheRequest{index, port, response_type, std::string(key), std::string(value)};
  block_cache_request_queue.enqueue(request);

  async_disk_requests++;
}

void Server::append_singleton_put_request(int index, int port, std::string_view key,
                                  std::string_view value, bool singleton, uint64_t forward_count)
{
  auto request = Server::AppendSingletonPutRequest{index, port, ResponseType::OK, std::string(key), std::string(value), singleton, forward_count};
  singleton_put_request_queue.enqueue(request);
}

void Server::append_fallback_get_request(int index, int port, std::string_view key)
{
  auto request = Server::AppendFallbackGetRequest{index, port, std::string(key)};
  fallback_get_request_queue.enqueue(request);
}

void Server::append_delete_request(int index, int port, std::string_view key)
{
  auto request = Server::AppendDeleteRequest{index, port, std::string(key)};
  delete_request_queue.enqueue(request);
}

void Server::append_shared_log_get_request(int index, int port, uint64_t shared_log_index)
{
  auto request = Server::SharedLogGetRequest{index, port, shared_log_index};
  shared_log_get_request_queue.enqueue(request);
}

void Server::append_put_response(int index, int port, ResponseType response_type)
{
  auto request = Server::AppendPutResponse{index, port, response_type};
  append_put_response_queue.enqueue(request);
}
