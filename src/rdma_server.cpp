#include <stdlib.h>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <netinet/in.h>
#include <cassert>
#include <condition_variable>
#include <mutex>

#include "ldc.h"
#include <infinity/infinity.h>

size_t buffer_size = size_t(6) * 1024 * 1024 * 1024;
std::mutex m;
std::condition_variable cv;
bool ready = false;

void * RDMA_Server_Init(int serverport, uint64_t buffer_size, int machine_index, Configuration ops_config)
{
    std::cout << "[RDMA SERVER] RDMA SERVER INIT";
	std::cout << "[server]: serverport = " << serverport << std::endl;
    std::cout << "[server]: buffer_size = " << buffer_size << std::endl;

    auto device_name = find_nic_containing(ops_config.infinity_bound_nic);
    if (!device_name.has_value())
    {
        panic("No device found with name: {}", ops_config.infinity_bound_nic);
    }
	infinity::core::Context *context = new infinity::core::Context(*device_name, ops_config.infinity_bound_device_port);
	infinity::queues::QueuePairFactory *qp_factory = new infinity::queues::QueuePairFactory(context);
	infinity::queues::QueuePair *qp1;
    infinity::queues::QueuePair *qp2;
    infinity::queues::QueuePair *qp3;

    std::cout << "[server]: qp = " << buffer_size << std::endl;
    

	infinity::memory::Buffer *rm_buffer = new infinity::memory::Buffer(context, buffer_size);
	infinity::memory::RegionToken *rm_buffer_token = rm_buffer->createRegionToken();


    infinity::memory::Buffer *receive_buffer = new infinity::memory::Buffer(context, 128 * sizeof(char));
	context->postReceiveBuffer(receive_buffer);

    {
        std::lock_guard lk(m);
        ready = true;        
    }
    cv.notify_one();

	qp_factory->bindToPort(serverport);
    std::cout << "[RDMA SERVER] RDMA bindToPort" << std::endl;
	qp1 = qp_factory->acceptIncomingConnection(rm_buffer_token, sizeof(infinity::memory::RegionToken));
    std::cout << "[RDMA SERVER] Connection 1" << std::endl;
    qp2 = qp_factory->acceptIncomingConnection(rm_buffer_token, sizeof(infinity::memory::RegionToken));
    std::cout << "[RDMA SERVER] Connection 2" << std::endl;
    qp3 = qp_factory->acceptIncomingConnection(rm_buffer_token, sizeof(infinity::memory::RegionToken));
    std::cout << "[RDMA SERVER] Connection 3" << std::endl;
    std::cout << "[RDMA SERVER] RDMA acceptIncomingConnection" << std::endl;

    //local node details
    // local_node.context = context;
    // local.remote_buffer_token = rm_buffer_token;
    // local_node.isLocal = true;
    // local_node.local_memory_region = rm_buffer;


	// wait till terminate signal
	// infinity::core::receive_element_t receive_element;
	// while (!context->receive(&receive_element))
	// 	;
	return rm_buffer->getData();
}

HashMap<uint64_t, RDMA_connect>  connect_to_servers(
    BlockCacheConfig config, int machine_index, int value_size, Configuration ops_config,
    std::shared_ptr<BlockCache<std::string, std::string>> block_cache)
{
    std::unique_lock lk(m);
    cv.wait(lk, []{ return ready; });

    HashMap<uint64_t, RDMA_connect> rdma_nodes;

    float cache_size = 1.0;
    if (config.baseline.selected == "random")
        cache_size = config.cache.random.cache_size;
    if (config.baseline.selected == "lru")
        cache_size = config.cache.lru.cache_size;
    if (config.baseline.selected == "split")
        cache_size = config.cache.split.cache_size;
    if (config.baseline.selected == "thread_safe_lru")
        cache_size = config.cache.thread_safe_lru.cache_size;
    // if (config.baseline.selected == "paged")
    //     cache_size = config.cache.paged.cache_size;
    // if (config.baseline.selected == "rdma")
    //     cache_size = config.cache.rdma.cache_size;
    auto device_name = find_nic_containing(ops_config.infinity_bound_nic);
    if (!device_name.has_value())
    {
        panic("No device found with name: {}", ops_config.infinity_bound_nic);
    }
    auto *context = new infinity::core::Context(*device_name, ops_config.infinity_bound_device_port);
    infinity::memory::Buffer *buffer_to_receive = new infinity::memory::Buffer(context, 4096 * sizeof(char));
    context->postReceiveBuffer(buffer_to_receive);

    auto *qpf = new infinity::queues::QueuePairFactory(context);
    for(auto &t : config.remote_machine_configs) {
        std::cout << "adding node "<< t.ip<< std::endl; 
        if(t.server && !t.shared_log){

            std::cout << "created node"<< t.ip<< std::endl; 
            RDMA_connect node;
            node.ip = t.ip;
            node.index = t.index;
            node.context = context;
            node.qp_factory = qpf;
            node.isLocal = false;
            auto size = BLKSZ;
            node.tmp_buffer = malloc(size);
            node.buffer = new infinity::memory::Buffer(node.context, node.tmp_buffer, size);
            std::cout << "connectToRemoteHost"<< t.ip<< std::endl;
            node.qp = node.qp_factory->connectToRemoteHost(t.ip.c_str(), 50000);
            std::cout << "connectToRemoteHost end"<< t.ip<< std::endl;
            node.remote_buffer_token = (infinity::memory::RegionToken *)node.qp->getUserData();
            if (!config.baseline.use_cache_indexing)
            {
                node.circularBuffer = new AsyncRdmaOpCircularBuffer(RDMA_ASYNC_BUFFER_SIZE, value_size, node.context);
            }

            std::cout << "created new Buffers"<<std::endl;
            node.free_blocks = static_cast<u_int64_t>((cache_size * GB_TO_BYTES)/config.db.block_db.block_size);
            if (t.index == machine_index) {
                node.isLocal = true;
                // node.local_memory_region = node.remote_buffer_token->getMemoryRegion()->getAddress();
            }
            rdma_nodes[node.index] = node;
        }
    }

	std::cout << "[client]: initialized rdma connections" << std::endl<< std::endl<< std::endl;
    return rdma_nodes;
}

void printRDMAConnect(const RDMA_connect& conn) {
    if(conn.isLocal){
        std::cout << "Local Access:" << std::endl;
        std::cout << "local address: " << (void *) conn.local_memory_region << std::endl;
        return;
    }
    std::cout << "RDMA Connect:" << std::endl;
    std::cout << "IP: " << conn.ip << std::endl;
    std::cout << "Index: " << conn.index << std::endl;
    std::cout << "Context: " << (void*)conn.context << std::endl;
    std::cout << "Queue Pair: " << (void*)conn.qp << std::endl;
    std::cout << "Queue ip: " << conn.qp->getRemoteAddr() << std::endl;
    std::cout << "Free Blocks: " << static_cast<u_int64_t>(conn.free_blocks) << std::endl;
    std::cout << "Queue Pair Factory: " << (void*)conn.qp_factory << std::endl;
    std::cout << "Remote Buffer Token: " << (void*)conn.remote_buffer_token << std::endl;
    std::cout << std::endl;
    std::cout << std::endl;
}

void printBuffer(const std::array<uint8_t, BLKSZ>& buffer) {
    for(size_t i = 0; i < buffer.size(); ++i) {
        std::cout << static_cast<int>(buffer[i]) << " ";
        if ((i + 1) % 16 == 0) { // New line every 16 elements for better readability
            std::cout << std::endl;
        }
    }
}