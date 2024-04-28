#pragma once

#include <atomic>
#include <vector>
#include <mutex>
#include <thread>
#include <condition_variable>

// Defines
#define LOG_STATE(...)
// #define LOG_STATE info
// #define LOG_STATE(...) debug(__VA_ARGS__)
// #define LOG_STATE(...) info(__VA_ARGS__)
#define LOG_RDMA_DATA
// #define LOG_RDMA_DATA info

#define BLKSZ 4096

#define WARMUP_TIME_IN_SECONDS 60

#define GB_TO_BYTES (1024 * 1024 * 1024)
#define LOCAL_RDMA_BUFFER_SIZE 20
#define RDMA_PORT 50000
#define NUM_SERVERS 3
#define RDMA_ASYNC_BUFFER_SIZE 512
// #define PRINT_RDMA_LATENCY

// Each client will sync with other clients (before starting workload and ending workload)
#define CLIENT_SYNC_WITH_OTHER_CLIENTS

// Initialize clients in parallel
#define INIT_CLIENTS_IN_PARALLEL

// Use separate threads for submitting and polling io_uring 
// #define IO_URING_SUBMITTING_THREAD

// Stop the program, check this if we get SIGINT
extern std::atomic<bool> g_stop;

extern std::atomic<bool> keepPolling;
extern std::mutex mtx;
extern std::condition_variable cvs;
extern std::vector<std::thread> pollingThread;