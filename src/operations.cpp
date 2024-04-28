#include "ldc.h"

// Function to generate the operation set
Operations
generateRandomOperationSet(const std::vector<std::string> &keys,
                           Configuration &config) {
  Operations operationSet;
  int numNodes = config.NUM_NODES;
  int totalOps = config.TOTAL_OPERATIONS;

  for (int i = 0; i < totalOps; i++) {
    const std::string &key = keys[rand() % keys.size()];
    int randomNode = rand() % numNodes + 1; // Node numbers start from 1
    operationSet.push_back({key, randomNode, READ_OP});
  }

  dumpOperationSetToFile(operationSet);
  return operationSet;
}

// Function to generate a partitioned operation set
Operations
generatePartitionedOperationSet(const std::vector<std::string> &keys,
                                Configuration &config) {
  Operations operationSet;
  int numNodes = config.NUM_NODES;
  int totalOps = config.TOTAL_OPERATIONS;

  int keysPerNode = keys.size() / numNodes; // Number of keys per node

  for (int i = 0; i < totalOps; i++) {
    const std::string &key = keys[i % keys.size()];
    int node = (static_cast<int>(std::floor(
                   static_cast<double>(std::stoi(key)) / keysPerNode))) %
                   numNodes +
               1;
    operationSet.push_back({key, node, READ_OP});
  }

  dumpOperationSetToFile(operationSet);
  return operationSet;
}

// Function to generate a Zipfian-distributed operation set
Operations
generateZipfianOperationSet(const std::vector<std::string> &keys,
                            Configuration &config) {
  Operations operationSet;
  int numKeys = keys.size();
  int numNodes = config.NUM_NODES;
  int totalOps = config.TOTAL_OPERATIONS;

  // Calculate the number of hot keys and cold keys based on the percentages
  int numHotKeys = static_cast<int>(config.HOT_KEY_PERCENTAGE * numKeys);
  int numColdKeys = numKeys - numHotKeys;

  // Generate a random order of keys, where hot keys come first and then cold
  // keys
  std::vector<int> keyOrder(numKeys);
  for (int i = 0; i < numKeys; i++) {
    keyOrder[i] = i;
  }
  std::random_shuffle(keyOrder.begin(), keyOrder.end());

  int hotKeyOps = static_cast<int>(config.HOT_KEY_ACCESS_PERCENTAGE * totalOps);

  for (int i = 0; i < totalOps; i++) {
    int randomIndex;

    // Determine if the access is for a hot key or cold key
    if (i < hotKeyOps) {
      randomIndex = keyOrder[rand() % numHotKeys];
    } else {
      randomIndex = keyOrder[numHotKeys + rand() % numColdKeys];
    }

    const std::string &key = keys[randomIndex];
    int randomNode = rand() % numNodes + 1;
    operationSet.push_back({key, randomNode, READ_OP});
  }

  dumpOperationSetToFile(operationSet);
  return operationSet;
}

// Function to generate a Zipfian-distributed partitioned operation set
Operations
generateZipfianPartitionedOperationSet(const std::vector<std::string> &keys,
                                       Configuration &config) {
  Operations operationSet;
  int numKeys = keys.size();
  int numNodes = config.NUM_NODES;
  int totalOps = config.TOTAL_OPERATIONS;

  // Calculate the number of hot keys and cold keys based on the percentages
  int numHotKeys = static_cast<int>(config.HOT_KEY_PERCENTAGE * numKeys);
  int numColdKeys = numKeys - numHotKeys;
  int keysPerNode = keys.size() / numNodes;

  // Generate a random order of keys, where hot keys come first and then cold
  // keys
  std::vector<int> keyOrder(numKeys);
  for (int i = 0; i < numKeys; i++) {
    keyOrder[i] = i;
  }
  std::random_shuffle(keyOrder.begin(), keyOrder.end());

  int hotKeyOps = static_cast<int>(config.HOT_KEY_ACCESS_PERCENTAGE * totalOps);

  for (int i = 0; i < totalOps; i++) {
    int randomIndex;

    // Determine if the access is for a hot key or cold key
    if (i < hotKeyOps) {
      randomIndex = keyOrder[rand() % numHotKeys];
    } else {
      randomIndex = keyOrder[numHotKeys + rand() % numColdKeys];
    }
    const std::string &key = keys[randomIndex];
    int node = (static_cast<int>(std::floor(
                   static_cast<double>(std::stoi(key)) / keysPerNode))) %
                   numNodes +
               1;
    operationSet.push_back({key, node, READ_OP});
  }

  dumpOperationSetToFile(operationSet);
  return operationSet;
}

Operations
singleNodeHotSetData(const std::vector<std::string> &keys,
                     Configuration &config) {
  Operations operationSet;
  int numKeys = keys.size();
  int numNodes = config.NUM_NODES;
  int totalOps = config.TOTAL_OPERATIONS;

  // Divide the given keyset into 3 parts and select the middle part as the new
  // keyset
  int startIdx = numKeys / 3;
  int endIdx = 2 * numKeys / 3;
  std::vector<std::string> newKeySet(keys.begin() + startIdx,
                                     keys.begin() + endIdx);

  // Calculate the number of hot keys and cold keys based on the percentages
  int numHotKeys =
      static_cast<int>(config.HOT_KEY_PERCENTAGE * newKeySet.size());
  int numColdKeys = numKeys - numHotKeys;

  // Create a key order where newKeySet comes first, followed by the remaining
  // keys from the original keyset
  std::vector<std::string> keyOrder;
  keyOrder.reserve(numKeys);
  keyOrder.insert(keyOrder.end(), newKeySet.begin(),
                  newKeySet.end()); // New keyset
  for (int i = 0; i < numKeys; i++) {
    if (i < startIdx || i >= endIdx) {
      keyOrder.push_back(keys[i]); // Old keyset, not in newKeySet
    }
  }

  int hotKeyOps = static_cast<int>(config.HOT_KEY_ACCESS_PERCENTAGE * totalOps);

  for (int i = 0; i < totalOps; i++) {
    int randomIndex;

    // Determine if the access is for a hot key or cold key
    if (i < hotKeyOps) {
      randomIndex = rand() % (numHotKeys);
    } else {
      randomIndex = numHotKeys + rand() % numColdKeys;
    }
    const std::string &key = keyOrder[randomIndex];

    // std::cout << "rand() \% numHotKeys: " << (rand() % numHotKeys) << "
    // randomIndex: " << randomIndex << "\n";

    int randomNode = rand() % numNodes + 1;
    operationSet.push_back({key, randomNode, READ_OP});
  }

  dumpOperationSetToFile(operationSet);
  return operationSet;
}

Operations
singleNodeHotSetDataToSecondNodeOnly(const std::vector<std::string> &keys,
                     Configuration &config) {
  Operations operationSet;
  int numKeys = keys.size();
  int numNodes = config.NUM_NODES;
  int totalOps = config.TOTAL_OPERATIONS;

  // Divide the given keyset into 3 parts and select the middle part as the new
  // keyset
  int startIdx = numKeys / 3;
  int endIdx = 2 * numKeys / 3;
  std::vector<std::string> newKeySet(keys.begin() + startIdx,
                                     keys.begin() + endIdx);

  // Calculate the number of hot keys and cold keys based on the percentages
  int numHotKeys =
      static_cast<int>(config.HOT_KEY_PERCENTAGE * newKeySet.size());
  int numColdKeys = numKeys - numHotKeys;

  // Create a key order where newKeySet comes first, followed by the remaining
  // keys from the original keyset
  std::vector<std::string> keyOrder;
  keyOrder.reserve(numKeys);
  keyOrder.insert(keyOrder.end(), newKeySet.begin(),
                  newKeySet.end()); // New keyset
  for (int i = 0; i < numKeys; i++) {
    if (i < startIdx || i >= endIdx) {
      keyOrder.push_back(keys[i]); // Old keyset, not in newKeySet
    }
  }

  int hotKeyOps = static_cast<int>(config.HOT_KEY_ACCESS_PERCENTAGE * totalOps);

  for (int i = 0; i < totalOps; i++) {
    int randomIndex;

    // Determine if the access is for a hot key or cold key
    if (i < hotKeyOps) {
      randomIndex = rand() % (numHotKeys);
    } else {
      randomIndex = numHotKeys + rand() % numColdKeys;
    }
    const std::string &key = keyOrder[randomIndex];
    
    int randomNode = 2;
    operationSet.push_back({key, randomNode, READ_OP});
  }

  dumpOperationSetToFile(operationSet);
  return operationSet;
}

Operations
singleNodeHotSetDataToSecondNodeOnlyRDMA(const std::vector<std::string> &keys,
                     Configuration &config) {
  Operations operationSet;
  int numKeys = keys.size();
  int numNodes = config.NUM_NODES;
  int totalOps = config.TOTAL_OPERATIONS;

  // Divide the given keyset into 3 parts and select the middle part as the new
  // keyset
  int startIdx = numKeys / 3;
  int endIdx = 2 * numKeys / 3;
  std::vector<std::string> newKeySet(keys.begin() + startIdx,
                                     keys.begin() + endIdx);

  // Calculate the number of hot keys and cold keys based on the percentages
  int numHotKeys =
      static_cast<int>(config.HOT_KEY_PERCENTAGE * newKeySet.size());
  int numColdKeys = numKeys - numHotKeys;

  // Create a key order where newKeySet comes first, followed by the remaining
  // keys from the original keyset
  std::vector<std::string> keyOrder;
  keyOrder.reserve(numKeys);
  keyOrder.insert(keyOrder.end(), newKeySet.begin(),
                  newKeySet.end()); // New keyset
  for (int i = 0; i < numKeys; i++) {
    if (i < startIdx || i >= endIdx) {
      keyOrder.push_back(keys[i]); // Old keyset, not in newKeySet
    }
  }

  int hotKeyOps = static_cast<int>(config.HOT_KEY_ACCESS_PERCENTAGE * totalOps);

  for (int i = 0; i < totalOps; i++) {
    int randomIndex;

    // Determine if the access is for a hot key or cold key
    if (i < hotKeyOps) {
      randomIndex = rand() % (numHotKeys);
    } else {
      randomIndex = numHotKeys + rand() % numColdKeys;
    }
    const std::string &key = keyOrder[randomIndex];
    
    int randomNode = 1;
    operationSet.push_back({key, randomNode, READ_OP});
    LOG_STATE("key {} node {}", key, randomNode);
  }

  dumpOperationSetToFile(operationSet);
  return operationSet;
}

Operations
singleNodeHotSetDataTo80SecondNode20Other(const std::vector<std::string> &keys,
                                          Configuration &config) {
  srand(static_cast<unsigned>(time(0)));


  int totalThreads = config.HOT_THREAD + config.RDMA_THREAD;
  int opsPerThread = config.TOTAL_OPERATIONS / totalThreads;
  int adjustedTotalOps = opsPerThread * totalThreads;

  Operations operationSet;
  operationSet.reserve(adjustedTotalOps);

  // Shuffle thread identifiers
  std::vector<int> threadIDs(totalThreads);
  std::iota(threadIDs.begin(), threadIDs.end(), 0);
  std::random_shuffle(threadIDs.begin(), threadIDs.end());

  int startIdx = keys.size() / 3;
  int endIdx = 2 * keys.size() / 3;
  std::vector<std::string> newKeySet(keys.begin() + startIdx, keys.begin() + endIdx);
  int numHotKeys = static_cast<int>(config.HOT_KEY_PERCENTAGE * newKeySet.size());

  for (int threadID : threadIDs) {
      bool isHotThread = threadID < config.HOT_THREAD;
      int randomNode = isHotThread ? 2 : (rand() % 2 == 0 ? 1 : 3);

      for (int op = 0; op < opsPerThread; ++op) {
          int randomIndex = rand() % numHotKeys;
          const std::string& key = newKeySet[randomIndex];
          operationSet.emplace_back(key, randomNode);
      }
  }

  // Assuming dumpOperationSetToFile is defined elsewhere
  dumpOperationSetToFile(operationSet);
  return operationSet;
}

// Function to execute the operation set
void executeOperations(
    const Operations &operationSet, int client_start_index,
    BlockCacheConfig config, Configuration &ops_config, Client& client, int client_index_per_thread, int machine_index) {
  std::vector<long long> timeStamps;
  int operationNumber = 1;
  std::string value;
  auto io_start = std::chrono::high_resolution_clock::now();
  for (int j = 0; j < ops_config.VALUE_SIZE; j++) {
    // value += static_cast<char>(rand() % 26 + 'A'); // Random uppercase
    // letters
    value += static_cast<char>('A'); // Random uppercase letters
  }

  for (const auto &operation : operationSet) {
    io_start = std::chrono::high_resolution_clock::now();
    const auto &[key, Node, op] = operation;
    // std::cout << "Operation " << operationNumber << ": Sending key=" << key
    //           << " to Node " << Node << std::endl;
    if (Node > ops_config.NUM_NODES) {
      panic("Invalid node number {}", Node);
    }
    // Timer latency_timer;
    std::string v;
    auto index = Node;
    if (op == READ_OP)
    {
      v = client.get(index + client_start_index, 0, key);
    }
    else if (op == INSERT_OP || op == UPDATE_OP)
    {
      client.put(index + client_start_index, 0, key, value);
    }
    // static const auto& baseline = config.baseline.selected;
    // if (baseline == "random")
    // {
    //   auto index = 0;
    //   while (true)
    //   {
    //     const auto& c = config.remote_machine_configs;
    //     index = rand() % c.size();
    //     const auto& remote_machine_config = c[index];
    //     if (remote_machine_config.server)
    //     {
    //       break;
    //     }
    //   }
    //   v = client.get(index, key);
    // }
    // else if (baseline == "client_aware")
    // {
    //   auto index = Node;
    //   v = client.get(index, key);
    // }
    // else if (baseline == "ldc")
    // {
    //   auto index = Node;
    //   v = client.get(index, key);
    // }
    // else
    // {
    //   panic("Invalid baseline {}", baseline);
    // }
    if (v != value) {
    // if (v[0] != value[0])
      panic("unexpected data {} {}", v, value);
    }
    // perf_monitor.add_latency(latency_timer.elapsed());
    // perf_monitor.add_request();
    auto elapsed = std::chrono::high_resolution_clock::now() - io_start;
    long long nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    timeStamps.push_back(nanoseconds);
    operationNumber++;
  }
  // dump_per_thread_latency_to_file(timeStamps , client_index_per_thread, machine_index);
}

std::vector<Operations> divideOperationSetPerThread(const Operations &operationSet , int total_threads) {
  std::vector<Operations> dividedSets(total_threads);
  
  // Check if the total_threads is positive
    if (total_threads <= 0) {
        throw std::invalid_argument("total_threads must be positive");
    }

    // Calculate the number of operations per thread
    size_t operationsPerThread = operationSet.size() / total_threads;
    size_t extraOperations = operationSet.size() % total_threads;

    size_t start = 0; // Start index for slicing operationSet
    for (int i = 0; i < total_threads; ++i) {

        size_t end = start + operationsPerThread + (i < extraOperations ? 1 : 0);


        if (start < operationSet.size()) { // Check to prevent out-of-range access
            dividedSets[i].assign(operationSet.begin() + start, operationSet.begin() + end);
        }

        start = end;
    }

    return dividedSets;
}

void dumpOperationSetToFile(
    const Operations &operationSet) {
  std::ofstream file("operations.txt");
  if (!file.is_open()) {
    std::cerr << "Failed to open operation set file: "
              << "operations.txt" << std::endl;
    return;
  }

  for (const auto &operation : operationSet) {
    const auto& [key, node, op] = operation;
    file << key << ' ' << node << ' ' << op << '\n';
  }

  file.close();
}

Operations loadOperationSetFromFile(std::string p) {
  Operations operationSet;
  std::ifstream file(p);
  if (!file.is_open()) {
    std::cerr << "Failed to open operation set file: "
              << p << std::endl;
    return operationSet;
  }

  std::string key;
  int node;
  char op;
  while (file >> key >> node >> op) {
    operationSet.push_back({key, node, op});
  }

  file.close();
  return operationSet;
}

void dump_latency_to_file(const std::string &filename, const std::vector<long long>& timestamps) {
  // Open a file in write mode.
  std::ofstream file(filename);
  if (!file.is_open()) {
      std::cerr << "Failed to open file: " << filename << std::endl;
      return;
  }

  for (const auto& timestamp : timestamps) {
      file << timestamp << std::endl;
  }

  file.close();
  // std::cout << "Latency data has been successfully written to " << filename << std::endl;
}

void dump_per_thread_latency_to_file(const std::vector<long long>& timestamps,int client_index_per_thread, int machine_index ,int thread_index) {
  std::string filename = "latency_data_client_" + std::to_string(machine_index) + "_thread_" + std::to_string(thread_index) + "_client_index_per_thread_" + std::to_string(client_index_per_thread) + ".txt";
  dump_latency_to_file(filename, timestamps);
}