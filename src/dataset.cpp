// dataset.cpp

#include "ldc.h"

// Function to read keys from the dataset file
std::vector<std::string> readKeysFromFile(const std::string &datasetFile)
{
  std::vector<std::string> keys;

  // Open the dataset file for reading
  std::ifstream file(datasetFile);
  if (!file.is_open())
  {
    std::cerr << "Failed to open dataset file: " << datasetFile << std::endl;
    return keys;
  }

  std::string line;
  while (std::getline(file, line))
  {
    // Split each line into key and value (assuming space-separated format)
    std::istringstream iss(line);
    std::string key;
    if (iss >> key)
    {
      keys.push_back(key);
    }
  }

  file.close();
  return keys;
}

// Function to create the dataset and write it to a file
void createAndWriteDataset(const std::string &datasetFile, int numberOfKeys,
                           int keySize, int valueSize)
{
  std::ofstream file(datasetFile);
  if (!file.is_open())
  {
    std::cerr << "Failed to open dataset file: " << datasetFile << std::endl;
    return;
  }

  std::string value;

  for (int j = 0; j < valueSize; j++)
  {
    value += static_cast<char>(rand() % 26 + 'A'); // Random uppercase letters
  }

  for (int i = 1; i < numberOfKeys + 1; i++)
  {
    // Generate a sequential key and a random value
    std::string key = std::to_string(i);
    file << key << ' ' << value
         << '\n'; // Write key and value to the dataset file
  }

  file.close();
}

std::vector<std::string> load_database(Configuration &ops_config,
                                       Client &client)
{
  createAndWriteDataset(ops_config.DATASET_FILE, ops_config.NUM_KEY_VALUE_PAIRS,
                        ops_config.KEY_SIZE, ops_config.VALUE_SIZE);
  std::vector<std::string> keys = readKeysFromFile(ops_config.DATASET_FILE);

  std::string value;

  for (int j = 0; j < ops_config.VALUE_SIZE; j++)
  {
    // value += static_cast<char>(rand() % 26 + 'A'); // Random uppercase
    // letters
    value += static_cast<char>('A'); // Random uppercase letters
  }

  for (const std::string &key : keys)
  {
    client.put(1, 0, key, value);
    client.put(2, 0, key, value);
    client.put(3, 0, key, value);
  }

  return keys;
}

int generateDatabaseAndOperationSet(Configuration &ops_config)
{
  createAndWriteDataset(ops_config.DATASET_FILE, ops_config.NUM_KEY_VALUE_PAIRS,
                        ops_config.KEY_SIZE, ops_config.VALUE_SIZE);
  std::vector<std::string> keys = readKeysFromFile(ops_config.DATASET_FILE);
  Operations operationSet;
  switch (ops_config.DISTRIBUTION_TYPE)
  {
  case RANDOM_DISTRIBUTION:
    operationSet = generateRandomOperationSet(keys, ops_config);
    break;
  case PARTITIONED_DISTRIBUTION:
    operationSet = generatePartitionedOperationSet(keys, ops_config);
    break;
  case ZIPFIAN_DISTRIBUTION:
    operationSet = generateZipfianOperationSet(keys, ops_config);
    break;
  case ZIPFIAN_PARTITIONED_DISTRIBUTION:
    operationSet = generateZipfianPartitionedOperationSet(keys, ops_config);
    break;
  case SINGLE_NODE_HOT_KEYS:
    operationSet = singleNodeHotSetData(keys, ops_config);
    break;
  case SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_ONLY:
    operationSet = singleNodeHotSetDataToSecondNodeOnly(keys, ops_config);
    break;
  case SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_RDMA_ONLY:
    operationSet = singleNodeHotSetDataToSecondNodeOnlyRDMA(keys, ops_config);
    break;
  case SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_SPLIT:
  info("SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_SPLIT");
    operationSet = singleNodeHotSetDataTo80SecondNode20Other(keys, ops_config);
    break;
  case YCSB:
    dumpOperationSetToFile(operationSet);
    break;
  default:
    std::cerr << "Invalid distribution type selected." << std::endl;
    return 1;
  }
  return 0;
}

int issueOps(BlockCacheConfig config, Configuration &ops_config, std::vector<std::string> &keys,
             Client client)
{
  Operations operationSet;
  switch (ops_config.DISTRIBUTION_TYPE)
  {
  case RANDOM_DISTRIBUTION:
    operationSet = generateRandomOperationSet(keys, ops_config);
    break;
  case PARTITIONED_DISTRIBUTION:
    operationSet = generatePartitionedOperationSet(keys, ops_config);
    break;
  case ZIPFIAN_DISTRIBUTION:
    operationSet = generateZipfianOperationSet(keys, ops_config);
    break;
  case ZIPFIAN_PARTITIONED_DISTRIBUTION:
    operationSet = generateZipfianPartitionedOperationSet(keys, ops_config);
    break;
  case SINGLE_NODE_HOT_KEYS:
    operationSet = singleNodeHotSetData(keys, ops_config);
    break;
  default:
    std::cerr << "Invalid distribution type selected." << std::endl;
    return 1;
  }
  // executeOperations(operationSet, 0, config, ops_config, client);
  return 0;
}

std::ostream &operator<<(std::ostream &os, const Configuration &config)
{
  os << "NUM_KEY_VALUE_PAIRS: " << config.NUM_KEY_VALUE_PAIRS << std::endl;
  os << "NUM_NODES: " << config.NUM_NODES << std::endl;
  os << "KEY_SIZE: " << config.KEY_SIZE << std::endl;
  os << "VALUE_SIZE: " << config.VALUE_SIZE << std::endl;
  os << "TOTAL_OPERATIONS: " << config.TOTAL_OPERATIONS << std::endl;
  os << "OP_FILE: " << config.OP_FILE << std::endl;
  os << "DATASET_FILE: " << config.DATASET_FILE << std::endl;
  os << "DISTRIBUTION_TYPE: " << config.DISTRIBUTION_TYPE << std::endl;
  os << "HOT_KEY_PERCENTAGE: " << config.HOT_KEY_PERCENTAGE << std::endl;
  os << "HOT_KEY_ACCESS_PERCENTAGE: " << config.HOT_KEY_ACCESS_PERCENTAGE
     << std::endl;
  return os;
}

Configuration parseConfigFile(const std::string &configFile)
{
  Configuration config;
  auto config_path = fs::path(configFile);
  std::ifstream file(config_path);
  if (!file.is_open())
  {
    std::cerr << "Error: Failed to open file:" << configFile << std::endl;
    exit(0);
  }

  json jsonData;
  file >> jsonData; // Assuming you have included nlohmann/json.hpp and properly
                    // set up the JSON library

  config.NUM_KEY_VALUE_PAIRS = jsonData["NUM_KEY_VALUE_PAIRS"];
  config.NUM_NODES = jsonData["NUM_NODES"];
  config.KEY_SIZE = jsonData["KEY_SIZE"];
  config.VALUE_SIZE = jsonData["VALUE_SIZE"];
  config.TOTAL_OPERATIONS = jsonData["TOTAL_OPERATIONS"];
  config.OP_FILE = jsonData["OP_FILE"];
  config.DATASET_FILE = jsonData["DATASET_FILE"];
  config.HOT_THREAD = jsonData["HOT_THREAD"];
  config.RDMA_THREAD = jsonData["RDMA_THREAD"];


  // Map JSON string to enum
  std::string distributionTypeStr = jsonData["DISTRIBUTION_TYPE"];

  if (distributionTypeStr == "RANDOM_DISTRIBUTION")
  {
    config.DISTRIBUTION_TYPE = RANDOM_DISTRIBUTION;
  }
  else if (distributionTypeStr == "PARTITIONED_DISTRIBUTION")
  {
    config.DISTRIBUTION_TYPE = PARTITIONED_DISTRIBUTION;
  }
  else if (distributionTypeStr == "ZIPFIAN_DISTRIBUTION")
  {
    config.DISTRIBUTION_TYPE = ZIPFIAN_DISTRIBUTION;
  }
  else if (distributionTypeStr == "ZIPFIAN_PARTITIONED_DISTRIBUTION")
  {
    config.DISTRIBUTION_TYPE = ZIPFIAN_PARTITIONED_DISTRIBUTION;
  }
  else if (distributionTypeStr == "SINGLE_NODE_HOT_KEYS")
  {
    config.DISTRIBUTION_TYPE = SINGLE_NODE_HOT_KEYS;
  }
  else if (distributionTypeStr == "SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_ONLY")
  {
    config.DISTRIBUTION_TYPE = SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_ONLY;
  } else if (distributionTypeStr == "SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_RDMA_ONLY") {
    config.DISTRIBUTION_TYPE = SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_RDMA_ONLY;
  } else if (distributionTypeStr == "SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_SPLIT") {
    config.DISTRIBUTION_TYPE = SINGLE_NODE_HOT_KEYS_TO_SECOND_NODE_SPLIT;
  } else if (distributionTypeStr == "YCSB") {
    config.DISTRIBUTION_TYPE = YCSB;
  } else {
    std::cerr << "Invalid distribution type selected." << std::endl;
    exit(0);
  }

  config.HOT_KEY_PERCENTAGE = jsonData["HOT_KEY_PERCENTAGE"];
  config.HOT_KEY_ACCESS_PERCENTAGE = jsonData["HOT_KEY_ACCESS_PERCENTAGE"];
  config.TOTAL_RUNTIME_IN_SECONDS = jsonData["TOTAL_RUNTIME_IN_SECONDS"];
  config.RDMA_ASYNC = jsonData["RDMA_ASYNC"];
  config.DISK_ASYNC = jsonData["DISK_ASYNC"];
  config.infinity_bound_nic = jsonData["infinity_bound_nic"];
  config.infinity_bound_device_port = jsonData["infinity_bound_device_port"];
  config.operations_pollute_cache = jsonData["operations_pollute_cache"];
  config.use_cache_logs = jsonData["use_cache_logs"];
  config.cache_log_sync_every_x_operations = jsonData["cache_log_sync_every_x_operations"];
  config.dump_snapshot_period_ms = jsonData["dump_snapshot_period_ms"];
  config.dump_snapshot_file = jsonData["dump_snapshot_file"];
  config.enable_fallback_rpc = jsonData["enable_fallback_rpc"];
  config.fallback_rpc_broadcast = jsonData["fallback_rpc_broadcast"];
  config.cache_logs_push_entries = jsonData["cache_logs_push_entries"];
  config.write_policy = jsonData["write_policy"];
  return config;
}