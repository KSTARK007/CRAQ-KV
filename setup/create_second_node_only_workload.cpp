#include <iostream>
#include <fstream>
#include <thread>
#include <vector>
#include <random>
#include <mutex>
#include <future>
#include <chrono>

const int M = 10000000;
const int MIN_NUM = M / 3;
const int MAX_NUM = 2 * (M / 3);
const long long TOTAL_OPERATIONS = 1000000000;
const int NUM_FILES_WITH_2 = 42; // Example value, adjust as needed
const int NUM_FILES_WITH_1_OR_3 = 6; // Example value, adjust as needed
const int BUFFER_SIZE = 8192;
const int NUM_CLIENTS = 3; // Example value, adjust as needed
const int THREADS_PER_CLIENT = 8; // Example value, adjust as needed
const int CLIENTS_PER_THREAD = 2; // Example value, adjust as needed

std::mutex mtx;

void generateFile(int clientIndex, int threadIndex, int clientPerThreadIndex, int constantNum, long long operationsPerFile) {
    std::string fileName = "client_" + std::to_string(clientIndex) +
                           "_thread_" + std::to_string(threadIndex) +
                           "_clientPerThread_" + std::to_string(clientPerThreadIndex) + ".txt";
    std::ofstream outFile(fileName);
    if (!outFile) {
        std::cerr << "Error creating file: " << fileName << std::endl;
        return;
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(MIN_NUM, MAX_NUM);

    std::vector<char> buffer;
    buffer.reserve(BUFFER_SIZE);

    for (long long i = 0; i < operationsPerFile; ++i) {
        int randomNum = dis(gen);
        std::string entry = std::to_string(randomNum) + " " + std::to_string(constantNum) + "\n";
        buffer.insert(buffer.end(), entry.begin(), entry.end());

        if (buffer.size() >= BUFFER_SIZE) {
            outFile.write(buffer.data(), buffer.size());
            buffer.clear();
        }
    }

    if (!buffer.empty()) {
        outFile.write(buffer.data(), buffer.size());
    }
    std::cout << "Generated " << fileName << std::endl;

    outFile.close();
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();

    int N = NUM_FILES_WITH_2 + NUM_FILES_WITH_1_OR_3;
    long long operationsPerFile = TOTAL_OPERATIONS / N;
    std::vector<std::future<void>> futures;

    int fileIndex = 0;

    for (int clientIndex = 0; clientIndex < NUM_CLIENTS; ++clientIndex) {
        for (int threadIndex = 0; threadIndex < THREADS_PER_CLIENT; ++threadIndex) {
            for (int clientPerThreadIndex = 0; clientPerThreadIndex < CLIENTS_PER_THREAD; ++clientPerThreadIndex) {
                if (fileIndex < NUM_FILES_WITH_2) {
                    futures.emplace_back(std::async(std::launch::async, generateFile,
                        clientIndex, threadIndex, clientPerThreadIndex, 2, operationsPerFile));
                } else {
                    int constantNum = (fileIndex % 2 == 0) ? 1 : 3;
                    futures.emplace_back(std::async(std::launch::async, generateFile,
                        clientIndex, threadIndex, clientPerThreadIndex, constantNum, operationsPerFile));
                }
                fileIndex++;
            }
        }
    }

    for (auto& fut : futures) {
        fut.get();
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;

    std::cout << "File generation completed in " << duration.count() << " seconds." << std::endl;

    return 0;
}
