#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <unistd.h>

#include "cloud/provider.hpp"
#include "network/tasked_send_receiver.hpp"
#include "network/transaction.hpp"
#include "network/message_result.hpp"
#include "utils/data_vector.hpp"

#include "nlohmann/json.hpp"

using namespace anyblob;

using json = nlohmann::json;

const std::string MINIO_CONFIG = "/home/tomas/TUM/Thesis/project/scheduling-opt/minio-utils/minio_config.json";
const uint8_t PROCESSING_PRINT_COUNT = 5;

const uint32_t CHUNK_SIZE = 1024 * 1024; // 1MB chunk size
const uint64_t MAX_CONCURRENT_MSGS = 2; // Low value since we are testing sync

// simulate simple processing over the data
void mock_process_data(const uint8_t* data, uint64_t size) {
    std::cout << "  > Starting local processing..." << std::endl;
    uint64_t printInfoMod = size / PROCESSING_PRINT_COUNT;
    uint64_t sum = 0;
    for (uint64_t i = 0; i < size; ++i) {
        sum += data[i];
        if (i && i % printInfoMod == 0) {
            std::cout << "  > Processed " << i << " bytes..." << std::endl;
        }
    }
    std::cout << "  > Finished local processing. Processed " << size << " bytes. (Mock sum: " << sum << ")" << std::endl;
}


void print_usage() {
    std::cerr << "Usage: ./naive_process -f <minio_object_path>\n"
              << "  -f <path>:  Required. The path to the file in MinIO (e.g., 'my-bucket/data.bin')\n"
              << "  -h:         Show this help message\n";
}


int main(int argc, char* argv[]) {

    std::string filePath;
    int opt;

    while ((opt = getopt(argc, argv, "f:h")) != -1) {
        switch (opt) {
            case 'f':
                filePath = optarg;
                break;
            case 'h':
                print_usage();
                return 0;
            default: /* '?' */
                print_usage();
                return 1;
        }
    }

    if (filePath.empty()) {
        std::cerr << "Error: Missing required argument -f <minio_object_path>\n" << std::endl;
        print_usage();
        return 1;
    }

    std::ifstream cfg(MINIO_CONFIG);
    if (!cfg.is_open()) {
        std::cerr << "Could not open config file: " << MINIO_CONFIG << std::endl;
        return 1;
    }

    json config;
    try {
        config = json::parse(cfg);
    }
    catch (json::parse_error& e) {
        std::cerr << "Error: failed to parse JSON: " << e.what() << std::endl;
        return 1;
    }

    const std::string minio_uri { config["endpoint"] };
    const std::string username { config["access_key"] };
    const std::string password { config["secret_key"] };

    // group manages the low-level io_uring/poll resources and buffer
    network::TaskedSendReceiverGroup group(CHUNK_SIZE, MAX_CONCURRENT_MSGS << 1); 
    network::TaskedSendReceiverHandle handle = group.getHandle();

    // instantiate provider
    std::unique_ptr<cloud::Provider> cloudProvider;
    try {
        cloudProvider = cloud::Provider::makeProvider(minio_uri, false, username, password, &handle); 
    }
    catch (const std::exception& e) {
        std::cerr << "Error creating provider: " << e.what() << std::endl;
        return 1;
    }

    network::Transaction downloadTxn(cloudProvider.get());
        
    // getObjectRequest returns a function that, when called, creates and returns the OriginalMessage.
    auto requestCreator = [&downloadTxn, &filePath]() {
        return downloadTxn.getObjectRequest(filePath); 
    };

    downloadTxn.verifyKeyRequest(handle, std::move(requestCreator));

    std::cout << "Starting synchronous download via processSync()..." << std::endl;
    try {
        downloadTxn.processSync(handle); 
    }
    catch (const std::exception& e) {
        std::cerr << "Exception during synchronous processing: " << e.what() << std::endl;
    }
    std::cout << "processSync() finished. Thread is unblocked." << std::endl;

    for (const auto& it: downloadTxn) {
        if (!it.success()) {
            std::cerr << "Request was not successful! Error: " << it.getErrorResponse() << std::endl;
            continue;
        }
        
        // offset contains the size of the HTTP header
        const uint8_t* rawData = it.getData() + it.getOffset();
        uint64_t dataSize = it.getSize();

        std::cout << "Successfully downloaded " << dataSize << " bytes." << std::endl;
        mock_process_data(rawData, dataSize);
    }

    std::cout << "Success." << std::endl;

    return 0;
}