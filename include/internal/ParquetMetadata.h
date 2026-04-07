// ParquetMetadata.h
#pragma once

#include "nlohmann/json.hpp"
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <memory>
#include "internal/CloudManager.h"

namespace p2cllvm {

using json = nlohmann::json;

// Represents a standalone chunk file (one row group)
struct RowGroupInfo {
    int id;
    std::string object_key;
    uint64_t compressed_size;
    uint64_t uncompressed_size;
    uint64_t num_rows;
};

struct ParquetFileMetadata {
    std::string table_name;
    std::vector<RowGroupInfo> row_groups;

    static ParquetFileMetadata loadFromPath(const std::string& json_path) {
        std::ifstream f(json_path);
        if (!f.is_open()) throw std::runtime_error("Could not open metadata file: " + json_path);
        std::stringstream buffer;
        buffer << f.rdbuf();
        return parseFromString(buffer.str());
    }

    static ParquetFileMetadata loadFromRemote(const std::string& object_key) {
        std::string json_str = AnyBlobManager::getInstance().fetchObject(object_key);
        return parseFromString(json_str);
    }

private:
    static ParquetFileMetadata parseFromString(const std::string& json_str) {
        try {
            json j = json::parse(json_str);
            ParquetFileMetadata md;
            
            if (j.contains("table_name")) md.table_name = j["table_name"];
            else if(j.contains("object_key")) md.table_name = j["object_key"];

            for (const auto& rg : j.at("row_groups")) {
                md.row_groups.push_back({
                    rg.at("id").get<int>(),
                    rg.at("object_key").get<std::string>(),
                    rg.at("compressed_size").get<uint64_t>(),
                    rg.at("uncompressed_size").get<uint64_t>(),
                    rg.at("num_rows").get<uint64_t>()
                });
            }
            return md;
        }
        catch (const json::exception& e) {
            throw std::runtime_error("JSON parsing failed: " + std::string(e.what()));
        }
    }
};

} // namespace p2cllvm