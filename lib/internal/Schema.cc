#include "IR/Types.h"
#include "internal/CloudManager.h"
#include "internal/Schema.h"
#include "nlohmann/json.hpp"
#include <vector>
#include <string>
#include <unordered_map>
#include <fstream>
#include <iostream>

namespace p2cllvm {

const ColumnDef& TableSchema::getColumn(std::string_view col_name) const {
    for(const auto& col : columns) {
        if(col.name == col_name) return col;
    }
    throw std::runtime_error("Column not found: " + std::string(col_name));
}

void SchemaManager::loadFromCloud(std::string_view json_path) {
    std::string path_str(json_path);
    std::string json_content;

    // Note: AnyBlobManager must be initialized before calling this
    try {
            json_content = AnyBlobManager::getInstance().fetchObject(path_str);
    } catch (const std::exception& e) {
        std::cerr << "Remote fetch failed (" << e.what() << "), trying local file: " << path_str << std::endl;
        std::ifstream f{path_str};
        if (!f.is_open()) {
            throw std::runtime_error("Failed to open schema file (remote & local): " + path_str);
        }
        std::stringstream buffer;
        buffer << f.rdbuf();
        json_content = buffer.str();
    }

    nlohmann::json j = nlohmann::json::parse(json_content);
    
    for (const auto& table_json : j["tables"]) {
        TableSchema schema;
        schema.name = table_json["name"];
        int idx = 0;
        for (const auto& col_json : table_json["columns"]) {
            ColumnDef def;
            def.name = col_json["name"];
            def.index = idx++;
            // Map string to TypeEnum
            std::string type_str = col_json["type"];
            if (type_str == "INTEGER") def.type = TypeEnum::Integer;
            else if (type_str == "BIGINT") def.type = TypeEnum::BigInt;
            else if (type_str == "DOUBLE") def.type = TypeEnum::Double;
            else if (type_str == "STRING") def.type = TypeEnum::String;
            else if (type_str == "DATE") def.type = TypeEnum::Date;
            else if (type_str == "CHAR") def.type = TypeEnum::Char;
            else throw std::runtime_error("Unknown type: " + type_str);
            
            schema.columns.push_back(def);
        }
        schemas[schema.name] = schema;
    }
}

const TableSchema& SchemaManager::getSchema(std::string_view table_name) {
    if (!schemas.count(std::string(table_name))) 
        throw std::runtime_error("Schema not found for table: " + std::string(table_name));
    return schemas.at(std::string(table_name));
}

std::vector<std::string> SchemaManager::getTableNames() const {
    std::vector<std::string> names;
    for (const auto& [name, schema] : schemas) {
        names.push_back(name);
    }
    return names;
}

} // namespace p2cllvm