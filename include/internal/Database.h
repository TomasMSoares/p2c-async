#pragma once

#include "internal/ParquetTable.h"
#include "internal/Config.h"
#include "internal/Schema.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <stdexcept>
#include <algorithm>
#include <cmath>
#include <iostream>

namespace p2cllvm { 

class Database {
public:
  std::vector<std::unique_ptr<ParquetTable>> tables;
  std::unordered_map<std::string, size_t> name_to_index;
  Config config;
  
  Database(Config cfg) : config(std::move(cfg)) {
    // Note: SchemaManager::loadFromCloud() must be called before Database init
    auto table_names = SchemaManager::get().getTableNames();
    
    if (table_names.empty()) {
        std::cerr << "Warning: No tables found in SchemaManager. Database will be empty.\n";
    }

    // Sort names to ensure deterministic index order (0, 1, 2...) across runs
    std::sort(table_names.begin(), table_names.end());

    tables.reserve(table_names.size());
    for (size_t i = 0; i < table_names.size(); ++i) {
        const auto& name = table_names[i];
        tables.push_back(std::make_unique<ParquetTable>(config, name));
        name_to_index[name] = i;
    }
  }
  
  ITable* getTable(size_t idx) {
    if (idx >= tables.size()) {
      throw std::runtime_error("Invalid table index: " + std::to_string(idx));
    }
    return tables[idx].get();
  }

  // Get table by name (used by Query logic)
  ITable* getTable(std::string_view name) {
      auto it = name_to_index.find(std::string(name));
      if (it == name_to_index.end()) return nullptr;
      return tables[it->second].get();
  }

  // Get index by name (used by Scan operator)
  size_t getTableIndex(std::string_view table_name) const {
    auto it = name_to_index.find(std::string(table_name));
    if (it == name_to_index.end()) {
      throw std::runtime_error("Unknown table name: " + std::string(table_name));
    }
    return it->second;
  }

  // Estimate Scale Factor dynamically based on 'lineitem' if it exists
  size_t get_total_tuple_count(const std::string_view table_name) {
    const auto idx = getTableIndex(table_name);
    const auto& metadata = tables[idx]->getMetadata();
    uint64_t total_rows = 0;
    for (const auto& rg : metadata.row_groups) {
      total_rows += rg.num_rows;
    }
    return total_rows;
  }
};

} // namespace p2cllvm