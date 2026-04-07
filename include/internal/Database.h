#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <memory>

namespace p2cllvm {

// forward declarations
class Config;
class ParquetTable;
class ITable;

class Database {
public:
  std::vector<std::unique_ptr<ParquetTable>> tables;
  std::unordered_map<std::string, size_t> name_to_index;
  Config& config;
  
  Database(Config& cfg);
  
  ITable* getTable(size_t idx);

  // Get table by name (used by Query logic)
  ITable* getTable(std::string_view name);

  // Get index by name (used by Scan operator)
  size_t getTableIndex(std::string_view table_name) const;

  // Estimate Scale Factor dynamically based on 'lineitem' if it exists
  size_t get_total_tuple_count(const std::string_view table_name);
};

} // namespace p2cllvm