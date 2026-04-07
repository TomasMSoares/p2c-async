#pragma once
#include <vector>
#include <string>
#include <unordered_map>

namespace p2cllvm {

// forward declaration
enum class TypeEnum : uint8_t;

struct ColumnDef {
    std::string name;
    TypeEnum type;
    int index; // idx in generic ptr array
};

struct TableSchema {
    std::string name;
    std::vector<ColumnDef> columns;
    
    // fast lookups by name
    const ColumnDef& getColumn(std::string_view col_name) const;
};

class SchemaManager {
public:
    static SchemaManager& get() { static SchemaManager i; return i; }

    void loadFromCloud(std::string_view json_path);

    const TableSchema& getSchema(std::string_view table_name);

    std::vector<std::string> getTableNames() const;

private:
    std::unordered_map<std::string, TableSchema> schemas;
};

}