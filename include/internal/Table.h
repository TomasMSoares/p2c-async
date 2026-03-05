#pragma once
#include <cstdint>
#include <string_view>
#include <string>
#include <vector>

namespace p2cllvm {

/**
 * Abstract table interface - manages all columns as a cohesive unit.
 * Implementation: ParquetTable (chunk-based remote storage).
 */
class ITable {
public:
    virtual ~ITable() = default;
    
    // Get column data pointer by name
    virtual void* getColumnData(std::string_view col_name) = 0;
    
    // Get number of rows in current chunk
    virtual uint64_t getRowCount() const = 0;

    // Get the name of the table
    virtual std::string getTableName() const = 0;
    
    // Get total number of chunks (row groups for ParquetTable)
    virtual size_t getTotalChunks() const = 0;
    
    // Check if there are more chunks to process
    virtual bool hasNextChunk() const = 0;
    
    // Reset to first chunk
    virtual void resetChunks() = 0;
    
    // Prepare column pointer array for LLVM execution
    // Returns vector of column pointers in schema index order
    virtual std::vector<void*> prepareColumnArray(const std::vector<std::string>& column_layout) = 0;
};

} // namespace p2cllvm
