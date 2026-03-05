#pragma once
#include "Table.h"
#include "CloudManager.h"
#include "ParquetMetadata.h"
#include "Config.h"
#include "runtime/BlockPool.h"
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <queue>
#include <atomic>
#include <functional>
#include <string_view>
#include <string>

namespace p2cllvm {

// Represents the state of a RowGroup in the pipeline
enum class ChunkState { UNLOADED, PREPARING, DOWNLOADING, DOWNLOADED, DECOMPRESSING, DONE };

struct FetchedChunk {
    int row_group_id;
    // std::unique_ptr with custom deleter (BlockPool::release)
    std::unique_ptr<char, std::function<void(char*)>> raw_data; 
    size_t size;
};

struct DecompressedChunk {
    int row_group_id;
    uint64_t row_count;
    // Maps column name to raw data pointer (for the LLVM runtime)
    std::unordered_map<std::string, void*> column_ptrs; 
    std::shared_ptr<arrow::Table> arrow_holder;
    std::vector<std::unique_ptr<char, std::function<void(char*)>>> string_buffers; 
};

class ParquetTable : public ITable {
public:
    using DownloadCallback = std::function<void(std::unique_ptr<FetchedChunk>)>;

    ParquetTable(Config& cfg, std::string_view table_directory_path);

    // Synchronous fetch (local or blocking remote)
    std::unique_ptr<FetchedChunk> fetchChunkSync(int row_group_idx);

    // Asynchronous fetch (requires callback for notification)
    bool scheduleFetchAsync(int row_group_idx, anyblob::network::Transaction& txn, DownloadCallback cb = nullptr);

    // Parses Parquet and converts columns
    std::shared_ptr<DecompressedChunk> decompressChunk(std::unique_ptr<FetchedChunk> chunk);

    const ParquetFileMetadata& getMetadata() const { return metadata; }
    ChunkState getChunkState(int idx) const { return states[idx].load(); }
    
    // Column registration (called during query planning)
    void registerColumn(std::string_view col_name) {
        registered_columns.push_back(std::string(col_name));
    }
    
    size_t getTotalChunks() const { return metadata.row_groups.size(); }
    size_t getCompressedChunkSize(const size_t row_grp_idx) const { return metadata.row_groups[row_grp_idx].compressed_size; }
    size_t getUncompressedChunkSize(const size_t row_grp_idx) const { return metadata.row_groups[row_grp_idx].uncompressed_size; }
    
    int getCurrentChunkIndex() const { return current_chunk_idx; }
    bool hasNextChunk() const { return current_chunk_idx + 1 < static_cast<int>(metadata.row_groups.size()); }
    
    bool loadNextChunk(std::shared_ptr<DecompressedChunk> chunk) {
        setThreadLocalChunk(chunk);
        current_chunk_idx = chunk->row_group_id;
        return true;
    }
    
    void resetChunks() override {
        current_chunk_idx = -1;
        for(auto& state : states) {
            state.store(ChunkState::UNLOADED);
        }
    }
    
    // ITable Interface (Used by LLVM Pipeline)
    void* getColumnData(std::string_view col_name) override;

    uint64_t getRowCount() const override {
        if (!tls_current_chunk) return 0;
        return tls_current_chunk->row_count;
    }

    std::string getTableName() const override {
        return metadata.table_name;
    }
    
    // Prepare column pointer array for LLVM execution
    std::vector<void*> prepareColumnArray(const std::vector<std::string>& column_layout) override {
        std::vector<void*> result(column_layout.size(), nullptr);
        for (size_t i = 0; i < column_layout.size(); ++i) {
            if (!column_layout[i].empty()) {
                result[i] = getColumnData(column_layout[i]);
            }
        }
        return result;
    }
    
    void setThreadLocalChunk(std::shared_ptr<DecompressedChunk> chunk);

private:
    ParquetFileMetadata metadata;
    AnyBlobManager* anyblob;
    std::vector<std::atomic<ChunkState>> states;
    std::vector<std::string> registered_columns;  // columns needed by query
    int current_chunk_idx = -1;  // track current chunk for iteration

    bool is_remote;
    std::string table_base_path;
    
    static thread_local std::shared_ptr<DecompressedChunk> tls_current_chunk;
    
    void* convertColumn(const std::shared_ptr<arrow::Array>& arr, 
                       std::shared_ptr<DecompressedChunk> chunk);

    std::string resolvePath(const std::string& relative_filename) const;
};
}