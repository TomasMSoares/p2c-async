// ParquetTable.cc
#include "internal/ParquetTable.h"
#include "internal/ParquetMetadata.h"
#include "internal/Schema.h"
#include "internal/basetypes.h"
#include "internal/utils.h"
#include "runtime/ArrowBlockPoolWrapper.h"
#include <arrow/io/memory.h>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <chrono>
#include <iostream>
#include <fstream>

namespace p2cllvm {

thread_local std::shared_ptr<DecompressedChunk> ParquetTable::tls_current_chunk = nullptr;


ParquetTable::ParquetTable(Config& cfg, std::string_view table_name) {
    table_base_path = cfg.dataset_path + "/" + std::string(table_name);
    is_remote = cfg.is_remote;
    
    std::string metadata_filename = std::string(table_name) + "_metadata.json";
    std::string full_meta_path = table_base_path + "/" + metadata_filename;
    

    if (is_remote) {
        metadata = ParquetFileMetadata::loadFromRemote(full_meta_path);
    } else {
        metadata = ParquetFileMetadata::loadFromPath(full_meta_path);
    }
    
    states = std::vector<std::atomic<ChunkState>>(metadata.row_groups.size());
    for(auto& s : states) s = ChunkState::UNLOADED;
    
    if (is_remote) anyblob = &AnyBlobManager::getInstance();
}


std::string ParquetTable::resolvePath(const std::string& relative_filename) const {
    if (table_base_path.empty()) return relative_filename;
    
    if (table_base_path.back() == '/' || table_base_path.back() == '\\') {
        return table_base_path + relative_filename;
    }
    return table_base_path + "/" + relative_filename;
}


std::unique_ptr<FetchedChunk> ParquetTable::fetchChunkSync(int idx) {
    if (idx < 0 || idx >= static_cast<int>(metadata.row_groups.size())) {
        return nullptr;
    }

    states[idx] = ChunkState::PREPARING;
    
    const auto& rg = metadata.row_groups[idx];
    std::string path = resolvePath(rg.object_key);
    
    auto block_and_size = BlockPool::get().acquire(rg.compressed_size);
    auto& buffer = block_and_size.first;

    if (!buffer) {
        std::cerr << "ERROR: OOM in BlockPool for chunk " << idx << std::endl;
        states[idx] = ChunkState::UNLOADED;
        return nullptr;
    }

    bool success = false;
    size_t bytes_read = 0;

    if (is_remote) {
        try {
            anyblob::network::Transaction txn(anyblob->getProvider());
            txn.getObjectRequest(path);

            auto handle = anyblob->getGroup()->getHandle();
            
            // BLOCKING CALL
            txn.processSync(handle);
            
            // Check results
            for (const auto& it : txn) {
                if (it.success()) {
                    auto result = it.getResult();
                    if (result.size() == rg.compressed_size) {
                        std::memcpy(buffer.get(), result.data(), result.size());
                        bytes_read = result.size();
                        success = true;
                    } else {
                        std::cerr << "ERROR: Size mismatch for " << path << std::endl;
                    }
                    break; // we assume one request per transaction
                } else {
                    std::cerr << "ERROR: Download failed: " << it.getFailureCode() << std::endl;
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "ERROR: Remote exception: " << e.what() << std::endl;
        }
    } else {
        std::ifstream file(path, std::ios::binary);
        if (file.is_open()) {
            if (file.read(buffer.get(), rg.compressed_size)) {
                bytes_read = rg.compressed_size;
                success = true;
            } else {
                std::cerr << "ERROR: Failed to read file: " << path << std::endl;
            }
        } else {
             std::cerr << "ERROR: File not found: " << path << std::endl;
        }
    }

    // Prepare downloaded chunk
    if (success) {
        auto chunk = std::make_unique<FetchedChunk>();
        chunk->row_group_id = idx;
        chunk->raw_data = std::move(buffer);
        chunk->size = bytes_read;
        
        states[idx] = ChunkState::DOWNLOADED;
        return chunk;
    } else {
        states[idx] = ChunkState::UNLOADED;
        return nullptr;
    }
}


bool ParquetTable::scheduleFetchAsync(int idx, anyblob::network::Transaction& txn, DownloadCallback cb) {
    if (!is_remote) throw std::runtime_error("Async not supported for local");
    
    ChunkState expected = ChunkState::UNLOADED;
    if (!states[idx].compare_exchange_strong(expected, ChunkState::PREPARING)) return false;

    auto& rg = metadata.row_groups[idx];
    auto block_and_size = BlockPool::get().acquire(rg.compressed_size);
    auto& buffer = block_and_size.first;

    auto callback = [this, idx, buf = std::move(buffer), size = rg.compressed_size, user_cb = cb]
                    (anyblob::network::MessageResult& res) mutable {
        if (res.success()) {
            std::memcpy(buf.get(), res.getResult().data(), res.getResult().size());
            auto chunk = std::make_unique<FetchedChunk>();
            chunk->row_group_id = idx;
            chunk->raw_data = std::move(buf);
            chunk->size = res.getResult().size();
            
            states[idx] = ChunkState::DOWNLOADED;
            
            if (user_cb) user_cb(std::move(chunk));
        } else {
            states[idx] = ChunkState::UNLOADED;
        }
    };

    std::string object_key = resolvePath(rg.object_key);
    
    txn.getObjectRequest(std::move(callback), object_key); // Prepare HTTP message

    auto group = anyblob->getGroup();
    txn.processAsync(*group); // submit newly added request to the kernel
    
    states[idx] = ChunkState::DOWNLOADING;
    return true;
}


std::shared_ptr<DecompressedChunk> ParquetTable::decompressChunk(std::unique_ptr<FetchedChunk> input) {
    states[input->row_group_id] = ChunkState::DECOMPRESSING;
    
    auto buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(input->raw_data.get()), input->size);
    auto reader = std::make_shared<arrow::io::BufferReader>(buffer);

    auto reader_result = parquet::arrow::OpenFile(reader, &ArrowBlockPoolWrapper::get());
    if (!reader_result.ok()) {
        states[input->row_group_id] = ChunkState::UNLOADED;
        throw std::runtime_error("Failed to open Parquet from buffer: " + 
                                reader_result.status().ToString());
    }
    
    auto arrow_reader = std::move(*reader_result);
    std::shared_ptr<arrow::Table> table;

    // Read whole table (which is just one chunk)
    auto status = arrow_reader->ReadTable(&table);
    if (!status.ok()) throw std::runtime_error("ReadTable failed: " + status.ToString());
    
    // Build Output
    auto out = std::make_shared<DecompressedChunk>();
    out->row_group_id = input->row_group_id;
    out->arrow_holder = table;
    out->row_count = table->num_rows();

    for(int i = 0; i < table->num_columns(); ++i) {
        auto col_name = table->schema()->field(i)->name();
        
        // Skip columns not needed by query
        if (!registered_columns.empty() && 
            std::find(registered_columns.begin(), registered_columns.end(), col_name) == registered_columns.end()) {
            continue;
        }
        
        auto col_data = table->column(i);
        if (col_data->num_chunks() != 1) {
            throw std::runtime_error("Expected single chunk per column in row group");
        }
        
        auto arrow_array = col_data->chunk(0);
        out->column_ptrs[col_name] = convertColumn(arrow_array, out);
    }
    
    states[input->row_group_id] = ChunkState::DONE;
    return out;
}


void ParquetTable::setThreadLocalChunk(std::shared_ptr<DecompressedChunk> chunk) {
    tls_current_chunk = chunk;
}


void* ParquetTable::convertColumn(const std::shared_ptr<arrow::Array>& arr,
                                  std::shared_ptr<DecompressedChunk> chunk) {
    switch (arr->type_id()) {
        case arrow::Type::INT32: {
            auto typed = std::static_pointer_cast<arrow::Int32Array>(arr);
            return const_cast<int32_t*>(typed->raw_values());
        }
        case arrow::Type::INT64: {
            auto typed = std::static_pointer_cast<arrow::Int64Array>(arr);
            return const_cast<int64_t*>(typed->raw_values());
        }
        case arrow::Type::DOUBLE: {
            auto typed = std::static_pointer_cast<arrow::DoubleArray>(arr);
            return const_cast<double*>(typed->raw_values());
        }
        case arrow::Type::FLOAT: {
            auto typed = std::static_pointer_cast<arrow::FloatArray>(arr);
            return const_cast<float*>(typed->raw_values());
        }
        case arrow::Type::UINT8: {
            auto typed = std::static_pointer_cast<arrow::UInt8Array>(arr);
            return const_cast<uint8_t*>(typed->raw_values());
        }
        case arrow::Type::DATE32: {
            auto typed = std::static_pointer_cast<arrow::Date32Array>(arr);
            return const_cast<int32_t*>(typed->raw_values());
        }
        case arrow::Type::STRING:
        case arrow::Type::BINARY: {
            // Strings need conversion to slotted page format
            auto string_array = std::static_pointer_cast<arrow::StringArray>(arr);
            int64_t num_values = string_array->length();
            
            // Calculate size needed
            size_t total_string_bytes = 0;
            for (int64_t i = 0; i < num_values; i++) {
                if (!string_array->IsNull(i)) {
                    total_string_bytes += string_array->value_length(i);
                }
            }
            
            size_t total_size = sizeof(String) + num_values * sizeof(String::StringData) + total_string_bytes;
            auto block_and_size = BlockPool::get().acquire(total_size);
            auto& buffer = block_and_size.first;
            char* raw = buffer.get();
            
            String* str_struct = reinterpret_cast<String*>(raw);
            str_struct->count = num_values;
            
            // Build slotted page
            size_t data_offset = sizeof(String) + num_values * sizeof(String::StringData);
            for (int64_t i = 0; i < num_values; i++) {
                if (string_array->IsNull(i)) {
                    str_struct->slot[i].length = 0;
                    str_struct->slot[i].offset = data_offset;
                } else {
                    int32_t length = string_array->value_length(i);
                    const uint8_t* data = string_array->GetValue(i, &length);
                    
                    str_struct->slot[i].length = length;
                    str_struct->slot[i].offset = data_offset;
                    
                    std::memcpy(raw + data_offset, data, length);
                    data_offset += length;
                }
            }
            
            chunk->string_buffers.push_back(std::move(buffer));
            return raw;
        }
        default:
            throw std::runtime_error("Unsupported Arrow type: " + arr->type()->ToString());
    }
}


void* ParquetTable::getColumnData(std::string_view col_name) {
    if (!tls_current_chunk) {
        throw std::runtime_error("Attempted to access column data without a loaded chunk in thread-local storage.");
    }
    try {
        return tls_current_chunk->column_ptrs.at(std::string(col_name));
    } catch (const std::out_of_range& e) {
        throw std::runtime_error("Column '" + std::string(col_name) + "' not found in the current chunk context.");
    }
}

}