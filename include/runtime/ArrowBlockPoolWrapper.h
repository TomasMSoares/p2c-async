#pragma once
#include <arrow/memory_pool.h>
#include "BlockPool.h"
#include <cstring> // For std::memcpy

namespace p2cllvm {

class ArrowBlockPoolWrapper : public arrow::MemoryPool {
public:
    static ArrowBlockPoolWrapper& get() {
        static ArrowBlockPoolWrapper instance(BlockPool::get());
        return instance;
    }

    // Offset used to store metadata. 64 bytes for Arrow's alignment requirements
    static constexpr size_t HEADER_OFFSET = 64;

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        try {
            auto [ptr, capacity] = pool_.acquire(static_cast<size_t>(size) + HEADER_OFFSET);
            char* raw_ptr = ptr.release();
            
            *reinterpret_cast<size_t*>(raw_ptr) = capacity;
            
            *out = reinterpret_cast<uint8_t*>(raw_ptr + HEADER_OFFSET);
            return arrow::Status::OK();
        } catch (const std::exception& e) {
            return arrow::Status::OutOfMemory(e.what());
        }
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
        if (!*ptr) return Allocate(new_size, alignment, ptr);

        char* raw_ptr = reinterpret_cast<char*>(*ptr) - HEADER_OFFSET;
        size_t current_capacity = *reinterpret_cast<size_t*>(raw_ptr);

        if (static_cast<size_t>(new_size) <= (current_capacity - HEADER_OFFSET)) {
            return arrow::Status::OK();
        }

        uint8_t* new_ptr;
        auto status = Allocate(new_size, alignment, &new_ptr);
        if (!status.ok()) return status;

        std::memcpy(new_ptr, *ptr, static_cast<size_t>(old_size));
        Free(*ptr, old_size, alignment);
        *ptr = new_ptr;
        return arrow::Status::OK();
    }

    void Free(uint8_t* ptr, int64_t size, int64_t alignment) override {
        if (!ptr) return;
        
        char* raw_ptr = reinterpret_cast<char*>(ptr) - HEADER_OFFSET;
        
        size_t capacity = *reinterpret_cast<size_t*>(raw_ptr);
        
        pool_.release(raw_ptr, capacity);
    }

    int64_t bytes_allocated() const override {
        return static_cast<int64_t>(pool_.getStats().currently_in_use);
    }

    int64_t total_bytes_allocated() const override {
        return static_cast<int64_t>(pool_.getStats().total_allocated);
    }

    int64_t num_allocations() const override {
        return static_cast<int64_t>(pool_.getStats().allocation_count);
    }

    int64_t max_memory() const override {
        return static_cast<int64_t>(pool_.getStats().total_allocated);
    }

    std::string backend_name() const override { return "BlockPoolWrapper-LockFree"; }

private:
    explicit ArrowBlockPoolWrapper(BlockPool& pool) : pool_(pool) {}

    BlockPool& pool_;
};

} // namespace p2cllvm