#include "runtime/ArrowBlockPoolWrapper.h"
#include "runtime/BlockPool.h"
#include <cstring> // For std::memcpy
#include <exception>

namespace p2cllvm {

ArrowBlockPoolWrapper& ArrowBlockPoolWrapper::get() {
    static ArrowBlockPoolWrapper instance(BlockPool::get());
    return instance;
}

ArrowBlockPoolWrapper::ArrowBlockPoolWrapper(BlockPool& pool) : pool_(pool) {}

arrow::Status ArrowBlockPoolWrapper::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
    try {
        // We add HEADER_OFFSET to store the capacity of the block for later retrieval
        auto [ptr, capacity] = pool_.acquire(static_cast<size_t>(size) + HEADER_OFFSET);
        char* raw_ptr = ptr.release();
        
        // Store the capacity at the very beginning of the allocated block
        *reinterpret_cast<size_t*>(raw_ptr) = capacity;
        
        // Return the pointer offset by HEADER_OFFSET so Arrow doesn't overwrite our metadata
        *out = reinterpret_cast<uint8_t*>(raw_ptr + HEADER_OFFSET);
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::OutOfMemory(e.what());
    }
}

arrow::Status ArrowBlockPoolWrapper::Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) {
    if (!*ptr) return Allocate(new_size, alignment, ptr);

    char* raw_ptr = reinterpret_cast<char*>(*ptr) - HEADER_OFFSET;
    size_t current_capacity = *reinterpret_cast<size_t*>(raw_ptr);

    // If the existing block is already large enough, just keep using it
    if (static_cast<size_t>(new_size) <= (current_capacity - HEADER_OFFSET)) {
        return arrow::Status::OK();
    }

    // Otherwise, allocate a new one and copy
    uint8_t* new_ptr;
    auto status = Allocate(new_size, alignment, &new_ptr);
    if (!status.ok()) return status;

    std::memcpy(new_ptr, *ptr, static_cast<size_t>(old_size));
    Free(*ptr, old_size, alignment);
    *ptr = new_ptr;
    return arrow::Status::OK();
}

void ArrowBlockPoolWrapper::Free(uint8_t* ptr, int64_t size, int64_t alignment) {
    if (!ptr) return;
    
    char* raw_ptr = reinterpret_cast<char*>(ptr) - HEADER_OFFSET;
    size_t capacity = *reinterpret_cast<size_t*>(raw_ptr);
    
    // Return the block to the original BlockPool
    pool_.release(raw_ptr, capacity);
}

int64_t ArrowBlockPoolWrapper::bytes_allocated() const {
    return static_cast<int64_t>(pool_.getStats().currently_in_use);
}

int64_t ArrowBlockPoolWrapper::total_bytes_allocated() const {
    return static_cast<int64_t>(pool_.getStats().total_allocated);
}

int64_t ArrowBlockPoolWrapper::num_allocations() const {
    return static_cast<int64_t>(pool_.getStats().allocation_count);
}

int64_t ArrowBlockPoolWrapper::max_memory() const {
    return static_cast<int64_t>(pool_.getStats().total_allocated);
}

std::string ArrowBlockPoolWrapper::backend_name() const { 
    return "BlockPoolWrapper-LockFree"; 
}

} // namespace p2cllvm