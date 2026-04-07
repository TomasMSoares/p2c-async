#pragma once
#include <arrow/memory_pool.h>
#include <string>

namespace p2cllvm {

class BlockPool; // Forward declaration

class ArrowBlockPoolWrapper : public arrow::MemoryPool {
public:
    static ArrowBlockPoolWrapper& get();

    // Offset used to store metadata. 64 bytes for Arrow's alignment requirements
    static constexpr size_t HEADER_OFFSET = 64;

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;
    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override;
    void Free(uint8_t* ptr, int64_t size, int64_t alignment) override;

    int64_t bytes_allocated() const override;
    int64_t total_bytes_allocated() const override;
    int64_t num_allocations() const override;
    int64_t max_memory() const override;
    
    std::string backend_name() const override;

private:
    explicit ArrowBlockPoolWrapper(BlockPool& pool);
    BlockPool& pool_;
};

} // namespace p2cllvm