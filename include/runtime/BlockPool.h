#pragma once
#include <vector>
#include <mutex>
#include <memory>
#include <unordered_map>
#include <functional>

namespace p2cllvm {

// Forward declaration for friendship
class ArrowBlockPoolWrapper;

class BlockPool {
public:
    struct Block {
        char* ptr;
        size_t capacity;
        Block(char* p, size_t cap) : ptr(p), capacity(cap) {}
    };

    struct Stats {
        size_t total_allocated = 0;
        size_t currently_in_use = 0;
        size_t blocks_in_pool = 0;
        size_t allocation_count = 0;
        size_t reuse_count = 0;
        size_t eviction_count = 0;
    };

    static BlockPool& get();

    explicit BlockPool(size_t max_size = 6UL * 1024 * 1024 * 1024);
    ~BlockPool();

    // Disable copying
    BlockPool(const BlockPool&) = delete;
    BlockPool& operator=(const BlockPool&) = delete;

    std::pair<std::unique_ptr<char, std::function<void(char*)>>, size_t> acquire(size_t size);

    bool canAllocate(size_t size) const;

    size_t availableMemory() const;

    Stats getStats() const;

    void setMaxPoolSize(size_t new_max);
    
    void trim();

private:
    friend class ArrowBlockPoolWrapper;

    static size_t getSizeClass(size_t requested);
    
    Block* findBlock(size_t size);
    
    void evictBlocks(size_t needed);
    
    Block* allocateNewBlock(size_t size);
    
    void release(char* p, size_t capacity);

    static constexpr size_t SIZE_BOUNDARY = 4UL * 1024 * 1024;
    static constexpr size_t SMALL_BLOCK_ALIGNMENT = 512;
    static constexpr size_t ALIGNMENT = 64;
    
    std::unordered_map<size_t, std::vector<Block>> size_classes;
    mutable std::mutex mtx;
    Stats stats;
    size_t max_pool_size;
};

} // namespace p2cllvm