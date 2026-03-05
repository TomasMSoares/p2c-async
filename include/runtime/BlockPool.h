#pragma once
#include <vector>
#include <mutex>
#include <memory>
#include <cstdlib>
#include <unordered_map>
#include <algorithm>
#include <stdexcept>
#include <cstring>
#include <functional>

// jemalloc support (falls back to std::aligned_alloc if not available)
#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#define POOL_ALLOC(size, align) mallocx(size, MALLOCX_ALIGN(align))
#define POOL_FREE(ptr) dallocx(ptr, 0)
#else
#define POOL_ALLOC(size, align) std::aligned_alloc(align, size)
#define POOL_FREE(ptr) std::free(ptr)
#endif

namespace p2cllvm {

// Forward declaration for friendship
class ArrowBlockPoolWrapper;

class BlockPool {
public:
    struct Block {
        char* ptr;
        size_t capacity;  // Actual allocated size
        Block(char* p, size_t cap) : ptr(p), capacity(cap) {}
    };

    struct Stats {
        size_t total_allocated = 0;   // Total bytes allocated from system
        size_t currently_in_use = 0;  // Bytes currently checked out
        size_t blocks_in_pool = 0;    // Number of blocks available
        size_t allocation_count = 0;  // Total allocations
        size_t reuse_count = 0;       // Successful reuses from pool
        size_t eviction_count = 0;    // Blocks freed due to memory pressure
    };

    static BlockPool& get() { 
        static BlockPool instance; 
        return instance; 
    }

    explicit BlockPool(size_t max_size = 6UL * 1024 * 1024 * 1024) 
        : max_pool_size(max_size) {}
    
    ~BlockPool() {
        std::lock_guard<std::mutex> lock(mtx);
        for (auto& [_, blocks] : size_classes) {
            for (auto& blk : blocks) {
                POOL_FREE(blk.ptr);
            }
        }
    }

    BlockPool(const BlockPool&) = delete;
    BlockPool& operator=(const BlockPool&) = delete;

    /**
     * Acquires a block of at least 'size' bytes. 
     * Returns a unique_ptr with a custom deleter that returns the block to the pool.
     */
    std::pair<std::unique_ptr<char, std::function<void(char*)>>, size_t> acquire(size_t size) {
        std::lock_guard<std::mutex> lock(mtx);
        
        Block* blk = findBlock(size);
        if (!blk) {
            blk = allocateNewBlock(size);
        }
        
        stats.currently_in_use += blk->capacity;
        size_t capacity = blk->capacity;
        char* ptr = blk->ptr;
        delete blk; 
    
        std::unique_ptr<char, std::function<void(char*)>> result_ptr = 
        {ptr, [capacity](char* p) { 
            BlockPool::get().release(p, capacity); 
        }};

        return {std::move(result_ptr), capacity};
    }

    bool canAllocate(size_t size) const {
        std::lock_guard<std::mutex> lock(mtx);
        size_t alloc_size = getSizeClass(size);
        return stats.total_allocated + alloc_size <= max_pool_size;
    }
    
    size_t availableMemory() const {
        std::lock_guard<std::mutex> lock(mtx);
        return max_pool_size - stats.currently_in_use;
    }
    
    Stats getStats() const {
        std::lock_guard<std::mutex> lock(mtx);
        Stats s = stats;
        s.blocks_in_pool = 0;
        for (const auto& [_, blocks] : size_classes) {
            s.blocks_in_pool += blocks.size();
        }
        return s;
    }

    void setMaxPoolSize(size_t new_max) {
        std::lock_guard<std::mutex> lock(mtx);
        max_pool_size = new_max;
        if (stats.total_allocated > max_pool_size) {
            evictBlocks(stats.total_allocated - max_pool_size);
        }
    }
    
    void trim() {
        std::lock_guard<std::mutex> lock(mtx);
        for (auto& [_, blocks] : size_classes) {
            for (auto& blk : blocks) {
                POOL_FREE(blk.ptr);
                stats.total_allocated -= blk.capacity;
            }
            blocks.clear();
        }
        stats.blocks_in_pool = 0;
    }

private:
    // Allow the Arrow wrapper to call release() directly
    friend class ArrowBlockPoolWrapper;

    static size_t getSizeClass(size_t requested) {
        if (requested == 0) return ALIGNMENT;
        
        // align to 1xboundary, 2xboundary, 3x, 4x, 5x, ...
        if (requested <= SIZE_BOUNDARY) {
            return (requested + (SMALL_BLOCK_ALIGNMENT - 1)) & ~ (SMALL_BLOCK_ALIGNMENT - 1);
        }

        size_t size = requested;
        size--;
        size |= size >> 1; size |= size >> 2; size |= size >> 4;
        size |= size >> 8; size |= size >> 16; size |= size >> 32;
        size++;
        return size;
    }
    
    Block* findBlock(size_t size) {
        size_t size_class = getSizeClass(size);
        
        // Exact match check
        auto it = size_classes.find(size_class);
        if (it != size_classes.end() && !it->second.empty()) {
            Block blk = it->second.back();
            it->second.pop_back();
            stats.blocks_in_pool--;
            stats.reuse_count++;
            return new Block(blk.ptr, blk.capacity);
        }
        
        // Oversize search (limited to 4x requested size)
        for (size_t cls = size_class * 2; cls <= size_class * 4; cls *= 2) {
            it = size_classes.find(cls);
            if (it != size_classes.end() && !it->second.empty()) {
                Block blk = it->second.back();
                it->second.pop_back();
                stats.blocks_in_pool--;
                stats.reuse_count++;
                return new Block(blk.ptr, blk.capacity);
            }
        }
        return nullptr;
    }

    void evictBlocks(size_t needed) {
        size_t freed = 0;
        std::vector<size_t> classes;
        for (const auto& [cls, _] : size_classes) classes.push_back(cls);
        std::sort(classes.rbegin(), classes.rend()); // Largest first
        
        for (size_t cls : classes) {
            auto& blocks = size_classes[cls];
            while (!blocks.empty() && freed < needed) {
                Block blk = blocks.back();
                blocks.pop_back();
                POOL_FREE(blk.ptr);
                freed += blk.capacity;
                stats.total_allocated -= blk.capacity;
                stats.blocks_in_pool--;
                stats.eviction_count++;
            }
            if (freed >= needed) break;
        }
    }

    Block* allocateNewBlock(size_t size) {
        size_t alloc_size = getSizeClass(size);
        if (stats.total_allocated + alloc_size > max_pool_size) {
            evictBlocks(alloc_size);
            if (stats.total_allocated + alloc_size > max_pool_size) {
                throw std::runtime_error("BlockPool: Memory limit exceeded");
            }
        }
        
        char* ptr = static_cast<char*>(POOL_ALLOC(alloc_size, ALIGNMENT));
        if (!ptr) throw std::bad_alloc();
        
        stats.total_allocated += alloc_size;
        stats.allocation_count++;
        return new Block(ptr, alloc_size);
    }

    void release(char* p, size_t capacity) {
        std::lock_guard<std::mutex> lock(mtx);
        size_classes[capacity].push_back(Block(p, capacity));
        stats.currently_in_use -= capacity;
        stats.blocks_in_pool++;
    }

    static constexpr size_t SIZE_BOUNDARY = 4UL * 1024 * 1024;
    static constexpr size_t SMALL_BLOCK_ALIGNMENT= 512;
    static constexpr size_t ALIGNMENT = 64;
    
    std::unordered_map<size_t, std::vector<Block>> size_classes;
    mutable std::mutex mtx;
    Stats stats;
    size_t max_pool_size;
};

} // namespace p2cllvm