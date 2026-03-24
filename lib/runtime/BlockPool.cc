#include "runtime/BlockPool.h"
#include <algorithm>
#include <stdexcept>
#include <cstring>
#include <cstdlib>

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#define POOL_ALLOC(size, align) mallocx(size, MALLOCX_ALIGN(align))
#define POOL_FREE(ptr) dallocx(ptr, 0)
#else
#define POOL_ALLOC(size, align) std::aligned_alloc(align, size)
#define POOL_FREE(ptr) std::free(ptr)
#endif

namespace p2cllvm {

BlockPool& BlockPool::get() {
    static BlockPool instance;
    return instance;
}

BlockPool::BlockPool(size_t max_size) : max_pool_size(max_size) {}

BlockPool::~BlockPool() {
    std::lock_guard<std::mutex> lock(mtx);
    for (auto& [_, blocks] : size_classes) {
        for (auto& blk : blocks) {
            POOL_FREE(blk.ptr);
        }
    }
}

std::pair<std::unique_ptr<char, std::function<void(char*)>>, size_t> BlockPool::acquire(size_t size) {
    std::lock_guard<std::mutex> lock(mtx);
    
    Block* blk = findBlock(size);
    if (!blk) {
        blk = allocateNewBlock(size);
    }
    
    stats.currently_in_use += blk->capacity;
    size_t capacity = blk->capacity;
    char* ptr = blk->ptr;
    delete blk; 

    // Custom deleter returns the pointer back to the Singleton pool
    std::unique_ptr<char, std::function<void(char*)>> result_ptr = 
    {ptr, [capacity](char* p) { 
        BlockPool::get().release(p, capacity); 
    }};

    return {std::move(result_ptr), capacity};
}

bool BlockPool::canAllocate(size_t size) const {
    std::lock_guard<std::mutex> lock(mtx);
    return stats.total_allocated + getSizeClass(size) <= max_pool_size;
}

size_t BlockPool::availableMemory() const {
    std::lock_guard<std::mutex> lock(mtx);
    return max_pool_size - stats.currently_in_use;
}

BlockPool::Stats BlockPool::getStats() const {
    std::lock_guard<std::mutex> lock(mtx);
    Stats s = stats;
    s.blocks_in_pool = 0;
    for (const auto& [_, blocks] : size_classes) {
        s.blocks_in_pool += blocks.size();
    }
    return s;
}

void BlockPool::setMaxPoolSize(size_t new_max) {
    std::lock_guard<std::mutex> lock(mtx);
    max_pool_size = new_max;
    if (stats.total_allocated > max_pool_size) {
        evictBlocks(stats.total_allocated - max_pool_size);
    }
}

void BlockPool::trim() {
    std::lock_guard<std::mutex> lock(mtx);
    for (auto& [_, blocks] : size_classes) {
        for (auto& blk : blocks) {
            POOL_FREE(blk.ptr);
            stats.total_allocated -= blk.capacity;
        }
        blocks.clear();
    }
}

// Private Helpers
size_t BlockPool::getSizeClass(size_t requested) {
    if (requested == 0) return ALIGNMENT;
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

BlockPool::Block* BlockPool::findBlock(size_t size) {
    size_t size_class = getSizeClass(size);
    auto it = size_classes.find(size_class);
    if (it != size_classes.end() && !it->second.empty()) {
        Block blk = it->second.back();
        it->second.pop_back();
        stats.reuse_count++;
        return new Block(blk.ptr, blk.capacity);
    }
    
    for (size_t cls = size_class * 2; cls <= size_class * 4; cls *= 2) {
        it = size_classes.find(cls);
        if (it != size_classes.end() && !it->second.empty()) {
            Block blk = it->second.back();
            it->second.pop_back();
            stats.reuse_count++;
            return new Block(blk.ptr, blk.capacity);
        }
    }
    return nullptr;
}

void BlockPool::evictBlocks(size_t needed) {
    size_t freed = 0;
    std::vector<size_t> classes;
    for (const auto& [cls, _] : size_classes) classes.push_back(cls);
    std::sort(classes.rbegin(), classes.rend());
    
    for (size_t cls : classes) {
        auto& blocks = size_classes[cls];
        while (!blocks.empty() && freed < needed) {
            Block blk = blocks.back();
            blocks.pop_back();
            POOL_FREE(blk.ptr);
            freed += blk.capacity;
            stats.total_allocated -= blk.capacity;
            stats.eviction_count++;
        }
        if (freed >= needed) break;
    }
}

BlockPool::Block* BlockPool::allocateNewBlock(size_t size) {
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

void BlockPool::release(char* p, size_t capacity) {
    std::lock_guard<std::mutex> lock(mtx);
    size_classes[capacity].push_back(Block(p, capacity));
    stats.currently_in_use -= capacity;
}

} // namespace p2cllvm