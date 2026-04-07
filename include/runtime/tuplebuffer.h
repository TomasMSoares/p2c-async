#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <functional>
#include <vector>
#include <stdexcept>
#include "runtime/BlockPool.h"

namespace p2cllvm {

using BlockPoolDeleter = std::function<void(char*)>;

class Builder;

struct Buffer {
    uint64_t ptr;
    uint64_t size;
    char *mem; 
    
    // Memory management field: holds the function that calls BlockPool::release(mem, size)
    BlockPoolDeleter deleter; 

    Buffer(): ptr(0), size(0), mem(nullptr) {}

    // Constructor: Acquires memory from BlockPool
    Buffer(uint64_t requested_size) : ptr(0) {
        if (requested_size == 0) {
            size = 0;
            mem = nullptr;
            return;
        }
        
        auto result = BlockPool::get().acquire(requested_size);
        
        // Extract the unique_ptr and the capacity
        std::unique_ptr<char, BlockPoolDeleter> mem_holder = std::move(result.first);
        size = result.second; // Store the actual allocated capacity

        // release raw pointer ownership from unique_ptr 
        // and save the deleter for manual call in the destructor
        mem = mem_holder.release(); 
        deleter = mem_holder.get_deleter(); 

        if (mem == nullptr) {
             throw std::bad_alloc();
        }
    }

    ~Buffer() {
        if (mem != nullptr) {
            deleter(mem); 
        }
    }

    Buffer(Buffer &&other) 
        : ptr(other.ptr), size(other.size), mem(other.mem), 
          deleter(std::move(other.deleter)) {
        other.ptr = 0;
        other.size = 0;
        other.mem = nullptr;
    }

    Buffer &operator=(Buffer &&other) {
        if (this != &other) {
            if (mem != nullptr) {
                deleter(mem);
            }
            
            ptr = other.ptr;
            size = other.size;
            mem = other.mem;
            deleter = std::move(other.deleter);

            other.ptr = 0;
            other.size = 0;
            other.mem = nullptr;
        }
        return *this;
    }

    char* insertUnchecked(size_t elemSize);

    static llvm::StructType *createType(llvm::LLVMContext &context);
};

class TupleBuffer {
public:
  TupleBuffer(uint64_t initial_base_size = 64 * sysconf(_SC_PAGESIZE))
      : base(initial_base_size) {}

  char*alloc(size_t elem_size) {
    if (buffers.empty() || buffers.back().size - buffers.back().ptr < elem_size){
      buffers.emplace_back(base *= 2);
    }
    auto &[ptr, size, mem, deleter] = buffers.back(); 
    auto *elem = mem + ptr;
    ptr += elem_size;
    return elem;
  }

  Buffer *getBuffers() { 
      return buffers.data(); }
  uint64_t getNumBuffers() {
      return buffers.size(); }

private:
  std::vector<Buffer> buffers;
  uint64_t base;
};
} // namespace p2cllvm