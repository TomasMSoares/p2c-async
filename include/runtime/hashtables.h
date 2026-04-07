#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>

namespace p2cllvm {
struct HashTableEntry {
  union {
    uint64_t hash;
    HashTableEntry *next;
  };
  char data[];

  static llvm::StructType *createType(llvm::LLVMContext &context);
};

class HashTable {
public:
  char *insert(HashTableEntry *entry) { return insert(entry, entry->hash); }
  char *insert(HashTableEntry *entry, uint64_t hash) {
    auto idx = hash & (size - 1);
    auto &head = ht[idx];
    entry->next = head;
    head = entry;
    return entry->data;
  }

  char *insertWithTag(HashTableEntry *entry, uint64_t hash) {
    auto idx = hash & (size - 1);
    auto &head = ht[idx];
    entry->next = head;
    auto tag = ((reinterpret_cast<uintptr_t>(head) | hash) >> 48) << 48;
    head = reinterpret_cast<HashTableEntry *>(
        reinterpret_cast<uintptr_t>(entry) & ((1ull << 48) - 1) | tag);
    return entry->data;
  }

  char *insertWithTagThreaded(HashTableEntry *entry, uint64_t hash) {
    auto idx = hash & (size - 1);
    std::atomic_ref<HashTableEntry *> head_ref(ht[idx]);
    HashTableEntry *head, *desired;
    do {
      head = head_ref.load();
      entry->next = head;
      auto tag = ((reinterpret_cast<uintptr_t>(head) | hash) >> 48) << 48;
      desired = reinterpret_cast<HashTableEntry *>(
          reinterpret_cast<uintptr_t>(entry) & ((1ull << 48) - 1) | tag);
    } while (!head_ref.compare_exchange_weak(head, desired));
    return entry->data;
  }

  HashTableEntry *lookup(uint64_t hash) {
    auto idx = hash & (size - 1);
    assert(idx < size);
    return ht[idx];
  }
  constexpr uint64_t getThreshold() const { return (size * 10) / 7; }
  void flush() { std::fill_n(ht, size, nullptr); }
  HashTable(size_t estimate) : size(std::bit_floor(estimate)) {
    ht = new HashTableEntry *[size];
    std::fill_n(ht, size, nullptr);
    assert(std::popcount(size) == 1);
  }
  HashTable() : ht(nullptr), size(0) {};
  HashTable &operator=(HashTable &&other) {
    if (this != &other) {
      ht = other.ht;
      size = other.size;
      other.ht = nullptr;
      other.size = 0;
    }
    return *this;
  }

  ~HashTable() {
    if (ht != nullptr)
      delete[] ht;
  }

private:
  HashTableEntry **ht;
  size_t size;
};
}; // namespace p2cllvm
