#pragma once

#include <atomic>
#include <bit>
#include <cassert>
#include <new>
#include <span>
#include <thread>
#include <type_traits>
#include <vector>

constexpr size_t cacheLineSize = 64;

template <typename T> constexpr size_t pad() {
  return (sizeof(T) + cacheLineSize - 1) & ~(cacheLineSize - 1);
}

/// Actual ThreadLocalStorage implementation
template <typename T> struct ThreadLocalStorageData {
  std::atomic<T *> data = nullptr;
  std::thread::id threadId{};

  bool insert(T *value) {
    if (data)
      return false;
    T* expected = nullptr;
    return data.compare_exchange_strong(expected, value);
  }
};

template <typename T> class AlignedAllocator {
public:
  using value_type = T;

  AlignedAllocator() = default;

  T *allocate(size_t n) { 
    auto* ptr = reinterpret_cast<T*>(::operator new[] (sizeof(T) * n ,std::align_val_t(cacheLineSize)));
    assert((std::bit_cast<intptr_t>(ptr) & 63) == 0);
    return ptr;
  }

  void deallocate(T *p, size_t n) {
    assert((std::bit_cast<intptr_t>(p) & 63) == 0);
    ::operator delete[] (p, n * sizeof(T), std::align_val_t(cacheLineSize));
  }
};

/// ObjectPool implementation for continus access
template <typename T> struct ObjectPool {
  /// Stored in pool to avoid false sharing
  struct PoolNode : public T {
    char padding[pad<T>() - sizeof(T)];
  };

  /// Bump allocator
  alignas(cacheLineSize) std::atomic<size_t> cur;
  std::vector<PoolNode, AlignedAllocator<PoolNode>> pool;

  ObjectPool() = delete;

  ObjectPool(size_t size) : cur(0), pool(size) {}
  T *alloc() {
    size_t idx = cur.fetch_add(1);
    assert(idx < pool.size());
    return &pool[idx];
  }

  static_assert((sizeof(PoolNode) & 63) == 0,
                "PoolNode must be aligned to cache line size");
};

template <typename T> requires std::is_default_constructible_v<T>
class ThreadLocalStorage {
public:
  // Struct def
  struct OpenAddrNode : public ThreadLocalStorageData<T> {
    char padding[pad<ThreadLocalStorageData<T>>() -
                 sizeof(ThreadLocalStorageData<T>)];
  };
  /// Interface

  ThreadLocalStorage(size_t numThreads = std::thread::hardware_concurrency())
      : size(std::bit_ceil((numThreads * 10) / 7)), data(size), pool(numThreads) {
    assert(size > 0);
    assert(pool.pool.size() >= numThreads);
  }

  T *getOrInsert(std::thread::id threadId) {
    auto hash = hasher(threadId) & (size - 1);
    assert(hash < size);
    auto idx = search(hash, threadId);
    if (idx != size) 
      return data[idx].data;
    
    /// allocate new element
    auto *ptr = pool.alloc();
    idx = hash;
    do {
      if (data[idx].insert(ptr)) {
        data[idx].threadId = threadId;
        return ptr;
      }
      idx = (idx + 1) & (size - 1);
    } while (idx != hash);
    return nullptr;
  }

  std::pair<std::span<typename ObjectPool<T>::PoolNode>, size_t> getElems() {
    return {std::span(pool.pool), pool.cur};
  }

  auto begin() { return pool.pool.begin(); }
    auto end() { return pool.pool.begin() + pool.cur; }

  T *at(size_t idx) { 
      return &pool.pool[idx]; 
  }

  static_assert(
      (sizeof(OpenAddrNode) & 63) == 0,
      "OpenAddrNode must be aligned to cache line size");

private:
  size_t size;
  std::vector<OpenAddrNode, AlignedAllocator<OpenAddrNode>> data;
  ObjectPool<T> pool;
  std::hash<std::thread::id> hasher;

  size_t search(size_t idx, std::thread::id threadId) {
    size_t start = idx;
    do {
      if (!data[idx].data) {
        return size;
      } else if (data[idx].threadId == threadId) {
        return idx;
      }
      idx = (idx + 1) & (size - 1);
    } while (idx != start);
    return size;
  }
};
