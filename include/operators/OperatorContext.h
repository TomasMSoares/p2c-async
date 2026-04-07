#pragma once

#include "runtime/Runtime.h"
#include "runtime/ThreadLocal.h"
#include "runtime/ThreadLocalContext.h"
#include "runtime/hashtables.h"
#include "runtime/tuplebuffer.h"
#include <cstdint>
#include <string>

namespace p2cllvm {
struct OperatorContext {
  virtual ~OperatorContext() = default;
};

struct JoinContext : public OperatorContext {
  ThreadLocalStorage<ThreadJoinContext> tls;
  HashTable ht;
  std::atomic<uint64_t> idx = 0;
};

struct AggregationContext : public OperatorContext {
    ThreadLocalStorage<ThreadAggregationContext> tls;
    TupleBuffer tb8{sizeof(void*)};
    HashTable ht;
};

struct SortContext : public OperatorContext{
    ThreadLocalStorage<ThreadSortContext> tls;
    SortBuffer sb;
};

struct AssertContext : public OperatorContext{
    std::vector<std::string> iter;
    size_t i = 0;
};

struct AssertLengthContext : public OperatorContext{
    size_t len;
    AssertLengthContext(size_t len): len(len) {}
};
} // namespace p2cllvm
