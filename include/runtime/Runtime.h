#pragma once

#include "ThreadLocal.h"
#include "ThreadLocalContext.h"
#include "internal/basetypes.h"
#include "runtime/hashtables.h"
#include "runtime/hyperloglog.h"
#include "runtime/tuplebuffer.h"

#include <cstdint>
#include <thread>
using namespace p2cllvm;

template <typename C> C *local(ThreadLocalStorage<C> *ctx) {
  return ctx->getOrInsert(std::this_thread::get_id());
}

template <typename C> TupleBuffer *getLocalTB(C *lctx) {
  return lctx->getTupleBuffer();
}

template <typename C> TupleBuffer *getLocalStringHeap(C *lctx) {
  return lctx->getStringHeap();
}

template <typename C> size_t combineSketches(ThreadLocalStorage<C> *ctx) {
  auto [elems, size] = ctx->getElems();
  auto &sketch = elems.front().sketch;
  for (size_t i = 1; i < size; ++i) {
    sketch.merge(elems[i].sketch);
  }
  return sketch.estimate();
}

template <typename C> C *getContext(ThreadLocalStorage<C> *ctx, size_t idx) {
  return ctx->at(idx);
}

template<typename C> C* getItem(ThreadLocalStorage<C> *ctx, std::atomic<uint64_t>* idx){
    return ctx->at(idx->fetch_add(1));
}

template <typename C>size_t getNumThreadContext(ThreadLocalStorage<C>* ctx){ 
    return ctx->getElems().second;
}

template ThreadJoinContext *
local<ThreadJoinContext>(ThreadLocalStorage<ThreadJoinContext> *ctx);
template ThreadAggregationContext *local<ThreadAggregationContext>(
    ThreadLocalStorage<ThreadAggregationContext> *ctx);
template ThreadSortContext *
local<ThreadSortContext>(ThreadLocalStorage<ThreadSortContext> *ctx);

template TupleBuffer *getLocalTB<ThreadJoinContext>(ThreadJoinContext *lctx);
template TupleBuffer *
getLocalStringHeap<ThreadJoinContext>(ThreadJoinContext *lctx);
template TupleBuffer *
getLocalTB<ThreadAggregationContext>(ThreadAggregationContext *lctx);
template TupleBuffer *getLocalTB<ThreadSortContext>(ThreadSortContext *lctx);

template size_t
combineSketches<ThreadJoinContext>(ThreadLocalStorage<ThreadJoinContext> *ctx);
template size_t combineSketches<ThreadAggregationContext>(
    ThreadLocalStorage<ThreadAggregationContext> *ctx);

template ThreadJoinContext *
getContext<ThreadJoinContext>(ThreadLocalStorage<ThreadJoinContext> *ctx,
                              size_t idx);
template ThreadAggregationContext *getContext<ThreadAggregationContext>(
    ThreadLocalStorage<ThreadAggregationContext> *ctx, size_t idx);
template ThreadSortContext *
getContext<ThreadSortContext>(ThreadLocalStorage<ThreadSortContext> *ctx,
                              size_t idx);

template size_t getNumThreadContext(ThreadLocalStorage<ThreadAggregationContext> *ctx);
template size_t getNumThreadContext(ThreadLocalStorage<ThreadJoinContext> *ctx);


template ThreadJoinContext* getItem(ThreadLocalStorage<ThreadJoinContext>* ctx, std::atomic<uint64_t>* idx);


///----------------------------------------------------
/// HashTable
uint64_t hash(char *x, size_t len);

template<typename T>
uint64_t hash_crc(T val, uint64_t crc);

uint64_t hash_crc8(char* x, size_t len, uint64_t crc);

char *hashtable_insert(HashTable *ht, HashTableEntry *entry, uint64_t hash);

char* hashtable_insert_tagged(HashTable* ht, HashTableEntry* entry, uint64_t hash);

HashTableEntry *hashtable_lookup(HashTable *ht, uint64_t hash);

void hashtable_alloc(HashTable *ht, uint64_t size);

///----------------------------------------------------
/// HyperLogLog
void hll_add(Sketch *sketch, uint64_t hash);

uint64_t hll_estimate(Sketch *sketch);

///----------------------------------------------------
/// TupleBuffer
char *tb_insert(TupleBuffer *tb, size_t elem_size);

Buffer *getBuffers(TupleBuffer *tb);

uint64_t getNumBuffers(TupleBuffer *tb);

///----------------------------------------------------
/// Strings
bool like_prefix(StringView *sv, char *prefix, size_t len);

bool like_suffix(StringView *sv, char *prefix, size_t len);

bool like(StringView *sv, char *prefix, size_t len);

bool string_eq(const StringView *s1, const StringView *s2);

bool string_lt(StringView *s1, StringView *s2);

bool string_gt(StringView *s1, StringView *s2);

///----------------------------------------------------
///  Page
void load_from_slotted_page(uint64_t idx, void *slotted_page_data,
                            StringView *sv);

///----------------------------------------------------
/// Print

void printStringView(StringView *sv);

void printDate(int32_t x);

void printDouble(double x);

void printBigInt(int64_t x);

void printBool(bool x);

void printInteger(int32_t x);

void printChar(char x);

void printNewline();
///----------------------------------------------------
/// Debug

void printDebug(void *ptr);

void* signExtend(void* ptr);

bool cmpTag(void* ptr, uint64_t hash);

///----------------------------------------------------
/// Join
char *insertJoinEntry(ThreadJoinContext *ctx, uint64_t hash, size_t elem_size);

char *allocStringHeap(TupleBuffer *heap, size_t size);

void insertAll(ThreadLocalStorage<ThreadJoinContext> *ctx, HashTable *ht,
               size_t elem_size);
void insertMultithreaded(ThreadJoinContext* ctx, HashTable* ht, size_t elem_size);

///----------------------------------------------------
/// Aggregation
HashTable *getLocalHashTable(ThreadAggregationContext *ctx);

void insertAggEntry(ThreadAggregationContext *ctx, uint64_t hash,
                    HashTableEntry *entry);
///-------------------------------------------------------
/// Sort
using SortBuffer = Buffer;

void allocSortBuffer(SortBuffer* sb, size_t size);
size_t combineSizes(ThreadLocalStorage<ThreadSortContext> *ctx, size_t tsize);
void insertSortBuffer(ThreadLocalStorage<ThreadSortContext> *ctx,
                      SortBuffer *buf, size_t size);
void sortBuffer(SortBuffer *buf, size_t size, int(*cmp)(const void*, const void*));

char* insertSortEntry(ThreadSortContext* lctx, size_t size);

char* getSorted(SortBuffer* sb);

