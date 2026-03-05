#include <algorithm>
#include <gtest/gtest.h>
#include <thread>
#include <barrier>

#include "operators/Aggregation.h"
#include "runtime/Runtime.h"
#include "runtime/ThreadLocal.h"
#include "runtime/ThreadLocalContext.h"

TEST(ThreadLocalTest, Basic) {
  struct TestStruct {
    std::unique_ptr<std::thread::id> threadId;
  };

  ThreadLocalStorage<TestStruct> tls;

  auto *storage = tls.getOrInsert(std::this_thread::get_id());
  EXPECT_NE(storage, nullptr);
  storage->threadId =
      std::make_unique<std::thread::id>(std::this_thread::get_id());

  auto *mystorage = tls.getOrInsert(std::this_thread::get_id());
  EXPECT_EQ(storage, mystorage);
  EXPECT_EQ(tls.at(0)->threadId, storage->threadId);
}

TEST(ThreadLocalTest, MultiThreaded) {
  struct TestStruct {
    std::thread::id threadId;
  };

  std::vector<std::thread> threads(4);
  ThreadLocalStorage<TestStruct> tls;
  std::barrier<> barrier(threads.size());

  for (auto &thread : threads) {
    thread = std::thread([&]() {
      auto *storage = tls.getOrInsert(std::this_thread::get_id());
      EXPECT_NE(storage, nullptr);
      storage->threadId = std::this_thread::get_id();
      auto *mystorage = tls.getOrInsert(std::this_thread::get_id());
      EXPECT_EQ(storage, mystorage);
      auto found =std::find_if(tls.begin(), tls.end(),
                             [&](const auto &elem) {
                               return elem.threadId ==
                                      std::this_thread::get_id();
                             });
      EXPECT_NE(found, tls.end());
      EXPECT_EQ(storage, &(*found));
    });

  }

  for (auto &thread : threads)
    thread.join();
}

TEST(ThreadLocalTest, MultiThreadedFull) {
  struct TestStruct {
    std::thread::id threadId;
  };

  std::vector<std::thread> threads(std::thread::hardware_concurrency());
  ThreadLocalStorage<TestStruct> tls;
  std::barrier<> barrier(threads.size());

  for (auto &thread : threads) {
    thread = std::thread([&]() {
      auto *storage = tls.getOrInsert(std::this_thread::get_id());
      EXPECT_NE(storage, nullptr);
      storage->threadId = std::this_thread::get_id();
      auto *mystorage = tls.getOrInsert(std::this_thread::get_id());
      EXPECT_EQ(storage, mystorage);
      auto found =std::find_if(tls.begin(), tls.end(),
                             [&](const auto &elem) {
                               return elem.threadId ==
                                      std::this_thread::get_id();
                             });
      EXPECT_NE(found, tls.end());
      EXPECT_EQ(storage, &(*found));
    });

  }

  for (auto &thread : threads)
    thread.join();
}

TEST(ThreadLocalTest, MultiThreadedAggregation) {

  std::vector<std::thread> threads(std::thread::hardware_concurrency());
  ThreadLocalStorage<ThreadAggregationContext> tls;
  std::barrier<> barrier(threads.size());

  for (auto &thread : threads) {
    thread = std::thread([&]() {
      auto *storage = local<ThreadAggregationContext>(&tls);
      EXPECT_NE(storage, nullptr);
      auto* tb = storage->getTupleBuffer();
      *tb = TupleBuffer(sizeof(std::thread::id));
      auto *mystorage = local<ThreadAggregationContext>(&tls);
      EXPECT_EQ(storage, mystorage);
    });

  }

  for (auto &thread : threads)
    thread.join();
}
