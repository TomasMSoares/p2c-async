#include <cstdint>
#include <gtest/gtest.h>
#include <random>

#include "runtime/Runtime.h"
#include "runtime/tuplebuffer.h"

using namespace p2cllvm;

TEST(MaterializationTest, Basic) {
  constexpr size_t elem_size = 24;
  TupleBuffer buffer{16};
  std::mt19937 rng(0);
  using TargetTuple = std::tuple<uint64_t, uint64_t, uint64_t>;
  std::vector<TargetTuple> tuples(8192);
  for (auto &[t1, t2, t3] : tuples) {
    t1 = rng();
    t2 = rng();
    t3 = rng();
  } 

  for (auto &[t1, t2, t3] : tuples) {
    auto *tuple = tb_insert(&buffer, 24);
    auto *target = reinterpret_cast<uint64_t *>(tuple);
    target[0] = t1;
    target[1] = t2;
    target[2] = t3;
  }

  EXPECT_EQ(buffer.getNumBuffers(), 2);
  auto *buffers = buffer.getBuffers();
  auto num_buffers = buffer.getNumBuffers();
  size_t ii = 0;
  for (auto i = 0ull; i < num_buffers; ++i) {
    auto ctuples = buffers[i].ptr;
    for (auto j = 0ull; j < ctuples; j += elem_size) {
      auto *tuple = reinterpret_cast<uint64_t *>(buffers[i].mem + j);
      EXPECT_EQ(tuple[0], std::get<0>(tuples[ii]));
      EXPECT_EQ(tuple[1], std::get<1>(tuples[ii]));
      EXPECT_EQ(tuple[2], std::get<2>(tuples[ii]));
      ++ii;
    }
  }
}
