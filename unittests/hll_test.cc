// TODO: Update tests for ParquetTable-only architecture
// LocalTable has been removed - tests need refactoring
#if 0

#include <gtest/gtest.h>
#include <string>

#include "internal/tpch.h"
#include "runtime/Murmur.h"

TEST(HLLTEST, HLL_VAL) {
  std::string path = std::getenv("tpchpath") ? std::getenv("tpchpath")
                                             : "../../data-generator/output";
  InMemoryTPCH db{path};
  Sketch sketch;
  auto* p_partkey_data = static_cast<int32_t*>(db.part.table->getColumnData("p_partkey"));
  for (size_t i = 0; i < db.part.tuple_count; ++i) {
    sketch.add(
        murmurHash(reinterpret_cast<const char *>(&p_partkey_data[i]),
                   sizeof(int32_t)));
  }
  EXPECT_GE(sketch.estimate(), db.part.tuple_count * 0.8);
}

#endif // 0 - LocalTable tests disabled
