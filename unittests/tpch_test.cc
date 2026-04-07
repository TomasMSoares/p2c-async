// TODO: Update tests for ParquetTable-only architecture
// InMemoryTPCH and tuple_count have been removed
#if 0

#include <gtest/gtest.h>

#include "internal/tpch.h"



TEST(TPCH, PART_READ_CORRECTLY) {
  std::string path = std::getenv("tpchpath") ? std::getenv("tpchpath")
                                             : "../../data-generator/output";
  InMemoryTPCH db{path};
  EXPECT_EQ(db.part.tuple_count, 200000);
  EXPECT_EQ(db.lineitem.tuple_count, 6001215);
  EXPECT_EQ(db.partsupp.tuple_count, 800000);
  EXPECT_EQ(db.customer.tuple_count, 150000);
  EXPECT_EQ(db.region.tuple_count, 5);
  EXPECT_EQ(db.orders.tuple_count, 1500000);
}

#endif // 0 - InMemoryTPCH tests disabled
