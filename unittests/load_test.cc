// TODO: Update tests for ParquetTable-only architecture
// LocalTable has been removed - tests need refactoring
#if 0

#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <unordered_set>

#include "internal/tpch.h"
#include "internal/basetypes.h"
#include "runtime/Runtime.h"


TEST(LOAD_STRING, REGIONS_READ_CORRECTLY){
    std::string path = std::getenv("tpchpath") ? std::getenv("tpchpath")
                                             : "../../data-generator/output";
    std::string region_path = path + "/region";

    p2cllvm::LocalTable region_table;
    region_table.loadColumn<StringView>(region_path, "r_name");

    void* raw_data = region_table.getColumnData("r_name");

    std::vector<std::string_view> regions{
        "AFRICA",
        "AMERICA",
        "ASIA",
        "EUROPE",
        "MIDDLE EAST"
    };
    size_t i = 0;
    StringView sV;
    for(auto& sv: regions){
        load_from_slotted_page(i++, raw_data, &sV);
        std::string_view region{sV.data, sV.length};
        EXPECT_EQ(region, sv);
    }
}

TEST(LOAD_STRING, CORRECT_PAIR){
    std::string path = std::getenv("tpchpath") ? std::getenv("tpchpath")
                                            : "../../data-generator/output";
    InMemoryTPCH db{path};
    std::string lineitem_path = path + "/lineitem";

    p2cllvm::LocalTable lineitem_table;
    lineitem_table.loadColumn<char>(lineitem_path, "l_returnflag");
    lineitem_table.loadColumn<char>(lineitem_path, "l_linestatus");

    char* returnflag_data = static_cast<char*>(lineitem_table.getColumnData("l_returnflag"));
    char* linestatus_data = static_cast<char*>(lineitem_table.getColumnData("l_linestatus"));

    StringView s1, s2;
    std::unordered_set<std::string> pairs;
    for(size_t i = 0; i < 6001215; ++i){
        pairs.insert(std::string(1, returnflag_data[i]) + std::string(1, linestatus_data[i]));
    }
    EXPECT_EQ(pairs.size(), 4);
}

TEST(LOAD_INTEGER, LINEITEM_READ_CORRECTLY){
    std::string path = std::getenv("tpchpath") ? std::getenv("tpchpath")
                                             : "../../data-generator/output";
    InMemoryTPCH db{path};
    auto* r_regionkey_data = static_cast<int32_t*>(db.region.table->getColumnData("r_regionkey"));
    for(size_t i = 0; i < db.region.tuple_count; ++i){
        EXPECT_EQ(r_regionkey_data[i], i);
    }
}

#endif // 0 - LocalTable tests disabled
