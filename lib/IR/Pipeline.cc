#include "IR/Pipeline.h"
#include "internal/Database.h"
#include "internal/ParquetTable.h"
#include <cstdint>
namespace p2cllvm {
    uint64_t Query::getLineItemCount() const{
        auto* lineitem = dynamic_cast<ParquetTable*>(dbref.getTable("lineitem"));
        if (!lineitem) return 0;
        
        const auto& metadata = lineitem->getMetadata();
        uint64_t total_rows = 0;
        for (const auto& rg : metadata.row_groups) {
            total_rows += rg.num_rows;
        }
        return total_rows;
    }
}