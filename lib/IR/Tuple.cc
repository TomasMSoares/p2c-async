#include "IR/Tuple.h"
#include "internal/basetypes.h"
#include "IR/Types.h"
#include <llvm/Support/raw_ostream.h>

namespace p2cllvm {
    std::pair<Layout, size_t> Tuple::createPackedTuple(const std::vector<IU *> &cols) {
    std::vector<IU *> sortedCols(cols); 
    std::sort(sortedCols.begin(), sortedCols.end(),
              [](const auto &a, const auto &b) {
                return typeAlignments[static_cast<int>(a->type.typeEnum)] >
                       typeAlignments[static_cast<int>(b->type.typeEnum)];
              });
    Layout offsets(cols.size());
    size_t offset = 0;
    for (auto iu : sortedCols) {
        offsets[iu] = offset;
        offset += typeSizes[static_cast<int>(iu->type.typeEnum)];
    }
    size_t max_align = typeAlignments[static_cast<size_t>(sortedCols.front()->type.typeEnum)];
    return {offsets, (offset + max_align - 1) & ~(max_align - 1)};
  }
}
