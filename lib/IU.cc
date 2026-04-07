#include "internal/basetypes.h"
#include "operators/Iu.h"
#include "IR/Types.h"

p2cllvm::IU::IU(std::string_view name, TypeEnum typeEnum)
    : name(name), type(Type::get(typeEnum)) {}

namespace p2cllvm {
IUSet operator|(const IUSet &a, const IUSet &b) {
  IUSet result;
  set_union(a.v.begin(), a.v.end(), b.v.begin(), b.v.end(),
            back_inserter(result.v));
  return result;
}

IUSet operator&(const IUSet &a, const IUSet &b) {
  IUSet result;
  set_intersection(a.v.begin(), a.v.end(), b.v.begin(), b.v.end(),
                   back_inserter(result.v));
  return result;
}

IUSet operator-(const IUSet &a, const IUSet &b) {
  IUSet result;
  set_difference(a.v.begin(), a.v.end(), b.v.begin(), b.v.end(),
                 back_inserter(result.v));
  return result;
}

bool operator==(const IUSet &a, const IUSet &b) {
  return equal(a.v.begin(), a.v.end(), b.v.begin(), b.v.end());
}
} // namespace p2cllvm
