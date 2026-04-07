#pragma once
#include <algorithm>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>

#include "internal/basetypes.h"
#include "IR/Types.h"
namespace p2cllvm {
struct Type;    
struct IU {
  std::string name;
  Type type;

  IU(std::string_view name, TypeEnum typeEnum);
};

// an unordered set of IUs
struct IUSet {
  // set is represented as array; invariant: IUs are sorted by pointer value
  std::vector<IU *> v;

  // empty set constructor
  IUSet() {}

  // move constructor
  IUSet(IUSet &&x) { v = std::move(x.v); }

  // copy constructor
  IUSet(const IUSet &x) { v = x.v; }

  // convert vector to set of IUs (assumes vector is unique, but not sorted)
  explicit IUSet(const std::vector<IU *> &vv) {
    v = vv;
    sort(v.begin(), v.end());
    // check that there are no duplicates
    assert(adjacent_find(v.begin(), v.end()) == v.end());
  }

  // iterate over IUs
  IU **begin() { return v.data(); }
  IU **end() { return v.data() + v.size(); }
  IU *const *begin() const { return v.data(); }
  IU *const *end() const { return v.data() + v.size(); }

  void add(IU *iu) {
    auto it = lower_bound(v.begin(), v.end(), iu);
    if (it == v.end() || *it != iu)
      v.insert(it, iu); // O(n), not nice
  }

  bool contains(IU *iu) const {
    auto it = lower_bound(v.begin(), v.end(), iu);
    return (it != v.end() && *it == iu);
  }

  unsigned size() const { return v.size(); };
};

// set union operator
IUSet operator|(const IUSet &a, const IUSet &b);

// set intersection operator
IUSet operator&(const IUSet &a, const IUSet &b);

// set difference operator
IUSet operator-(const IUSet &a, const IUSet &b);

// set equality operator
bool operator==(const IUSet &a, const IUSet &b);
}; // namespace p2cllvm
