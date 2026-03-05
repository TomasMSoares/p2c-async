#pragma once

#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Type.h>
#include <llvm/Support/raw_ostream.h>
#include <vector>

#include "internal/basetypes.h"
#include "operators/Iu.h"

namespace p2cllvm {
using Layout = llvm::DenseMap<IU*, size_t>;

class Tuple {
public:
  Tuple(llvm::StructType *type, size_t size, Layout&& layout)
      : type(type), size(size), layout(std::move(layout)) {}
  Tuple() = default;

  static Tuple get(llvm::LLVMContext &context,
                   const IUSet &ius) {
    auto *type = p2cllvm::getOrCreateType(context, "tuple", [&]() {
      return llvm::StructType::create(
          context, {llvm::ArrayType::get(llvm::Type::getInt8Ty(context), 0)},
          "tuple");
    });
    auto [layout, size] = createPackedTuple(ius.v);
    return {type, size, std::move(layout)};
  }

  static Tuple get(llvm::LLVMContext &context, std::vector<IU*>& keys, std::vector<IU*>& values){
    auto *type = p2cllvm::getOrCreateType(context, "tuple", [&]() {
      return llvm::StructType::create(
          context, {llvm::ArrayType::get(llvm::Type::getInt8Ty(context), 0)},
          "tuple");
    });
    auto elems = keys;
    elems.insert(elems.end(), values.begin(), values.end());
    auto [layout, size] = createPackedTuple(elems);
    return {type, size, std::move(layout)};
  }

  llvm::StructType *getType() const { return type; }

  size_t getSize() const { return size; }

  const Layout &getLayout() const { return layout; } 

private:
  static std::pair<Layout, size_t> createPackedTuple(const std::vector<IU *> &cols);
  llvm::StructType *type;
  size_t size;
  llvm::DenseMap<IU*, size_t> layout;
};
}; // namespace p2cllvm
