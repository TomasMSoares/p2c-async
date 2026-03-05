#include "IR/Defs.h"
#include "IR/Types.h"
#include "internal/basetypes.h"
#include "runtime/hashtables.h"
#include <llvm/IR/DerivedTypes.h>

namespace p2cllvm {
TypeRef<llvm::StructType> HashTableEntry::createType(llvm::LLVMContext &context) {
  auto* hunion = getOrCreateType(context, "union-anon", [&]() {
    return llvm::StructType::create(
        context,
        {BigIntTy::createType(context)},
        "union-anon");
  });
  return getOrCreateType(context, "HashTableEntry", [&]() {
    return llvm::StructType::create(
        context,
        {hunion,
         llvm::ArrayType::get(CharTy::createType(context), 0)},
        "HashTableEntry");
  });
}
}; // namespace p2cllvm
