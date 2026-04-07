#pragma once

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <llvm/ADT/ArrayRef.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/Support/raw_ostream.h>

#include "IR/Builder.h"
#include "IR/Defs.h"
#include "IR/Types.h"
#include "internal/basetypes.h"
#include "runtime/Runtime.h"

namespace p2cllvm {
template <typename T> struct Hasher {
  static ValueRef<> createHash(llvm::ArrayRef<IU *> ius, Builder &builder) {
    llvm::SmallVector<llvm::Value *, 8> hashvals;
    auto &scope = builder.getCurrentScope();
    if (ius.empty())
      return llvm::ConstantInt::get(
          llvm::Type::getInt64Ty(builder.getContext()), 0);

    llvm::SmallVector<ValueRef<>, 8> hvals;
    ValueRef<> acc = T::createHashSingle(ius.front(), builder);
    for(size_t i = 1; i < ius.size(); ++i)
        acc = T::createHashImpl(ius[i], acc, builder);
    return acc;
  }
};

struct MurmurHasher : public Hasher<MurmurHasher> {
  static ValueRef<> createHashTemplate(ValueRef<> val, Builder &builder,
                                       ValueRef<> len) {
    llvm::Module *module = builder.query.getModule();
    return builder.createCall("hash", &hash, builder.getInt64ty(), val, len);
  }

  static inline ValueRef<> createCombineHash(ValueRef<> h1, ValueRef<> h2,
                                             Builder &builder) {

    ValueRef<> lhash = builder.builder.CreateShl(h2, 6);
    ValueRef<> rhash = builder.builder.CreateLShr(h2, 2);
    ValueRef<> added = builder.builder.CreateAdd(lhash, rhash);
    added = builder.builder.CreateAdd(
        added, builder.getInt64Constant(0x517cc1b727220a95ul));
    added = builder.builder.CreateAdd(added, h1);
    return builder.builder.CreateXor(h2, added);
  }

  static inline ValueRef<> createHashSingle(IU *iu, Builder &builder) {
    auto type = iu->type.typeEnum;
    auto &scope = builder.getCurrentScope();
    ValueRef<> val = scope.lookupPtr(iu);
    if (!val)
      val =
          scope.updatePtr(iu, builder.createStackStore(scope.lookupValue(iu)));
    auto &context = builder.getContext();
    switch (type) {
    case TypeEnum::Integer:
    case TypeEnum::Double:
    case TypeEnum::Char:
    case TypeEnum::BigInt:
    case TypeEnum::Bool:
    case TypeEnum::Date:
      return createHashTemplate(
          val, builder,
          builder.getInt64Constant(typeSizes[static_cast<uint8_t>(type)]));
    case TypeEnum::String: {
      auto *t = StringTy::createType(context);
      return createHashTemplate(
          builder.builder.CreateLoad(
              builder.getPtrTy(), builder.builder.CreateStructGEP(t, val, 0)),
          builder,
          builder.builder.CreateLoad(
              builder.getInt64ty(),
              builder.builder.CreateStructGEP(t, val, 1)));
    }
    default:
      throw std::runtime_error("Unsupported type");
    }
  }

  static inline ValueRef<> createHashImpl(IU *iu, ValueRef<> hacc,
                                          Builder &builder) {
    ValueRef<> hval = createHashSingle(iu, builder);
    return createCombineHash(hacc, hval, builder);
  }
};

} // namespace p2cllvm
