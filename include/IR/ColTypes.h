#pragma once

#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Type.h>
#include <string_view>

#include "Builder.h"
#include "IR/Defs.h"
#include "Types.h"
#include "internal/basetypes.h"
#include "internal/utils.h"
#include "runtime/Runtime.h"

namespace p2cllvm {

template <typename T> struct p2c_col_gen;

// Generic generator for the standard { void* data, int64_t size } struct
struct GenericColumnTypeGen {
  static TypeRef<> createType(llvm::LLVMContext &context) {
    // NOTE: ColumnMapping only refers to the LLVM name for the symbolic representation
    // of (void* data, uint64_t size) pair in ColumnMappingJIT
    return getOrCreateType(context, "ColumnMapping", [&]() {
      return llvm::StructType::create("ColumnMapping",
                                      llvm::PointerType::get(context, 0),
                                      llvm::Type::getInt64Ty(context));
    });
  }
};

template <> struct p2c_col_gen<int32_t> : public GenericColumnTypeGen {
  static ValueRef<> createAccess(ValueRef<> index, ValueRef<> ptr, Builder &builder) {
    auto *elemptr = builder.builder.CreateGEP(
        IntegerTy::createType(builder.getContext()), ptr, {index});
    return elemptr;
  }
};

template <> struct p2c_col_gen<double> : public GenericColumnTypeGen {
  static ValueRef<> createAccess(ValueRef<> index, ValueRef<> ptr, Builder &builder) {
    auto *elemptr = builder.builder.CreateInBoundsGEP(
        llvm::Type::getDoubleTy(builder.getContext()), ptr, {index});
    return elemptr;
  }
};

template <> struct p2c_col_gen<char> : public GenericColumnTypeGen {
  static ValueRef<> createAccess(ValueRef<> index, ValueRef<> ptr, Builder &builder) {
    auto *elemptr = builder.builder.CreateInBoundsGEP(
        llvm::Type::getInt8Ty(builder.getContext()), ptr, {index});
    return elemptr;
  }
};

template <> struct p2c_col_gen<StringView> : public GenericColumnTypeGen {
  static ValueRef<> createAccess(ValueRef<> index, ValueRef<> ptr, Builder &builder) {
    auto &context = builder.getContext();
    auto *sv = builder.createAlloca(StringTy::createType(context));
    auto *module = builder.query.getModule();
    auto fn = builder.query.symbolManager.getOrInsertFunction(
        module, "load_from_slotted_page", &load_from_slotted_page,
        llvm::Type::getVoidTy(context), llvm::Type::getInt64Ty(context),
        llvm::PointerType::get(context, 0), llvm::PointerType::get(context, 0));
    builder.builder.CreateCall(fn, {index, ptr, sv});
    return sv;
  }
};

template <> struct p2c_col_gen<Date> : public GenericColumnTypeGen {
  static ValueRef<> createAccess(ValueRef<> index, ValueRef<> ptr, Builder &builder) {
    auto *date_t = DateTy::createType(builder.getContext());
    auto *elemptr = builder.builder.CreateInBoundsGEP(date_t, ptr, index);
    return elemptr;
  }
};

template <> struct p2c_col_gen<bool> : public GenericColumnTypeGen {
  static ValueRef<> createAccess(ValueRef<> index, ValueRef<> ptr, Builder &builder) {
    auto *elemptr = builder.builder.CreateInBoundsGEP(
        llvm::Type::getInt8Ty(builder.getContext()), ptr, {index});
    return elemptr;
  }
};

template <> struct p2c_col_gen<int64_t> : public GenericColumnTypeGen {
  static ValueRef<> createAccess(ValueRef<> index, ValueRef<> ptr, Builder &builder) {
    auto *elemptr = builder.builder.CreateInBoundsGEP(
        llvm::Type::getInt64Ty(builder.getContext()), ptr, {index});
    return elemptr;
  }
};

template <> struct p2c_col_gen<uint64_t> {
  static TypeRef<> createType(llvm::LLVMContext &context) {
    return llvm::Type::getInt64Ty(context);
  }
};

template <typename... T>
TypeRef<llvm::StructType> createTable(llvm::LLVMContext &context,
                              llvm::StringRef name) {
  constexpr auto fn = []<typename C>(llvm::LLVMContext &context) {
    return p2c_col_gen<C>::createType(context);
  };
  std::array<TypeRef<>, sizeof...(T) + 1> types {
    fn.template operator()<T>(context)...,
    p2c_col_gen<uint64_t>::createType(context)
  };

  return getOrCreateType(context, name, [&]() {
    return llvm::StructType::create(context, types, name);
  });
};

TypeRef<> createColumnType(llvm::LLVMContext &context, TypeEnum type);

} // namespace p2cllvm