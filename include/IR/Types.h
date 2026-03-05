#pragma once

#include <cstdint>
#include <llvm/ADT/ArrayRef.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>

#include "IR/Binop.h"
#include "IR/Defs.h"
#include "IR/Concepts.h"
#include "IR/Unop.h"
#include "internal/basetypes.h"
#include <llvm/IR/Value.h>
#include <llvm/Support/Alignment.h>
#include <llvm/Support/raw_ostream.h>
#include <variant>

namespace p2cllvm {

class Builder;

struct BigIntTy : public signed_type, public trivial_unop {
  using value_type = int64_t;
  using arg_type = int64_t;

  static TypeRef<> createType(llvm::LLVMContext &context);

  static ValueRef<> createCopy(ValueRef<> val, ValueRef<> dest,
                               Builder &builder, ValueRef<> string_heap = nullptr);

  static ValueRef<> createLoad(ValueRef<> ptr, Builder &builder);

  static ValueRef<> createConstant(Builder &builder, value_type value);

  static ValueRef<> createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                               Builder &builder);
};

struct IntegerTy : public signed_type, public trivial_unop {
  using value_type = int32_t;
  using arg_type = int32_t;

  static TypeRef<> createType(llvm::LLVMContext &context);

  static ValueRef<> createCopy(ValueRef<> val, ValueRef<> dest,
                               Builder &builder, ValueRef<> string_heap = nullptr);

  static ValueRef<> createLoad(ValueRef<> ptr, Builder &builder);

  static ValueRef<> createConstant(Builder &builder, value_type value);

  static ValueRef<> createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                               Builder &builder);
};

struct DoubleTy : public floating_point_type, public trivial_unop {
  using value_type = double;
  using arg_type = double;

  static TypeRef<> createType(llvm::LLVMContext &context);

  static ValueRef<> createCopy(ValueRef<> val, ValueRef<> dest,
                               Builder &builder, ValueRef<> string_heap = nullptr);

  static ValueRef<> createLoad(ValueRef<> ptr, Builder &builder);

  static ValueRef<> createConstant(Builder &builder, value_type value);

  static ValueRef<> createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                               Builder &builder);
};

struct CharTy : public signed_type, public trivial_unop {
  using value_type = char;
  using arg_type = char;

  static TypeRef<> createType(llvm::LLVMContext &context);

  static ValueRef<> createCopy(ValueRef<> val, ValueRef<> dest,
                               Builder &builder, ValueRef<> string_heap = nullptr);

  static ValueRef<> createLoad(ValueRef<> ptr, Builder &builder);

  static ValueRef<> createConstant(Builder &builder, value_type value);

  static ValueRef<> createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                               Builder &builder);
};

struct BoolTy : public signed_type, public trivial_unop {
  using value_type = bool;
  using arg_type = bool;

  static TypeRef<> createType(llvm::LLVMContext &context);

  static ValueRef<> createCopy(ValueRef<> val, ValueRef<> dest,
                               Builder &builder, ValueRef<> string_heap = nullptr);

  static ValueRef<> createLoad(ValueRef<> ptr, Builder &builder);

  static ValueRef<> createConstant(Builder &builder, value_type value);

  static ValueRef<> createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                               Builder &builder);
};

struct StringTy : public string_type {
  using value_type = StringView;
  using arg_type = StringView *;

  static TypeRef<> createType(llvm::LLVMContext &context);

  static ValueRef<> createCopy(ValueRef<> src, ValueRef<> dest,
                               Builder &builder, ValueRef<> string_heap = nullptr);

  static ValueRef<> createLoad(ValueRef<> ptr, Builder &builder);

  static ValueRef<> createConstant(Builder &builder, value_type value);

  static ValueRef<> createCMPEQ(ValueRef<> lhs, ValueRef<> rhs,
                                Builder &builder);

  static ValueRef<> createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                               Builder &builder);
  static ValueRef<> createUnOp(Builder &builder, UnOp op, ValueRef<> val);
};

struct DateTy : public unsigned_type {
  using value_type = uint32_t;
  using arg_type = uint32_t;

  static TypeRef<> createType(llvm::LLVMContext &context);

  static ValueRef<> createCopy(ValueRef<> val, ValueRef<> dest,
                               Builder &builder, ValueRef<> string_heap = nullptr);

  static ValueRef<> createLoad(ValueRef<> ptr, Builder &builder);

  static ValueRef<> createConstant(Builder &builder, value_type value);

  static ValueRef<> createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                               Builder &builder);
  static ValueRef<> createUnOp(Builder &builder, UnOp op, ValueRef<> val);
};

struct PointerTy {
  using value_type = void *;

  static TypeRef<llvm::PointerType> createType(llvm::LLVMContext &context);
};

struct VoidTy {
  using value_type = void;

  static TypeRef<> createType(llvm::LLVMContext &context);
};

struct Type {
  const TypeEnum typeEnum;
  Mixin<std::variant, BigIntTy, IntegerTy, DoubleTy, CharTy, BoolTy, StringTy,
        DateTy>
      type;

  Type(TypeEnum typeEnum, auto &&type)
      : typeEnum(typeEnum), type(std::move(type)) {}

  static Type get(TypeEnum typeEnum);

  TypeRef<> createType(llvm::LLVMContext &context) {
    return std::visit<TypeRef<>>(
        [&](auto arg) { return arg.createType(context); }, type);
  }

  ValueRef<> createCopy(ValueRef<> val, ValueRef<> dest, Builder &builder) {
    // Delegates to the new 4-argument version with nullptr for heap
    return createCopy(val, dest, builder, nullptr);
  }

  // new 4-argument createCopy overload (called by Builder::createCopy)
  // for deep copying of strings
  ValueRef<> createCopy(ValueRef<> val, ValueRef<> dest, Builder &builder, 
                        ValueRef<> string_heap) {
    return std::visit<ValueRef<>>(
        [&](auto arg) { return arg.createCopy(val, dest, builder, string_heap); }, type);
  }

  ValueRef<> createLoad(ValueRef<> ptr, Builder &builder) {
    return std::visit<ValueRef<>>(
        [&](auto arg) { return arg.createLoad(ptr, builder); }, type);
  }

  ValueRef<> createCast(ValueRef<> val, TypeEnum other, TypeRef<> target,
                        Builder &builder) {
    return std::visit<ValueRef<>>(
        [&](auto arg) { return arg.createCast(val, other, target, builder); },
        type);
  }

  ValueRef<> createBinOp(BinOp op, ValueRef<> lhs, ValueRef<> rhs,
                         Builder &builder) {
    return std::visit<ValueRef<>>(
        [&](auto arg) { return arg.createBinOp(builder, op, lhs, rhs); }, type);
  }

  ValueRef<> createUnOp(UnOp op, ValueRef<> val, Builder &builder) {
    return std::visit<ValueRef<>>(
        [&](auto arg) { return arg.createUnOp(builder, op, val); }, type);
  }
};

template <typename T> struct p2c_type_mixin;

template <> struct p2c_type_mixin<int32_t> {
  using type = IntegerTy;
  static constexpr TypeEnum type_enum = TypeEnum::Integer;
};

template <> struct p2c_type_mixin<int64_t> {
  using type = BigIntTy;
  static constexpr TypeEnum type_enum = TypeEnum::BigInt;
};

template <> struct p2c_type_mixin<bool> {
  using type = BoolTy;
  static constexpr TypeEnum type_enum = TypeEnum::Bool;
};

template <> struct p2c_type_mixin<double> {
  using type = DoubleTy;
  static constexpr TypeEnum type_enum = TypeEnum::Double;
};

template <> struct p2c_type_mixin<char> {
  using type = CharTy;
  static constexpr TypeEnum type_enum = TypeEnum::Char;
};

template <> struct p2c_type_mixin<StringView> {
  using type = StringTy;
  static constexpr TypeEnum type_enum = TypeEnum::String;
};

template <> struct p2c_type_mixin<Date> {
  using type = DateTy;
  static constexpr TypeEnum type_enum = TypeEnum::Date;
};

}; // namespace p2cllvm
