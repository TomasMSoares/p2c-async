#pragma once
#include "internal/basetypes.h"
#include "Defs.h"

#include <concepts>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/Support/ErrorHandling.h>
#include <type_traits>
namespace p2cllvm {
class Builder;

template <typename T>
concept p2c_llvm_struct_type = requires(T t) {
  {
    std::decay_t<T>::createType(std::declval<llvm::LLVMContext &>())
  } -> std::same_as<llvm::StructType *>;
};
template <typename T>
concept TypeTy = requires(T t, TypeEnum type) {
  {
    T::createType(std::declval<llvm::LLVMContext &>())
  } -> std::same_as<TypeRef<>>;
  {
    T::createCopy(std::declval<ValueRef<>>(), std::declval<ValueRef<>>(),
                  std::declval<Builder &>())
  } -> std::same_as<ValueRef<>>;
  {
    T::createLoad(std::declval<ValueRef<>>(), std::declval<Builder &>())
  } -> std::same_as<ValueRef<>>;
  {
    T::createConstant(std::declval<Builder &>(),
                      std::declval<typename T::value_type>())
  } -> std::same_as<ValueRef<>>;
  {
    T::createCast(std::declval<ValueRef<>>(), type,
                  std::declval<TypeRef<>>(), std::declval<Builder &>())
  } -> std::same_as<ValueRef<>>;
};

template <template <typename> typename C, typename... T>
using PtrMixin = C<std::unique_ptr<T>...>;

template <template <typename> typename C, TypeTy... T> using Mixin = C<T...>;

template <TypeEnum type> struct TypeInfo {
  static constexpr bool isIntegerType = true;
  static constexpr bool isFloatType = false;
};

template <> struct TypeInfo<TypeEnum::Double> {
  static constexpr bool isIntegerType = false;
  static constexpr bool isFloatType = true;
};

template <> struct TypeInfo<TypeEnum::String> {
  static constexpr bool isIntegerType = false;
  static constexpr bool isFloatType = false;
};
}; // namespace p2cllvm
