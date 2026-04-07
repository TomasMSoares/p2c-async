#include "IR/Builder.h"
#include "IR/Types.h"
#include "IR/Defs.h"
#include "internal/basetypes.h"
#include "runtime/Runtime.h"

#include <cstdint>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/Twine.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Type.h>
#include <stdexcept>
#include <string>

namespace p2cllvm {

inline ValueRef<> createLoadTemplate(TypeRef<> type, ValueRef<> ptr,
                                     Builder &builder) {
  return builder.builder.CreateLoad(type, ptr);
}

inline ValueRef<> createCopyTemplate(ValueRef<> dest, ValueRef<> val,
                                     Builder &builder) {
  return builder.builder.CreateStore(val, dest);
}

template <TypeEnum type>
static constexpr ValueRef<>
createCastIntegerToType(ValueRef<> val, TypeEnum other, TypeRef<> target,
                        Builder &builder) {
  if constexpr (TypeInfo<type>::isIntegerType) {
    switch (other) {
    case TypeEnum::Double:
      return builder.builder.CreateSIToFP(val, target);
    default:
      return builder.builder.CreateSExtOrTrunc(val, target);
    }
  } else if constexpr (TypeInfo<type>::isFloatType) {
    switch (other) {
    case TypeEnum::Integer:
      return builder.builder.CreateFPToSI(val, target);
    default:
      return val;
    }
  }
}

// Helper for copy of non-heap-reliant data types
template<typename T>
ValueRef<> createSimpleCopy(ValueRef<> val, ValueRef<> dest, Builder &builder, ValueRef<> string_heap) {
    (void)string_heap;
    return builder.builder.CreateStore(val, dest);
}


TypeRef<> BigIntTy::createType(llvm::LLVMContext &context) {
  return llvm::Type::getInt64Ty(context);
}

ValueRef<> BigIntTy::createCopy(ValueRef<> val, ValueRef<> dest,
                                Builder &builder, ValueRef<> string_heap) {
  return createSimpleCopy<BigIntTy>(val, dest, builder, string_heap);
}

ValueRef<> BigIntTy::createLoad(ValueRef<> ptr, Builder &builder) {
  return createLoadTemplate(llvm::Type::getInt64Ty(builder.getContext()), ptr,
                            builder);
}

ValueRef<> BigIntTy::createConstant(Builder &builder, int64_t value) {
  return llvm::ConstantInt::get(llvm::Type::getInt64Ty(builder.getContext()),
                                value, true);
}

ValueRef<> BigIntTy::createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                                Builder &builder) {
  return createCastIntegerToType<TypeEnum::BigInt>(val, other, type, builder);
}

TypeRef<> IntegerTy::createType(llvm::LLVMContext &context) {
  return llvm::Type::getInt32Ty(context);
}

ValueRef<> IntegerTy::createCopy(ValueRef<> val, ValueRef<> dest,
                                 Builder &builder, ValueRef<> string_heap) {
  return createSimpleCopy<IntegerTy>(val, dest, builder, string_heap);
}

ValueRef<> IntegerTy::createLoad(ValueRef<> ptr, Builder &builder) {
  return createLoadTemplate(llvm::Type::getInt32Ty(builder.getContext()), ptr,
                            builder);
}

ValueRef<> IntegerTy::createConstant(Builder &builder, int32_t value) {
  return llvm::ConstantInt::get(llvm::Type::getInt32Ty(builder.getContext()),
                                value, true);
}

ValueRef<> IntegerTy::createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                                 Builder &builder) {
  return createCastIntegerToType<TypeEnum::Integer>(val, other, type, builder);
}

TypeRef<> DoubleTy::createType(llvm::LLVMContext &context) {
  return llvm::Type::getDoubleTy(context);
}

ValueRef<> DoubleTy::createCopy(ValueRef<> val, ValueRef<> dest,
                                Builder &builder, ValueRef<> string_heap) {
  return createSimpleCopy<DoubleTy>(val, dest, builder, string_heap);
}

ValueRef<> DoubleTy::createLoad(ValueRef<> ptr, Builder &builder) {
  return createLoadTemplate(llvm::Type::getDoubleTy(builder.getContext()), ptr,
                            builder);
}

ValueRef<> DoubleTy::createConstant(Builder &builder, double value) {
  return llvm::ConstantFP::get(llvm::Type::getDoubleTy(builder.getContext()),
                               value);
}

ValueRef<> DoubleTy::createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                                Builder &builder) {
  return createCastIntegerToType<TypeEnum::Double>(val, other, type, builder);
}

TypeRef<> CharTy::createType(llvm::LLVMContext &context) {
  return llvm::Type::getInt8Ty(context);
}

ValueRef<> CharTy::createCopy(ValueRef<> val, ValueRef<> dest,
                              Builder &builder, ValueRef<> string_heap) {
  return createSimpleCopy<CharTy>(val, dest, builder, string_heap);
}

ValueRef<> CharTy::createLoad(ValueRef<> ptr, Builder &builder) {
  return createLoadTemplate(llvm::Type::getInt8Ty(builder.getContext()), ptr,
                            builder);
}

ValueRef<> CharTy::createConstant(Builder &builder, char value) {
  return llvm::ConstantInt::get(llvm::Type::getInt8Ty(builder.getContext()),
                               value);
}

ValueRef<> CharTy::createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                              Builder &builder) {
  return createCastIntegerToType<TypeEnum::Char>(val, other, type, builder);
}

TypeRef<> BoolTy::createType(llvm::LLVMContext &context) {
  return llvm::Type::getInt1Ty(context);
}

ValueRef<> BoolTy::createCopy(ValueRef<> val, ValueRef<> dest,
                              Builder &builder, ValueRef<> string_heap) {
  return createSimpleCopy<BoolTy>(val, dest, builder, string_heap);
}

ValueRef<> BoolTy::createLoad(ValueRef<> ptr, Builder &builder) {
  return createLoadTemplate(llvm::Type::getInt1Ty(builder.getContext()), ptr,
                            builder);
}

ValueRef<> BoolTy::createConstant(Builder &builder, bool value) {
  return llvm::ConstantInt::get(llvm::Type::getInt1Ty(builder.getContext()),
                                value, true);
}

ValueRef<> BoolTy::createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                              Builder &builder) {
  return createCastIntegerToType<TypeEnum::Bool>(val, other, type, builder);
}

TypeRef<> StringTy::createType(llvm::LLVMContext &context) {
  return StringView::createType(context);
}

ValueRef<> StringTy::createCopy(ValueRef<> src, ValueRef<> dest, Builder &builder,
                                ValueRef<> string_heap /*=nullptr*/) {
  // Check if we have a string heap available 
  if (string_heap != nullptr) {
    // Deep copy: allocate and copy actual string data
    auto *sv_type = StringView::createType(builder.getContext());
    auto *data_ptr_addr = builder.builder.CreateStructGEP(sv_type, src, 0);
    auto *length_addr = builder.builder.CreateStructGEP(sv_type, src, 1);
    auto *orig_data = builder.builder.CreateLoad(builder.builder.getPtrTy(), data_ptr_addr);
    auto *length = builder.builder.CreateLoad(builder.builder.getInt64Ty(), length_addr);
    
    // Allocate space in string heap
    auto *module = builder.query.getModule();
    auto heap_fn = builder.query.symbolManager.getOrInsertFunction(
        module, "allocStringHeap", &allocStringHeap,
        llvm::FunctionType::get(builder.builder.getPtrTy(),
                                {builder.builder.getPtrTy(),
                                 builder.builder.getInt64Ty()},
                                false));  
    
    auto *heap_mem = builder.builder.CreateCall(heap_fn, {string_heap, length});
    builder.builder.CreateMemCpy(heap_mem, llvm::MaybeAlign(1), orig_data,
                                 llvm::MaybeAlign(1), length);
    
    // Store new pointer and length in destination
    auto *dest_data = builder.builder.CreateStructGEP(sv_type, dest, 0);
    auto *dest_len = builder.builder.CreateStructGEP(sv_type, dest, 1);
    builder.builder.CreateStore(heap_mem, dest_data);
    builder.builder.CreateStore(length, dest_len);
  } else {
    // Shallow copy: just copy the StringView structure (for non-deep-copy operators)
    builder.builder.CreateMemCpy(dest, llvm::MaybeAlign(alignof(StringView)), src,
                                 llvm::MaybeAlign(alignof(StringView)),
                                 sizeof(StringView));
  }
  
  return dest;
}

ValueRef<> StringTy::createLoad(ValueRef<> ptr, Builder &builder) {
  return ptr;
}

ValueRef<> StringTy::createConstant(Builder &builder, StringView value) {
  static size_t off = 0;
  auto &m = *builder.query.getModule();
  auto *str = m.getOrInsertGlobal(
      "str" + std::to_string(off++),
      llvm::ArrayType::get(llvm::Type::getInt8Ty(m.getContext()),
                           value.length + 1));
  auto *strptr = llvm::cast<llvm::GlobalVariable>(str);
  strptr->setLinkage(llvm::GlobalValue::InternalLinkage);
  strptr->setConstant(true);
  strptr->setInitializer(llvm::ConstantDataArray::getString(
      m.getContext(), llvm::StringRef(value.data, value.length), true));
  return builder.builder.CreateConstInBoundsGEP2_64(
      llvm::Type::getInt8Ty(m.getContext()), strptr, 0, 0);
}

ValueRef<> StringTy::createCMPEQ(ValueRef<> lhs, ValueRef<> rhs,
                                 Builder &builder) {
  auto *module = builder.query.getModule();
  auto fn = builder.query.symbolManager.getOrInsertFunction(
      module, "string_eq", string_eq,
      BoolTy::createType(builder.getContext()),
      PointerTy::createType(builder.getContext()),
      PointerTy::createType(builder.getContext()));
  return builder.builder.CreateCall(fn, {lhs, rhs});
}

ValueRef<> StringTy::createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                                Builder &builder) {
  return val;
}

llvm::Value *StringTy::createUnOp(Builder &builder, UnOp op, ValueRef<> val) {
  (void)builder;
  (void)op;
  return val;
}

static llvm::Value *callExtractYear(Builder &builder, llvm::Value *val) {
  return builder.createCall("extractYear", &Date::extractYear,
                            llvm::Type::getInt32Ty(builder.getContext()), val);
}

TypeRef<> DateTy::createType(llvm::LLVMContext &context) {
  return llvm::Type::getInt32Ty(context);
}

ValueRef<> DateTy::createCopy(ValueRef<> val, ValueRef<> dest,
                              Builder &builder, ValueRef<> string_heap) {
  return createSimpleCopy<DateTy>(val, dest, builder, string_heap);
}

ValueRef<> DateTy::createLoad(ValueRef<> ptr, Builder &builder) {
  return createLoadTemplate(DateTy::createType(builder.getContext()), ptr,
                            builder);
}

ValueRef<> DateTy::createConstant(Builder &builder, Date::date value) {
  return llvm::ConstantInt::get(DateTy::createType(builder.getContext()),
                                value, true);
}

ValueRef<> DateTy::createCast(ValueRef<> val, TypeEnum other, TypeRef<> type,
                              Builder &builder) {
  return createCastIntegerToType<TypeEnum::Date>(val, other, type, builder);
}

llvm::Value *DateTy::createUnOp(Builder &builder, UnOp op, ValueRef<> val) {
  switch (op) {
  case UnOp::EXTRACT_YEAR:
    return callExtractYear(builder, val);
  default:
    throw std::runtime_error("Unsupported UnOp");
  }
}

TypeRef<llvm::PointerType> PointerTy::createType(llvm::LLVMContext &context){
    return llvm::PointerType::get(context, 0);
}

TypeRef<> VoidTy::createType(llvm::LLVMContext &context){
    return llvm::Type::getVoidTy(context);
}

Type Type::get(TypeEnum typeEnum) {
  switch (typeEnum) {
  case TypeEnum::Integer:
    return Type(typeEnum, IntegerTy());
  case TypeEnum::Double:
    return Type(typeEnum, DoubleTy());
  case TypeEnum::Char:
    return Type(typeEnum, CharTy());
  case TypeEnum::String:
    return Type(typeEnum, StringTy());
  case TypeEnum::BigInt:
    return Type(typeEnum, BigIntTy());
  case TypeEnum::Bool:
    return Type(typeEnum, BoolTy());
  case TypeEnum::Date:
    return Type(typeEnum, DateTy());
  default:
    throw std::runtime_error("Unsupported type");
  }
}
}; // namespace p2cllvm