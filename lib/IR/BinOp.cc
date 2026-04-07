#include "IR/Binop.h"

#include "IR/Builder.h"
#include "IR/Types.h"
#include "runtime/Runtime.h"

namespace p2cllvm {
ValueRef<> signed_type::createBinOp(Builder &builder, BinOp op, ValueRef<> lhs,
                                     ValueRef<> rhs) {
  switch (op) {
  case BinOp::Add:
    return builder.builder.CreateAdd(lhs, rhs);
  case BinOp::Sub:
    return builder.builder.CreateSub(lhs, rhs);
  case BinOp::Mul:
    return builder.builder.CreateMul(lhs, rhs);
  case BinOp::Div:
    return builder.builder.CreateSDiv(lhs, rhs);
  case BinOp::Or:
    return builder.builder.CreateOr(lhs, rhs);
  case BinOp::And:
    return builder.builder.CreateAnd(lhs, rhs);
  case BinOp::CMPEQ:
    return builder.builder.CreateICmpEQ(lhs, rhs);
  case BinOp::CMPGE:
    return builder.builder.CreateICmpSGE(lhs, rhs);
  case BinOp::CMPGT:
    return builder.builder.CreateICmpSGT(lhs, rhs);
  case BinOp::CMPLE:
    return builder.builder.CreateICmpSLE(lhs, rhs);
  case BinOp::CMPLT:
    return builder.builder.CreateICmpSLT(lhs, rhs);
  case BinOp::CMPNE:
    return builder.builder.CreateICmpNE(lhs, rhs);
  }
  return nullptr;
}
ValueRef<> unsigned_type::createBinOp(Builder &builder, BinOp op,
                                      ValueRef<> lhs, ValueRef<> rhs) {
  switch (op) {
  case BinOp::Add:
    return builder.builder.CreateAdd(lhs, rhs);
  case BinOp::Sub:
    return builder.builder.CreateSub(lhs, rhs);
  case BinOp::Mul:
    return builder.builder.CreateMul(lhs, rhs);
  case BinOp::Div:
    return builder.builder.CreateUDiv(lhs, rhs);
  case BinOp::Or:
    return builder.builder.CreateOr(lhs, rhs);
  case BinOp::And:
    return builder.builder.CreateAnd(lhs, rhs);
  case BinOp::CMPEQ:
    return builder.builder.CreateICmpEQ(lhs, rhs);
  case BinOp::CMPGE:
    return builder.builder.CreateICmpUGE(lhs, rhs);
  case BinOp::CMPGT:
    return builder.builder.CreateICmpUGT(lhs, rhs);
  case BinOp::CMPLE:
    return builder.builder.CreateICmpULE(lhs, rhs);
  case BinOp::CMPLT:
    return builder.builder.CreateICmpULT(lhs, rhs);
  case BinOp::CMPNE:
    return builder.builder.CreateICmpNE(lhs, rhs);
  }
  return nullptr;
}
ValueRef<> floating_point_type::createBinOp(Builder &builder, BinOp op,
                                            ValueRef<> lhs, ValueRef<> rhs) {
  switch (op) {
  case BinOp::Add:
    return builder.builder.CreateFAdd(lhs, rhs);
  case BinOp::Sub:
    return builder.builder.CreateFSub(lhs, rhs);
  case BinOp::Mul:
    return builder.builder.CreateFMul(lhs, rhs);
  case BinOp::Div:
    return builder.builder.CreateFDiv(lhs, rhs);
  case BinOp::CMPEQ:
    return builder.builder.CreateFCmpOEQ(lhs, rhs);
  case BinOp::CMPGE:
    return builder.builder.CreateFCmpOGE(lhs, rhs);
  case BinOp::CMPGT:
    return builder.builder.CreateFCmpOGT(lhs, rhs);
  case BinOp::CMPLE:
    return builder.builder.CreateFCmpOLE(lhs, rhs);
  case BinOp::CMPLT:
    return builder.builder.CreateFCmpOLT(lhs, rhs);
  case BinOp::CMPNE:
    return builder.builder.CreateFCmpONE(lhs, rhs);
  default:
    throw std::runtime_error("Unsupported Operation");
  }
}
ValueRef<> string_type::createCMPEQ(ValueRef<> lhs, ValueRef<> rhs,
                                    Builder &builder) {

  auto *module = builder.query.getModule();
  auto fn = builder.query.symbolManager.getOrInsertFunction(
      module, "string_eq", string_eq, BoolTy::createType(builder.getContext()),
      PointerTy::createType(builder.getContext()),
      PointerTy::createType(builder.getContext()));
  return builder.builder.CreateCall(fn, {lhs, rhs});
}

ValueRef<> string_type::createBinOp(Builder &builder, BinOp op, ValueRef<> lhs,
                                    ValueRef<> rhs) {
  switch (op) {
  case BinOp::CMPEQ:
    return builder.createCall("string_eq", &string_eq,
                              BoolTy::createType(builder.getContext()), lhs,
                              rhs);
  case BinOp::CMPLT:
    return builder.createCall("string_lt", &string_lt,
                              BoolTy::createType(builder.getContext()), lhs,
                              rhs);
  case BinOp::CMPGT:
    return builder.createCall("string_gt", &string_gt,
                              BoolTy::createType(builder.getContext()), lhs,
                              rhs);
  default:
    throw std::runtime_error("Unsupported Operation");
  }
}

} // namespace p2cllvm
