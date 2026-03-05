#pragma once

#include "IR/Defs.h"
#include <llvm/IR/Value.h>

namespace p2cllvm {
enum class BinOp : uint8_t {
  Add = 0,
  Sub,
  Mul,
  Div,
  Or,
  And,
  CMPEQ,
  CMPNE,
  CMPLE,
  CMPGE,
  CMPLT,
  CMPGT
};

class Builder;

struct signed_type {
  static ValueRef<> createBinOp(Builder &builder, BinOp op, ValueRef<> lhs,
                                ValueRef<> rhs);
};

struct unsigned_type {
  static ValueRef<> createBinOp(Builder &builder, BinOp op, ValueRef<> lhs,
                                ValueRef<> rhs);
};

struct floating_point_type {
  static ValueRef<> createBinOp(Builder &builder, BinOp op, ValueRef<> lhs,
                                ValueRef<> rhs);
};
struct string_type {
  static ValueRef<> createCMPEQ(ValueRef<> lhs, ValueRef<> rhs,
                                Builder &builder);

  static ValueRef<> createBinOp(Builder &builder, BinOp op, ValueRef<> lhs,
                                ValueRef<> rhs);
};
}
// namespace p2cllvm
