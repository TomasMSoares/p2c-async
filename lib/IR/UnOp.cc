#include "IR/Builder.h"
#include "IR/Unop.h"
#include <stdexcept>

namespace p2cllvm {
ValueRef<> trivial_unop::createUnOp(Builder &builder, UnOp op, ValueRef<> val) {
  switch (op) {
  case UnOp::NOT:
    return builder.builder.CreateNot(val);
  default:
    throw std::runtime_error("Unsupported Unop");
  }
}
} // namespace p2cllvm
