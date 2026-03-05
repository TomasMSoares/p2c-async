#pragma once

#include <type_traits>
#include <llvm/IR/Value.h>

namespace p2cllvm {
template <typename T = llvm::Value>
  requires(std::derived_from<T, llvm::Value> &&
           !std::is_same_v<llvm::BasicBlock, T>)
using ValueRef = T *;

template<typename T = llvm::Type> requires std::derived_from<T, llvm::Type>
using TypeRef = T*;
using BasicBlockRef = llvm::BasicBlock *;
}
