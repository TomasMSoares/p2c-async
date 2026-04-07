#pragma once
#include <algorithm>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Instructions.h>
#include <memory>
#include <span>

#include "IR/Defs.h"
#include "operators/Iu.h"
namespace p2cllvm {
class Builder;
using LoopInfo = std::pair<BasicBlockRef, ValueRef<llvm::PHINode>>;    
struct Scope {
  llvm::Function *pipeline;
  llvm::DenseMap<IU *, ValueRef<>> iuValues;
  llvm::DenseMap<IU *, ValueRef<>> iuPtrs;
  llvm::SmallVector<LoopInfo, 4> loopInfo;

  inline ValueRef<> updatePtr(IU *iu, ValueRef<>ptr) { iuPtrs[iu] = ptr; 
    return ptr;
  }

  inline const LoopInfo& getCurrentLoopInfo() const{
      return loopInfo.back();
  }

  inline void updateValue(IU *iu, ValueRef<>value) { iuValues[iu] = value; }

  inline void clearValues(std::span<IU*> ius){ std::ranges::for_each(ius, [&](const auto& iu){ iuValues[iu] = nullptr; });}

  inline ValueRef<>lookupPtr(IU *iu) {return iuPtrs.lookup(iu);}

  inline ValueRef<>lookupValue(IU *iu){ return iuValues.lookup(iu); }

  static std::unique_ptr<Scope> get(llvm::Function *pipeline) {
    return std::make_unique<Scope>(pipeline);
  }

  llvm::BasicBlock* getCurrentEntryBlock() {
      return &pipeline->getEntryBlock();
  }

  ValueRef<>getOperatorArgs() {
    assert(pipeline->arg_size() > 0);
    return pipeline->getArg(pipeline->arg_size() - 1);
  }

  Scope(llvm::Function *pipeline) : pipeline(pipeline) {}
};
} // namespace p2cllvm
