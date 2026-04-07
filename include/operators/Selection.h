#pragma once
#include "IR/Builder.h"
#include "IR/Expression.h"
#include "Operator.h"
#include <llvm/IR/BasicBlock.h>

namespace p2cllvm {
class Selection : public Operator {
public:
  Selection(std::unique_ptr<Operator> &&parent,
            std::unique_ptr<Exp> &&predicate)
      : parent(std::move(parent)), predicate(std::move(predicate)) {}
  void produce(IUSet &required, Builder &builder, ConsumerFn consumer, InitFn fn) override {
      IUSet iuset = required | predicate->getIUs();
    parent->produce(iuset, builder, [&](Builder &builder) {      
      ValueRef<> res = builder.createExpEval(predicate);
      BasicBlockRef cnd = builder.builder.GetInsertBlock();
      BasicBlockRef body = builder.createBasicBlock("body");
      builder.builder.SetInsertPoint(body);
      consumer(builder);
      BasicBlockRef cnt = builder.createBasicBlock("cnt");
      builder.builder.CreateBr(cnt);
      builder.builder.SetInsertPoint(cnd);
      builder.builder.CreateCondBr(res, body, cnt);
      builder.builder.SetInsertPoint(cnt);
    }, fn);
  }

  IUSet availableIUs() override { return parent->availableIUs(); }

private:
  std::unique_ptr<Operator> parent;
  std::unique_ptr<Exp> predicate;
};
} // namespace p2cllvm
