#pragma once

#include "IR/Binop.h"
#include "IR/Builder.h"
#include "IR/Defs.h"
#include "IR/Tuple.h"
#include "IR/Types.h"
#include "operators/OperatorContext.h"
#include "operators/Operator.h"
#include "runtime/Runtime.h"
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <memory>
#include <utility>
#include <vector>
namespace p2cllvm {

template <typename T, typename C> class SortOp : public Operator {
public:
  SortOp(std::unique_ptr<Operator> &&parent, std::vector<IU *> &&ius,
         std::vector<bool> &&cmp)
      : parent(std::move(parent)), ius(std::move(ius)), cmp(std::move(cmp)) {}
  ValueRef<llvm::Function> createCmpFn(Builder &builder) {
    auto &context = builder.getContext();
    BasicBlockRef bb = builder.getInsertBlock();
    ValueRef<llvm::Function> cmpFn = builder.createCmpFunction();
    ValueRef<> t0 = cmpFn->getArg(0);
    ValueRef<> t1 = cmpFn->getArg(1);
    auto t0Vals =
        builder.createUnpackTuple<std::vector<ValueRef<>>>(t, t0, ius);
    auto t1Vals =
        builder.createUnpackTuple<std::vector<ValueRef<>>>(t, t1, ius);
    size_t i = 0;
    llvm::SmallVector<std::tuple<BasicBlockRef, ValueRef<>, ValueRef<>>, 8> bbs;
    bbs.reserve(ius.size() + 1);
    for (auto *iu : ius) {
      ValueRef<> res;
      ValueRef<> larger =
          iu->type.createBinOp(BinOp::CMPGT, t0Vals[i], t1Vals[i], builder);
      ValueRef<> smaller =
          iu->type.createBinOp(BinOp::CMPLT, t0Vals[i], t1Vals[i], builder);
      larger = builder.builder.CreateSExtOrTrunc(larger, builder.getInt32ty());
      smaller =
          builder.builder.CreateSExtOrTrunc(smaller, builder.getInt32ty());
      if (!cmp[i])
        std::swap(larger, smaller);
      res = builder.builder.CreateSub(larger, smaller);
      ValueRef<> brCond = builder.builder.CreateTrunc(res, builder.getInt1ty());
      BasicBlockRef next = builder.createBasicBlock("next", cmpFn);
      bbs.emplace_back(builder.builder.GetInsertBlock(), brCond, res);
      builder.builder.SetInsertPoint(next);
      ++i;
    }
    BasicBlockRef last = builder.builder.GetInsertBlock();
    builder.builder.SetInsertPoint(last);
    ValueRef<llvm::PHINode> phi =
        builder.builder.CreatePHI(cmpFn->getReturnType(), ius.size());
    bbs.emplace_back(last, nullptr, nullptr);
    for (i = 0; i < ius.size() - 1; ++i) {
      auto &[cbb, cval, cres] = bbs[i];
      builder.setInsertPoint(cbb);
      builder.builder.CreateCondBr(cval, last, std::get<0>(bbs[i + 1]));
      phi->addIncoming(cres, cbb);
    }
    auto &[cbb, _, cres] = bbs[ius.size() - 1];
    builder.setInsertPoint(cbb);
    builder.builder.CreateBr(last);
    phi->addIncoming(cres, cbb);
    builder.setInsertPoint(last);
    builder.builder.CreateRet(phi);
    builder.setInsertPoint(bb);
    return cmpFn;
  }
  void produce(IUSet &required, Builder &builder, ConsumerFn consumer,
               InitFn init) override {
    IUSet fromParent = required | IUSet(ius);
    t = Tuple::get(builder.getContext(), fromParent);
    ValueRef<llvm::Function> cmpFn = createCmpFn(builder);
    ValueRef<> tsize = builder.getInt64Constant(t.getSize());
    ValueRef<> ltls;
    auto mInit = [&](Builder &builder) {
      static_cast<T &>(*this).init(builder, ltls);
    };

    auto consumerFn = [&](Builder &builder) {
      ValueRef<> tuple = builder.createCall("insertSortEntry", &insertSortEntry,
                                            builder.getPtrTy(), ltls, tsize);
      builder.createPackTuple(t, tuple, fromParent.v);
    };
    parent->produce(fromParent, builder, consumerFn, mInit);
    builder.finishPipeline();

    builder.createPipeline();
    init(builder);
    static_cast<T&>(*this).continuation(builder, tsize, cmpFn, fromParent, consumer);
  }

  IUSet availableIUs() override { return IUSet(ius) | parent->availableIUs(); }

protected:
  std::unique_ptr<Operator> parent;
  std::vector<IU *> ius;
  std::vector<bool> cmp;
  Tuple t;
  C *sctx;
};

class Sort : public SortOp<Sort, SortContext> {
public:
  Sort(std::unique_ptr<Operator> &&parent, std::vector<IU *> &&ius,
       std::vector<bool> &&cmp)
      : SortOp(std::move(parent), std::move(ius), std::move(cmp)) {}

  void init(Builder &builder, ValueRef<> &ltls) {
    sctx = builder.query.addOperatorContext(std::make_unique<SortContext>());
    ValueRef<> tls = builder.addAndCreatePipelineArg(&sctx->tls);
    ltls = builder.createCall("localSort", &local<ThreadSortContext>,
                              builder.getPtrTy(), tls);
  }

  void continuation(Builder &builder, ValueRef<> tsize,
                    ValueRef<llvm::Function> cmpFn, IUSet &fromParent,
                    ConsumerFn consumer) {
    ValueRef<> tls = builder.addAndCreatePipelineArg(&sctx->tls);
    ValueRef<> sb = builder.addAndCreatePipelineArg(&sctx->sb);
    ValueRef<> combinedSize = builder.createCall(
        "combineSizes", &combineSizes, builder.getInt64ty(), tls, tsize);
    builder.createCall("allocSortBuffer", &allocSortBuffer, builder.getVoidTy(),
                       sb, combinedSize);
    builder.createCall("insertSortBuffer", &insertSortBuffer,
                       builder.getVoidTy(), tls, sb, tsize);
    builder.createCall("sortBuffer", &sortBuffer, builder.getVoidTy(), sb,
                       tsize, cmpFn);
    ValueRef<> buffer =
        builder.createCall("getSorted", &getSorted, builder.getPtrTy(), sb);
    ValueRef<llvm::PHINode> iter =
        builder.createBeginIndexIter(builder.getInt64Constant(0), combinedSize);
    ValueRef<> tuple =
        builder.builder.CreateGEP(builder.getInt8ty(), buffer, iter);
    builder.createUnpackTuple<>(t, tuple, fromParent.v);
    consumer(builder);
    builder.createEndIndexIter(t.getSize());
  }
};
}; // namespace p2cllvm
