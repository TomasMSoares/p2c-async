#pragma once
#include "IR/Builder.h"
#include "IR/Defs.h"
#include "IR/Expression.h"
#include "IR/Hash.h"
#include "IR/Tuple.h"
#include "IR/Types.h"
#include "Operator.h"
#include "OperatorContext.h"
#include "runtime/Runtime.h"
#include "runtime/ThreadLocalContext.h"
#include "runtime/TypeInfo.h"
#include "runtime/hashtables.h"
#include "runtime/tuplebuffer.h"
#include <cstddef>
#include <functional>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Type.h>
#include <memory>

namespace p2cllvm {

class InnerJoin : public Operator {
public:
  void produce(IUSet &required, Builder &builder, ConsumerFn fn,
               InitFn iit) override {
        Tuple elem;
    JoinContext *ijContext;
    ValueRef<> tls, ltls, ht;  
    IUSet leftRequiredIUs =
        (required & left->availableIUs()) | IUSet(leftKeyIUs) |
        (condition ? left->availableIUs() & condition->getIUs() : IUSet());
    IUSet rightRequiredIUs =
        (required & right->availableIUs()) | IUSet(rightKeyIUs) |
        (condition ? right->availableIUs() & condition->getIUs() : IUSet());
    IUSet leftPayloadIUs = leftRequiredIUs - IUSet(leftKeyIUs);
    elem = Tuple::get(builder.getContext(), leftRequiredIUs);
    size_t allocSize = calculateElemSize<>(elem);
    auto initLeft = [&](Builder &builder) {
      ijContext =
          builder.query.addOperatorContext(std::make_unique<JoinContext>());
      tls = builder.addAndCreatePipelineArg(&ijContext->tls);
      ltls = builder.createCall("localJoin", local<ThreadJoinContext>,
                                builder.getPtrTy(), tls);
    };

    auto initRight = [&](Builder &builder) {
      iit(builder);
      ht = builder.addAndCreatePipelineArg(&ijContext->ht);
    };
    left->produce(
        leftRequiredIUs, builder,
        [&](Builder &builder) {
          // use the thread-local string heap from ThreadJoinContext (ltls)
          ValueRef<> string_heap = builder.createCall(
              "getLocalStringHeap", &getLocalStringHeap<ThreadJoinContext>, builder.getPtrTy(), ltls);
          
          ValueRef<> hash =
              builder.createHashKeysHasher<MurmurHasher>(leftKeyIUs);
          ValueRef<> entry = builder.createCall(
              "insertJoinEntry", &insertJoinEntry, builder.getPtrTy(), ltls,
              hash, builder.getInt64Constant(allocSize));
          builder.createStoreHash<>(entry, hash);
          ValueRef<> tuple = builder.createLoadData<>(entry);

          builder.createPackTuple(elem, tuple, leftRequiredIUs.v, string_heap);
        },
        initLeft);
    builder.finishPipeline();

    builder.createPipeline();
    ht = builder.addAndCreatePipelineArg(&ijContext->ht);
    tls = builder.addAndCreatePipelineArg(&ijContext->tls);
    ValueRef<> htSize = builder.createCall("estimateCombinedJoinSize",
                                           &combineSketches<ThreadJoinContext>,
                                           builder.getInt64ty(), tls);
    ht = builder.createHashTableAlloc(ht, htSize);
    builder.finishPipeline();

    /// Pipeline to insert materialized tuples into hashtables
    builder.createContinuationPipeline();
    ht = builder.addAndCreatePipelineArg(&ijContext->ht);
    tls = builder.addAndCreatePipelineArg(&ijContext->tls);

    // Loop over all thread contexts to merge their buffers.
    // Get the number of active thread contexts
    ValueRef<> threads = builder.createCall("getNumThreadContext",
                                          &getNumThreadContext<ThreadJoinContext>,
                                          builder.getInt64ty(), tls);

    ValueRef<llvm::PHINode> iter = builder.createBeginIndexIter(builder.getInt64Constant(0), threads);

    // Get the context at index `iter`
    ValueRef<> mine = builder.createCall("getJoinContext", 
                                          &getContext<ThreadJoinContext>,
                                          builder.getPtrTy(), tls, iter);

    // Merge this thread's data into the global hash table
    builder.createCall("insertThreaded", &insertMultithreaded,
                       builder.getVoidTy(), mine, ht,
                       builder.getInt64Constant(allocSize));

    builder.createEndIndexIter();
    builder.finishPipeline();

    right->produce(
        rightRequiredIUs, builder,
        [&](Builder &builder) {
          auto &scope = builder.getCurrentScope();
          ValueRef<> hash =
              builder.createHashKeysHasher<MurmurHasher>(rightKeyIUs);
          ValueRef<> bucket = builder.createHashTableLookUp(ht, hash);
          ValueRef<> ptr = builder.createBeginForwardIter(bucket);
          ValueRef<> taggedPtr = ptr;
          ptr = builder.createGetPtr<>(ptr);
          BasicBlockRef cbb = builder.createCmpTag<>(taggedPtr, hash);
          BasicBlockRef fbb = builder.getInsertBlock();
          ValueRef<> tuple = builder.createLoadData<>(ptr);
          builder.createUnpackTuple<>(elem, tuple, leftKeyIUs);
          llvm::SmallVector<ValueRef<>, 8> leftV, rightV;
          for (auto *iu : leftKeyIUs) {
            leftV.push_back(scope.lookupValue(iu));
          }
          for (auto *iu : rightKeyIUs) {
            rightV.push_back(scope.lookupValue(iu));
          }
          auto branches = builder.createCmpKeys(leftV, rightV, leftKeyIUs);
          builder.createUnpackTuple(elem, tuple, leftPayloadIUs.v);
          if (condition) {
            ValueRef<> val = builder.createExpEval(condition);
            BasicBlockRef trBB = builder.createBasicBlock("trueJoinCond");
            BasicBlockRef cnt = builder.createBasicBlock("cntJoinCond");
            builder.createBranch(val, trBB, cnt);
            builder.setInsertPoint(trBB);
            fn(builder);
            builder.createBranch(cnt);
            builder.setInsertPoint(cnt);
          } else {
            fn(builder);
          }
          BasicBlockRef bb = builder.createEndCmpKeys(branches);
          builder.createBranch(bb);
          builder.setInsertPoint(bb);
          builder.createEndForwardIter<HashTableEntry, true>();
          builder.createMissingBranch(cbb, builder.getInsertBlock(), fbb);
        },
        initRight);
  }

  IUSet availableIUs() override {
    return left->availableIUs() | right->availableIUs();
  }

  InnerJoin(std::unique_ptr<Operator> &&left, std::unique_ptr<Operator> &&right,
            std::vector<IU *> &&leftKeyIUs, std::vector<IU *> &&rightKeyIUs,
            std::unique_ptr<Exp> &&condition)
      : left(std::move(left)), right(std::move(right)),
        condition(std::move(condition)), leftKeyIUs(std::move(leftKeyIUs)),
        rightKeyIUs(std::move(rightKeyIUs)) {}

private:
  std::unique_ptr<Operator> left;
  std::unique_ptr<Operator> right;
  std::unique_ptr<Exp> condition;
  std::vector<IU *> leftKeyIUs, rightKeyIUs;
};
} // namespace p2cllvm