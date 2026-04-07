// Aggregation.h
#pragma once
#include "IR/Binop.h"
#include "IR/Builder.h"
#include "IR/Defs.h"
#include "IR/Hash.h"
#include "IR/Tuple.h"
#include "IR/Types.h"
#include "internal/basetypes.h"
#include "operators/OperatorContext.h"
#include "operators/Operator.h"
#include "runtime/Runtime.h"
#include "runtime/ThreadLocalContext.h"
#include "runtime/TypeInfo.h"
#include <cassert>
#include <llvm/ADT/SmallVector.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/InstrTypes.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <memory>
#include <string_view>
namespace p2cllvm {
struct Aggregate {
  IU *input;
  IU result;

  Aggregate(std::string_view name, IU *input, TypeEnum type)
      : input(input), result(name, type) {}
  virtual ~Aggregate() = default;
  virtual void init(Builder &builder) = 0;
  virtual void createAggregation(Builder &builder) = 0;
  virtual void createReduceAggregation(Builder &builder,
                                       ValueRef<> other) = 0;
  IU *getResult() { return &result; }
};

struct CountAggregate : public Aggregate {
  CountAggregate(std::string_view name, IU *input = nullptr)
      : Aggregate(name, input, TypeEnum::BigInt) {}
  void init(Builder &builder) override {
    builder.getCurrentScope().updateValue(
        &result, llvm::ConstantInt::get(
                     result.type.createType(builder.getContext()), 1));
  }

  void createAggregation(Builder &builder) override {
    ValueRef<> accVal = result.type.createBinOp(BinOp::Add,
        builder.getCurrentScope().lookupValue(&result),
        llvm::ConstantInt::get(result.type.createType(builder.getContext()),
                               1), builder);
    builder.builder.CreateStore(accVal,
                                builder.getCurrentScope().lookupPtr(&result));
  }

  void createReduceAggregation(Builder &builder, ValueRef<> other) override {
    ValueRef<> accVal = result.type.createBinOp(BinOp::Add,
        builder.getCurrentScope().lookupValue(&result), other, builder);
    builder.builder.CreateStore(accVal,
                                builder.getCurrentScope().lookupPtr(&result));
  }
};

struct SumAggregate : public Aggregate {
  SumAggregate(std::string_view name, IU *input)
      : Aggregate(name, input, input->type.typeEnum) {}

  void init(Builder &builder) override {
    auto &scope = builder.getCurrentScope();
    scope.updateValue(&result, scope.lookupValue(input));
  }

  void createAggregation(Builder &builder) override {
    auto &scope = builder.getCurrentScope();
    ValueRef<> lhs = scope.lookupValue(&result);
    ValueRef<> rhs = scope.lookupValue(input);
    if (result.type.typeEnum != input->type.typeEnum)
      rhs = input->type.createCast(rhs, result.type.typeEnum, lhs->getType(),
                                   builder);
    ValueRef<> accVal = result.type.createBinOp(BinOp::Add, lhs, rhs, builder);
    scope.updateValue(&result, accVal);
    builder.builder.CreateStore(accVal, scope.lookupPtr(&result));
  }

  void createReduceAggregation(Builder &builder, ValueRef<> other) override {
    auto &scope = builder.getCurrentScope();
    ValueRef<> lhs = scope.lookupValue(&result);
    ValueRef<> accVal = result.type.createBinOp(BinOp::Add, lhs, other, builder);
    scope.updateValue(&result, accVal);
    builder.builder.CreateStore(accVal, scope.lookupPtr(&result));
  }
};

template <BinOp cmp>
struct CmpAggregate : public Aggregate {
  CmpAggregate(std::string_view name, IU *input)
      : Aggregate(name, input, input->type.typeEnum) {}
  void init(Builder &builder) override {
    auto &scope = builder.getCurrentScope();
    scope.updateValue(&result, scope.lookupValue(input));
  }

  void createAggregation(Builder &builder) override {
    auto &scope = builder.getCurrentScope();
    ValueRef<> lhs = scope.lookupValue(&result);
    ValueRef<> rhs = scope.lookupValue(input);
    ValueRef<> cmpVal = result.type.createBinOp(cmp, lhs, rhs, builder);
    auto *minVal = builder.builder.CreateSelect(cmpVal, lhs, rhs);
    scope.updateValue(&result, minVal);
    builder.builder.CreateStore(minVal, scope.lookupPtr(&result));
  }

  void createReduceAggregation(Builder &builder, ValueRef<> other) override {
    auto &scope = builder.getCurrentScope();
    ValueRef<> lhs = scope.lookupValue(&result);
    ValueRef<> cmpVal = result.type.createBinOp(cmp, lhs, other, builder);
    ValueRef<> minVal = builder.builder.CreateSelect(cmpVal, lhs, other);
    scope.updateValue(&result, minVal);
    builder.builder.CreateStore(minVal, scope.lookupPtr(&result));
  }
};

struct MinAggregate : public CmpAggregate<BinOp::CMPLT> {
  MinAggregate(std::string_view name, IU *input) : CmpAggregate(name, input) {}
};

struct MaxAggregate : public CmpAggregate<BinOp::CMPGT> {
  MaxAggregate(std::string_view name, IU *input) : CmpAggregate(name, input) {}
};

struct AnyAggregate : public Aggregate{
    AnyAggregate(std::string_view name, IU* input): Aggregate(name, input, input->type.typeEnum){}

void init(Builder &builder) override {
    auto &scope = builder.getCurrentScope();
    scope.updateValue(&result, scope.lookupValue(input));
  }

  void createAggregation(Builder &builder) override {
      (void) builder;
  }

  void createReduceAggregation(Builder &builder, ValueRef<> other) override {
      (void)builder;
      (void)other;
  }
};

class Aggregation : public Operator {
public:
  Aggregation(std::unique_ptr<Operator> &&parent, IUSet &&groupByIUs)
      : parent(std::move(parent)), groupByIUs(std::move(groupByIUs)) {};

  void produce(IUSet &required, Builder &builder, ConsumerFn consumer,
               InitFn fn) override {
    Tuple elem;
    AggregationContext *aContext;
    IUSet requiredGb = required | groupByIUs;
    IUSet aggIUs;

    /// inOrder for correct mapping between aggs and IUs
    /// might be reordered otherwise
    std::vector<IU*> inOrder;
    inOrder.reserve(aggs.size());
    for (auto &agg : aggs) {
      aggIUs.add(agg->getResult());
      inOrder.push_back(agg->getResult());
    }
    IUSet resultIUs = groupByIUs | aggIUs;
    elem = Tuple::get(builder.getContext(), required);
    size_t allocSize = calculateElemSize<>(elem);
    ValueRef<> tls = nullptr, tb = nullptr, ltls = nullptr,
              localHt = nullptr;
    auto init = [&](Builder &builder) {
      aContext = builder.query.addOperatorContext(
          std::make_unique<AggregationContext>());
      tls = builder.addAndCreatePipelineArg(&aContext->tls);
      ltls = builder.createCall(
          "localAggregation", &local<ThreadAggregationContext>,
          builder.getPtrTy(), tls);
      tb = builder.createCall("getAggTB", &getLocalTB<ThreadAggregationContext>,
                              builder.getPtrTy(),
                              ltls);
      localHt = builder.createCall(
          "getLocalHashTable", &getLocalHashTable,
          builder.getPtrTy(), ltls);
    };
    auto consumerFn = [&](Builder &builder) {
      assert(tb != nullptr);
      auto &scope = builder.getCurrentScope();
      llvm::SmallVector<ValueRef<> , 8> groupby;
      for (auto *iu : groupByIUs.v) {
        groupby.push_back(scope.lookupValue(iu));
      }
      ValueRef<> hash = builder.createHashKeysHasher<MurmurHasher>(groupByIUs.v);
      ValueRef<> bucket = builder.createHashTableLookUp(localHt, hash);
      ValueRef<> ptr= builder.createBeginForwardIter(bucket);
      ValueRef<> tuple = builder.createLoadData<>(ptr);
      auto valArray = builder.createUnpackTuple<std::vector<ValueRef<> >>(
          elem, tuple, groupByIUs.v);
      auto branches =
          builder.createCmpKeys(valArray, groupby, groupByIUs.v);
      builder.createUnpackTuple<>(elem, tuple, aggIUs.v);
      for (const auto &agg : aggs) {
        agg->createAggregation(builder);
      }
      BasicBlockRef body = builder.getInsertBlock();
      BasicBlockRef gcnt = builder.createEndCmpKeys(branches);
      if (gcnt)
        builder.setInsertPoint(gcnt);
      builder.createEndForwardIter();
      
      ValueRef<> string_heap = builder.createCall(
              "getLocalStringHeap", &getLocalStringHeap<ThreadJoinContext>, builder.getPtrTy(), ltls);

      ValueRef<> entry = builder.createTupleBufferInsert(tb, allocSize);
      tuple = builder.createLoadData<>(entry);
      for (const auto &agg : aggs) {
        agg->init(builder);
      }
      builder.createPackTuple(elem, tuple, resultIUs.v, string_heap); // deep copy
      
      builder.createCall("insertAggEntry", &insertAggEntry,
                         builder.getPtrTy(), ltls,
                         hash, entry);
      BasicBlockRef fbb = builder.createBasicBlock("final");
      builder.builder.CreateBr(fbb);
      builder.createBranch(body, fbb);
      builder.setInsertPoint(fbb);
    };

    IUSet prod = groupByIUs | inputIUs();
    parent->produce(prod, builder, consumerFn, init);
    builder.finishPipeline();

    // create new pipeline
    ValueRef<> ht, tbP;
    llvm::SmallVector<ValueRef<> , 8> groupby;
    builder.createPipeline();
    fn(builder);
    auto &scope = builder.getCurrentScope();
    tls = builder.addAndCreatePipelineArg(&aContext->tls);
    ht = builder.addAndCreatePipelineArg(&aContext->ht);
    tbP = builder.addAndCreatePipelineArg(&aContext->tb8);
    ValueRef<> combinedSize = builder.createCall(
        "estimateCombinedAggSize", &combineSketches<ThreadAggregationContext>,
        builder.getInt64ty(), tls);
    builder.createHashTableAlloc(ht, combinedSize);
    ValueRef<> threads = builder.createCall("getNumThreadContext",
    &getNumThreadContext<ThreadAggregationContext>,
     builder.getInt64ty(), tls); 
    ValueRef<llvm::PHINode> titer =
     builder.createBeginIndexIter(builder.getInt64Constant(0),
     threads);
    ValueRef<> tctx = builder.createCall(
        "getAggContext", &getContext<ThreadAggregationContext>,
        builder.getPtrTy(), tls, titer);
    tb = builder.createCall("getAggTB", &getLocalTB<ThreadAggregationContext>,
                            builder.getPtrTy(),
                            tctx);
    ValueRef<> telem = builder.createBeginTupleBufferIter(tb);
    ValueRef<> tuple = builder.createLoadData<>(telem);
    builder.createUnpackTuple(elem, tuple, groupByIUs.v);
    for (auto *iu : groupByIUs) {
      groupby.push_back(scope.lookupValue(iu));
    }
    ValueRef<> hash = builder.createHashKeysHasher<MurmurHasher>(groupByIUs.v);
    ValueRef<> bucket = builder.createHashTableLookUp(ht, hash);
    ValueRef<> ptr = builder.createBeginForwardIter(bucket);
    ValueRef<> nodeTuple = builder.createLoadData<>(ptr);
    auto keyValArray = builder.createUnpackTuple<std::vector<ValueRef<> >>(
        elem, nodeTuple, groupByIUs.v);
    auto branches =
        builder.createCmpKeys(keyValArray, groupby, groupByIUs.v);
    auto aggValArray = builder.createUnpackTuple<std::vector<ValueRef<> >>(
        elem, tuple, inOrder);
    builder.createUnpackTuple<>(elem, nodeTuple, inOrder);
    size_t i = 0;
    for (const auto &agg : aggs) {
      agg->createReduceAggregation(builder, aggValArray[i++]);
    }
    BasicBlockRef body = builder.builder.GetInsertBlock();
    BasicBlockRef gcnt = builder.createEndCmpKeys(branches);
    builder.setInsertPoint(gcnt);
    builder.createEndForwardIter();

    ValueRef<> tbPelem = builder.createTupleBufferInsert(tbP, sizeof(void *));
    builder.builder.CreateStore(telem, tbPelem);
    builder.createHashTableInsert(ht, telem, hash);
    BasicBlockRef fbb = builder.createBasicBlock("final");
    builder.createBranch(fbb);
    builder.createBranch(body, fbb);
    builder.setInsertPoint(fbb);
    builder.createEndTupleBufferIter(allocSize);
    builder.createEndIndexIter();

    /// iterate over aggregats
    ValueRef<> telemptr = builder.createBeginTupleBufferIter(tbP);
    telem = builder.builder.CreateLoad(
        builder.getPtrTy(), telemptr, false);
    tuple = builder.createLoadData<>(telem);
    builder.createUnpackTuple<>(elem, tuple, resultIUs.v);
    consumer(builder);
    builder.createEndTupleBufferIter(sizeof(void *));
  }

  void addAggregate(std::unique_ptr<Aggregate> &&agg) {
    aggs.push_back(std::move(agg));
  }

  IUSet availableIUs() override {
    IUSet available = groupByIUs;
    for (auto &agg : aggs) {
      available.add(agg->getResult());
    }
    return available;
  }

  IUSet inputIUs() {
    IUSet input;
    for (auto &agg : aggs) {
      if (agg->input)
        input.add(agg->input);
    }
    return input;
  }

  IU *getIU(std::string_view name) {
    for (auto &agg : aggs) {
      if (agg->result.name == name)
        return agg->getResult();
    }

    for (auto *iu : groupByIUs) {
      if (iu->name == name)
        return iu;
    }
    return nullptr;
  }

private:
  std::unique_ptr<Operator> parent;
  IUSet groupByIUs;
  std::vector<std::unique_ptr<Aggregate>> aggs;
};
} // namespace p2cllvm
