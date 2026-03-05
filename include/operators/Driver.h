#pragma once
#include "IR/Builder.h"
#include "IR/Types.h"
#include "internal/basetypes.h"
#include "Operator.h"
#include "operators/OperatorContext.h"
#include "runtime/Test.h"
#include <cassert>
#include <cstdint>
#include <llvm/Support/ErrorHandling.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace p2cllvm {
class Sink {
public:
  virtual ~Sink() = default;
  virtual void produce(std::unique_ptr<Operator> &parent,
                       std::span<IU *> required, std::span<std::string> names,
                       Builder &builder) = 0;
};
class PrintTupleSink : public Sink {
public:
  ~PrintTupleSink() override = default;

  void produce(std::unique_ptr<Operator> &parent, std::span<IU *> required,
               std::span<std::string> names, Builder &builder) override {
    IUSet requiredAsSet(std::vector<IU *>{required.begin(), required.end()});
    parent->produce(
        requiredAsSet, builder,
        [&](Builder &builder) {
          builder.createPrints({required.data(), required.size()});
        },
        [&](Builder &builder) {});
    builder.finishPipeline();
  }
};

class AssertTupleSink : public Sink {
public:
  ~AssertTupleSink() override = default;

  AssertTupleSink(
      const std::unordered_map<std::string, std::vector<std::string>> &expected)
      : expected(expected) {}

  void produce(std::unique_ptr<Operator> &parent, std::span<IU *> required,
               std::span<std::string> names, Builder &builder) override {
    IUSet requiredAsSet(std::vector<IU *>(required.begin(), required.end()));
    llvm::SmallVector<ValueRef<> , 8> iuVals;
    ValueRef<> alContext;
    AssertLengthContext *aLengthContext;
    iuVals.reserve(required.size());
    size_t len = expected.begin()->second.size();
    auto init = [&](Builder &builder) {
      size_t i = 0;
      for (auto *iu : required) {
        auto *vecit =
            builder.query.addOperatorContext(std::make_unique<AssertContext>());
        iuVals.push_back(builder.addAndCreatePipelineArg(vecit));
        assert(expected.contains(names[i]));
        vecit->iter = expected[names[i++]];
      }
      aLengthContext = builder.query.addOperatorContext(
          std::make_unique<AssertLengthContext>(
              builder.query.getLineItemCount()));
      alContext = builder.addAndCreatePipelineArg(aLengthContext);
    };
    size_t i = 0;
    auto consume = [&](Builder &builder) {
      BasicBlockRef assertBB = builder.createBasicBlock("assertBB");
      BasicBlockRef cntBB = builder.createBasicBlock("cnt");
      ValueRef<> res = builder.createCall("skipTooLong", &skipTooLong,
                                     builder.getInt1ty(),
                                     alContext);
      builder.builder.CreateCondBr(res, cntBB, assertBB);
      builder.builder.SetInsertPoint(assertBB);
      auto &scope = builder.getCurrentScope();
      for (auto *iu : required) {
        ValueRef<> val = scope.lookupValue(iu);
#define CASE(fun, tEnum)                                                       \
  builder.createCall("compare" + typeNames[static_cast<uint8_t>(tEnum)], fun,  \
                     builder.getVoidTy(), val, iuVals[i++]);

        switch (iu->type.typeEnum) {
        case TypeEnum::BigInt:
          CASE(&compareFromString<BigIntTy>, TypeEnum::BigInt);
          break;
        case TypeEnum::Integer:
          CASE(&compareFromString<IntegerTy>, TypeEnum::Integer);
          break;
        case TypeEnum::Double:
          CASE(&compareFromString<DoubleTy>, TypeEnum::Double);
          break;
        case TypeEnum::Char:
          CASE(&compareFromString<CharTy>, TypeEnum::Char);
          break;
        case TypeEnum::Date:
          CASE(&compareFromString<DoubleTy>, TypeEnum::Date);
          break;
        case TypeEnum::String:
          CASE(&compareFromString<StringTy>, TypeEnum::String);
          break;
        default:
          throw std::runtime_error("Invalid Col Type");
        }
#undef CASE
      }
      builder.builder.CreateBr(cntBB);
      builder.setInsertPoint(cntBB);
    };
    parent->produce(requiredAsSet, builder, consume, init);

    // Do some pruning
    if (!skipTooLong(aLengthContext))
      builder.createCall("checkLen", &compareLength,
                         builder.getVoidTy(),
                         builder.getInt64Constant(len),
                         iuVals.front());
    builder.finishPipeline();
  }

private:
  std::unordered_map<std::string, std::vector<std::string>> expected;
};

void initialize();

void produce(std::unique_ptr<Operator> op, std::vector<IU *> outputs,
             std::vector<std::string> names, std::unique_ptr<Sink> sink);
}; // namespace p2cllvm
