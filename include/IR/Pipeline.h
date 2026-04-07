#pragma once
#include "IR/Defs.h"
#include "operators/OperatorContext.h"
#include <cassert>
#include <concepts>
#include <cstdint>
#include <llvm/ADT/SmallVector.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <memory>
#include <string_view>
#include <utility>

#include "SymbolManager.h"
enum class PipelineType { Scan, Default, Continuation};

namespace p2cllvm {
class Database; // Changed from TPCH, forward declared
struct Pipeline {
  PipelineType type;
  std::string name;
  llvm::SmallVector<void *, 8> args;
  Pipeline(PipelineType ptype, ValueRef<llvm::Function> pipeline, std::string_view name)
      : type(ptype), name(name) {}

  size_t addPipelineArg(void *arg) {
    size_t idx = args.size();
    args.push_back(arg);
    return idx;
  }

   virtual ~Pipeline() = default;
};

struct ScanPipeline : public Pipeline {
    ScanPipeline(ValueRef<llvm::Function> pipeline, 
                 std::string_view name, size_t tableIndex)
        : Pipeline(PipelineType::Scan, pipeline, name), tableIndex(tableIndex) {}
    size_t tableIndex;
    ~ScanPipeline() override = default;
};

struct Query {

  llvm::SmallVector<std::unique_ptr<Pipeline>, 8> pipelines;
  llvm::SmallVector<std::unique_ptr<OperatorContext>, 8> operatorContext;
  SymbolManager symbolManager;
  std::unique_ptr<llvm::LLVMContext> context;
  std::unique_ptr<llvm::Module> module;
  Database& dbref;

  uint64_t getLineItemCount() const;

  unsigned pipelineIndex = 0;

  Query(Database& db, std::string_view name = "query")
      : dbref(db), context(std::make_unique<llvm::LLVMContext>()), module(std::make_unique<llvm::Module>(name, *context)) {}

  template <PipelineType ptype>
  llvm::Function *addPipeline(llvm::FunctionType *type, auto &&...args) {
    auto *pipeline = llvm::Function::Create(
        type, llvm::Function::ExternalLinkage,
        "f" + llvm::Twine(pipelineIndex++), module.get());
    if constexpr (ptype == PipelineType::Scan)
      pipelines.emplace_back(std::make_unique<ScanPipeline>(pipeline, pipeline->getName(), args...));
    else
      pipelines.emplace_back(std::make_unique<Pipeline>(ptype, pipeline, pipeline->getName()));
    return pipeline;
  }

  template <typename T>
    requires std::derived_from<T, OperatorContext>
  T *addOperatorContext(std::unique_ptr<T> &&context) {
    auto *ptr = context.get();
    operatorContext.push_back(std::move(context));
    return ptr;
  }

  [[nodiscard]] inline Pipeline &getPipeline() { return *pipelines.back(); }


  [[nodiscard]] inline llvm::Module *getModule() { return module.get(); };

};
} // namespace p2cllvm