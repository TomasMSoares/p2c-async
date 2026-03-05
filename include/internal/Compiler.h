#pragma once

#include <cassert>
#include <cstdint>
#include <llvm/ADT/STLExtras.h>
#include <llvm/ADT/iterator_range.h>
#include <llvm/Analysis/CGSCCPassManager.h>
#include <llvm/Analysis/LoopAnalysisManager.h>
#include <llvm/ExecutionEngine/JITEventListener.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/ExecutionEngine/Orc/Shared/ExecutorAddress.h>
#include <llvm/ExecutionEngine/Orc/SymbolStringPool.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Dominators.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/InstVisitor.h>
#include <llvm/IR/Instruction.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/PassManager.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/Casting.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Scalar/Reassociate.h>
#include <llvm/Transforms/Scalar/SimplifyCFG.h>
#include <llvm/ADT/PostOrderIterator.h>
#include <memory>
#include <string_view>

#include "IR/Pipeline.h"
#include "IR/SymbolManager.h"

namespace p2cllvm {
struct LoadVisitor : public llvm::InstVisitor<LoadVisitor> {
  llvm::IRBuilder<> builder;
  llvm::DominatorTree &domtree;

  LoadVisitor(llvm::LLVMContext& context, llvm::DominatorTree& domtree): builder(context), domtree(domtree) {}

  void visitLoad(llvm::LoadInst &inst) {
    if (inst.getType()->isPointerTy())
      return;
    auto uiter = inst.user_begin();
    auto uedn = inst.user_end();
    if(uiter == uedn)
        return;
    llvm::Instruction *domInst = llvm::dyn_cast<llvm::Instruction>(*uiter);
    for (auto it = ++uiter; it != uedn; ++it) {
        domInst = domtree.findNearestCommonDominator(domInst, llvm::dyn_cast<llvm::Instruction>(*it));
    }
    builder.SetInsertPoint(domInst);
    auto *load = builder.CreateLoad(inst.getType(), inst.getPointerOperand());
    inst.replaceAllUsesWith(load);
    inst.eraseFromParent();
  }
};
class LateMaterializationPass
    : public llvm::PassInfoMixin<LateMaterializationPass> {
public:
  llvm::PreservedAnalyses run(llvm::Function &f,
                              llvm::FunctionAnalysisManager &fam) {
    auto &domtree = fam.getResult<llvm::DominatorTreeAnalysis>(f);
    LoadVisitor visitor(f.getContext(), domtree);
    for(auto* node: llvm::post_order(&domtree)){
        llvm::BasicBlock* bb = node->getBlock();
        for(auto& I: llvm::make_early_inc_range(llvm::reverse(*bb))){
            visitor.visit(I);
        }

    }
    return llvm::PreservedAnalyses::none();

  }

  static bool isRequired() { return true; }
};

class Optimizer {
public:
  void run(llvm::Module &module) {
    /// apparently we need all
    llvm::PassBuilder pb;
    llvm::FunctionAnalysisManager fam;
    llvm::LoopAnalysisManager lam;
    llvm::CGSCCAnalysisManager cam;
    llvm::ModuleAnalysisManager mam;
    pb.registerFunctionAnalyses(fam);
    pb.registerLoopAnalyses(lam);
    pb.registerCGSCCAnalyses(cam);
    pb.registerModuleAnalyses(mam);
    pb.crossRegisterProxies(lam, fam, cam, mam);

    for (auto &fn : module) {
      if (fn.isDeclaration())
        continue;
      fpm.run(fn, fam);
    }
  }
  Optimizer() {
    /// adapted from LingoDB
    fpm.addPass(llvm::InstCombinePass());
    // fpm.addPass(llvm::ReassociatePass());
    fpm.addPass(llvm::GVNPass());
    fpm.addPass(llvm::SimplifyCFGPass());
    fpm.addPass(LateMaterializationPass());
  }

private:
  llvm::FunctionPassManager fpm;
};

class QueryCompiler {
public:
  void createJIT() {
    /// adapted from https://github.com/llvm/llvm-project/blob/main/llvm/examples/OrcV2Examples/LLJITWithGDBRegistrationListener/LLJITWithGDBRegistrationListener.cpp  
    llvm::ExitOnError ExitOnErr;
    auto res = ExitOnErr(
        llvm::orc::LLJITBuilder()
            .setObjectLinkingLayerCreator(
                [&](llvm::orc::ExecutionSession &ES) -> llvm::Expected<std::unique_ptr<llvm::orc::ObjectLayer>> {
                  auto GetMemMgr = [](const llvm::MemoryBuffer &) {
                    return std::make_unique<llvm::SectionMemoryManager>();
                  };
                  auto ObjLinkingLayer =
                      std::make_unique<llvm::orc::RTDyldObjectLinkingLayer>(
                          ES, GetMemMgr);

                  ObjLinkingLayer->registerJITEventListener(
                      *llvm::JITEventListener::createPerfJITEventListener());

                  ObjLinkingLayer->setProcessAllSections(true);

                  return std::move(ObjLinkingLayer);
                })
            .create());
    jit = std::move(res);
  }

  void addQuery(Query &&query, std::unique_ptr<llvm::LLVMContext> &context) {
    query.module->setDataLayout(jit->getDataLayout());
    query.module->setTargetTriple(jit->getTargetTriple());
    optimizer.run(*query.getModule());
#ifndef NDEBUG
    llvm::errs() << *query.module << "\n";
    llvm::verifyModule(*query.module, &llvm::errs());
#endif
    auto ret = jit->addIRModule(llvm::orc::ThreadSafeModule(
        std::move(query.module),
        llvm::orc::ThreadSafeContext(std::move(context))));
    if (ret) {
      llvm::report_fatal_error(std::move(ret));
    }
  }

  void addSymbols(SymbolManager &symbolManager) {
    auto &jitDyLib = jit->getMainJITDylib();
    auto mangle = llvm::orc::MangleAndInterner(jit->getExecutionSession(),
                                               jit->getDataLayout());
    for (auto &[fun, ptr] : symbolManager) {
      addSymbolImpl(jitDyLib, fun, ptr, mangle);
    }
#ifndef NDEBUG
    jit->getMainJITDylib().dump(llvm::errs());
#endif
  }

  llvm::Expected<llvm::orc::ExecutorAddr>
  getPipelineFunction(std::string_view name) {
    auto fun = jit->lookup(name);
    if (!fun)
      return fun.takeError();
    return fun;
  }

private:
  void addSymbolImpl(llvm::orc::JITDylib &jitDyLib, std::string_view name,
                     uint64_t ptr, llvm::orc::MangleAndInterner &mangle) {
    auto ret = jitDyLib.define(llvm::orc::absoluteSymbols(
        {{mangle(name),
          {llvm::orc::ExecutorAddr(ptr), llvm::JITSymbolFlags::Callable}}}));
    if (ret) {
      llvm::report_fatal_error(std::move(ret));
    }
  }
  llvm::orc::LLJITBuilder builder;
  std::unique_ptr<llvm::orc::LLJIT> jit;
  Optimizer optimizer;
};

} // namespace p2cllvm
