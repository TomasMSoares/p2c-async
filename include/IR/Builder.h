#pragma once
#include <concepts>
#include <cstdint>
#include <llvm/ADT/ArrayRef.h>
#include <llvm/ADT/DenseMap.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instruction.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/Support/raw_ostream.h>
#include <memory>
#include <type_traits>

#include "Defs.h"
#include "Pipeline.h"
#include "Scope.h"
#include "Tuple.h"
#include "internal/basetypes.h"
#include "operators/Iu.h"
#include "runtime/Runtime.h"
#include "runtime/hashtables.h"
#include "runtime/tuplebuffer.h"
namespace p2cllvm {
class Operator;
class Tuple;
class Exp;
class Type;

class Builder {
public:
  llvm::IRBuilder<> builder;
  Query &query;
  std::unique_ptr<Scope> scope;

  Builder(Query &query) : builder(*query.context), query(query) {}

  Pipeline &createPipeline();

  Pipeline &createScanPipeline(size_t tableIndex);

  Pipeline &createContinuationPipeline();

  void finishPipeline();

  ValueRef<> createExpEval(std::unique_ptr<Exp> &exp);

  ValueRef<> createAPipelineArgAccess(ValueRef<> args, unsigned idx);

  ValueRef<> addAndCreatePipelineArg(void *ptr);

  ValueRef<llvm::Function> createCmpFunction();

  ///-------------------------------------------------------------------------------
  /// Memory Accesses
  /// ------------------------------------------------------------------------------
  void createPackTuple(Tuple &type, ValueRef<> tuple, std::vector<IU *> &ius,
                       ValueRef<> string_heap = nullptr);

  template <typename R = void>
    requires std::same_as<R, void> || std::same_as<R, std::vector<ValueRef<>>>
  R createUnpackTuple(Tuple &type, ValueRef<> tuple, std::vector<IU *> &ius) {
    auto &layout = type.getLayout();
    auto *ttype = type.getType();
    auto *ptype =
        llvm::ArrayType::get(llvm::Type::getInt8Ty(builder.getContext()), 0);
    if constexpr (!std::same_as<R, void>) {
      R vec;
      vec.reserve(ius.size());
      for (size_t i = 0; i < ius.size(); ++i) {
        size_t offset = layout.lookup(ius[i]);
        auto *iu = ius[i];
        auto *elemptr = builder.CreateConstGEP2_32(ptype, tuple, 0, offset);
        vec.push_back(createLoad(elemptr, iu->type));
      }
      return vec;
    } else {
      for (size_t i = 0; i < ius.size(); ++i) {
        size_t offset = layout.lookup(ius[i]);
        auto *iu = ius[i];
        auto *elemptr = builder.CreateConstGEP2_32(ptype, tuple, 0, offset);
        scope->updatePtr(iu, elemptr);
        scope->updateValue(iu, iu->type.createLoad(elemptr, *this));
      }
    }
  }

  ValueRef<> createStackStore(ValueRef<> val);

  template <typename T = HashTableEntry>
  ValueRef<> createDataAccess(ValueRef<> entry) {
    return T::createAccess(entry, *this);
  }

  template <typename T = HashTableEntry>
  ValueRef<llvm::Instruction> createStoreHash(ValueRef<> entry,
                                              ValueRef<> hash) {
    ValueRef<> hashptr =
        builder.CreateStructGEP(T::createType(builder.getContext()), entry, 0);
    return builder.CreateStore(hash, hashptr);
  }

  template <typename T = HashTableEntry>
  llvm::Instruction *createLoadHash(ValueRef<> entry) {
    auto *hashptr =
        builder.CreateStructGEP(T::createType(builder.getContext()), entry, 0);
    return builder.CreateLoad(llvm::Type::getInt64Ty(builder.getContext()),
                              hashptr);
  }

  template <typename T = HashTableEntry>
  ValueRef<> createLoadData(ValueRef<> entry) {
    return builder.CreateStructGEP(T::createType(builder.getContext()), entry,
                                   1);
  }

  template <typename T = HashTableEntry>
  llvm::Instruction *createLoadNext(ValueRef<> entry) {
    auto *nextptr =
        builder.CreateStructGEP(T::createType(builder.getContext()), entry, 0);
    return builder.CreateLoad(getPtrTy(), nextptr);
  }

  llvm::AllocaInst *createAlloca(llvm::Type *type, llvm::Twine name = "");

  ///-------------------------------------------------------------------------------
  /// Hashtable
  /// ------------------------------------------------------------------------------
  ValueRef<> createHashTableLookUp(ValueRef<> ht, ValueRef<> hash);

  ValueRef<> createHashTableAlloc(ValueRef<> ht, ValueRef<> size);

  ValueRef<> createHashTableInsert(ValueRef<> ht, ValueRef<> entry,
                                   ValueRef<> hash);

  llvm::SmallVector<llvm::BranchInst *, 8>
  createCmpKeys(llvm::ArrayRef<ValueRef<>> left,
                     llvm::ArrayRef<ValueRef<>> right,
                     llvm::ArrayRef<IU *> ius);
  BasicBlockRef createEndCmpKeys(llvm::SmallVector<llvm::BranchInst *, 8> &cmp);

  template <typename H>
  ValueRef<> createHashKeysHasher(llvm::ArrayRef<IU *> ius) {
    return H::createHash(ius, *this);
  }

  void createBranch(llvm::BasicBlock *bb, llvm::BasicBlock *to);

  ///-------------------------------------------------------------------------------
  /// Sketch
  /// ------------------------------------------------------------------------------
  ValueRef<> createSketchAdd(ValueRef<> sketch, ValueRef<> hash);

  ValueRef<> createHLLEstimate(ValueRef<> sketch);

  ///-------------------------------------------------------------------------------
  /// TupleBuffer
  /// ------------------------------------------------------------------------------
  ValueRef<> createTupleBufferInsert(ValueRef<> tb, size_t size);

  std::pair<ValueRef<>, ValueRef<>> createTupleBufferGet(ValueRef<> tb);

  std::pair<ValueRef<>, ValueRef<>> createAccessBuffer(ValueRef<> buffers,
                                                       ValueRef<> idx);

  ValueRef<> createBufferElemAccess(ValueRef<> mem, ValueRef<> idx);

  ///-------------------------------------------------------------------------------
  /// Aggregation
  /// ------------------------------------------------------------------------------
  ValueRef<> createCountAggregation(ValueRef<> agg);

  ValueRef<> createSumAggregation(ValueRef<> agg);

  ///-------------------------------------------------------------------------------
  /// Type Dependent
  /// ------------------------------------------------------------------------------
  void createColumnAccess(ValueRef<> index, ValueRef<> ptr, IU *column);

  ValueRef<> createCopy(ValueRef<> src, ValueRef<> dest, Type &column,
                        ValueRef<> string_heap = nullptr); // optional string_heap for deep copies

  ValueRef<> createColumnLoad(ValueRef<> ptr, Type &type);

  ValueRef<> createLoad(ValueRef<> ptr, Type &type);

  ValueRef<> createCompare(ValueRef<> lhs, ValueRef<> rhs, Type &type);

  ValueRef<> createCast(Type &mtype, ValueRef<> val, Type &type,
                        TypeRef<> target);

  void createPrints(llvm::ArrayRef<IU *> ius);

  ///-------------------------------------------------------------------------------
  /// Loop Building Blocks
  /// ------------------------------------------------------------------------------
  ValueRef<llvm::PHINode> createBeginIndexIter(ValueRef<> start,
                                               ValueRef<> end);

  void createEndIndexIter(size_t diff = 1);

  llvm::PHINode *createBeginForwardIter(ValueRef<> ptr);

  template <p2c_llvm_struct_type T = HashTableEntry, bool signExt = false>
  llvm::BasicBlock *createEndForwardIter() {
    auto [begin, iter] = scope->loopInfo.back();
    llvm::BasicBlock *cnt =
        llvm::BasicBlock::Create(builder.getContext(), "cnt", scope->pipeline);

    ValueRef<> rptr;
    if constexpr (signExt) {
      rptr = createGetPtr<>(iter);
    } else {
      rptr = iter;
    }
    ValueRef<> next = builder.CreateLoad(
        getPtrTy(),
        builder.CreateStructGEP(T::createType(builder.getContext()), rptr, 0));
    iter->addIncoming(next, builder.GetInsertBlock());
    builder.CreateBr(begin);
    auto *branch = llvm::dyn_cast<llvm::BranchInst>(begin->getTerminator());
    branch->setSuccessor(1, cnt);
    builder.SetInsertPoint(cnt);
    scope->loopInfo.pop_back();
    return cnt;
  }

  ValueRef<> createBeginTupleBufferIter(ValueRef<> tupleBuffer);

  void createEndTupleBufferIter(size_t allocSize);

  ValueRef<> createColumnPtrLoad(ValueRef<> ptr, TypeRef<> coltype, Type &type);

  [[nodiscard]] llvm::IRBuilder<> &getBuilder();

  [[nodiscard]] llvm::LLVMContext &getContext();

  [[nodiscard]] llvm::Function *getCurrentPipeline();

  [[nodiscard]] Scope &getCurrentScope();
  ///--------------------------------------------------------------------------------
  /// IU Handling
  ValueRef<> createIULoad(IU *iu);

  ///--------------------------------------------------------------------------------
  /// LLVM-Wrapper

  template <typename T, typename... Args>
    requires(std::derived_from<std::remove_pointer_t<Args>,
                               std::remove_pointer_t<ValueRef<>>> &&
             ...)
  ValueRef<> createCall(llvm::StringRef fname, T *fun, TypeRef<> ret,
                        Args... args) {
    auto fn = query.symbolManager.getOrInsertFunction(
        query.getModule(), fname, fun, ret, args->getType()...);
    return builder.CreateCall(fn, {args...});
  }

  ///--------------------------------------------------------------------------------
  /// Types
  template <typename... T>
  TypeRef<llvm::StructType> createStructType(llvm::StringRef name,
                                             T &&...args) {
    return getOrCreateType(builder.getContext(), name,
                           [&]() { return llvm::StructType::get(args...); });
  }

  TypeRef<llvm::PointerType> getPtrTy();
  TypeRef<llvm::IntegerType> getInt64ty();
  TypeRef<llvm::IntegerType> getInt32ty();
  TypeRef<llvm::IntegerType> getInt8ty();
  TypeRef<llvm::IntegerType> getInt1ty();
  TypeRef<llvm::Type> getVoidTy();

  ValueRef<> getInt64Constant(uint64_t val);
  ValueRef<> getInt32Constant(uint32_t val);
  ValueRef<> getInt8Constant(uint8_t val);
  ValueRef<> getInt1Constant(bool val);
  ValueRef<> getNullPtrConstant();

  ///--------------------------------------------------------------------------------
  /// Basic Block
  BasicBlockRef createBasicBlock(llvm::StringRef name,
                                 ValueRef<llvm::Function> fun);
  BasicBlockRef createBasicBlock(llvm::StringRef name);
  void setInsertPoint(BasicBlockRef bb);
  BasicBlockRef getInsertBlock() const;

  void createMissingBranch(BasicBlockRef bb, BasicBlockRef tbb,
                           BasicBlockRef fbb);
  void createBranch(ValueRef<> cnd, BasicBlockRef tbb, BasicBlockRef fbb);
  void createBranch(BasicBlockRef cbb);

  ///--------------------------------------------------------------------------------
  /// PointerMagic
  template <bool fcall = false> ValueRef<> createGetPtr(ValueRef<> ptr) {
    if constexpr (!fcall) {
      constexpr size_t tagSize = 16;
      ValueRef<> intptr = builder.CreatePtrToInt(ptr, getInt64ty());
      intptr = builder.CreateShl(intptr, tagSize);
      intptr = builder.CreateAShr(intptr, tagSize);
      return builder.CreateIntToPtr(intptr, getPtrTy());
    } else {
      return createCall("signExtend", &signExtend, getPtrTy(), ptr);
    }
  }

  template <bool fcall = false>
  BasicBlockRef createCmpTag(ValueRef<> ptr, ValueRef<> hash) {
    BasicBlockRef bb = getInsertBlock();
    if constexpr (!fcall) {
      ValueRef<> intptr = builder.CreatePtrToInt(ptr, getInt64ty());
      ValueRef<> reftag = builder.CreateLShr(intptr, builder.getInt64(48));
      ValueRef<> shifted = builder.CreateLShr(hash, builder.getInt64(48));
      builder.CreateICmpNE(shifted, builder.CreateAnd(shifted, reftag));
    } else {
      createCall("cmpTag", &cmpTag, getInt1ty(), ptr, hash);
    }
    setInsertPoint(createBasicBlock("cnt"));
    return bb;
  }

private:
  void createPrint(llvm::Type *type, ValueRef<> val, auto *fname,
                   std::string_view name) {
    auto *module = query.getModule();
    auto fn = query.symbolManager.getOrInsertFunction(
        module, name, fname,
        llvm::FunctionType::get(llvm::Type::getVoidTy(builder.getContext()),
                                {type}, false));
    builder.CreateCall(fn, {val});
  }

  template <PipelineType t> Pipeline &createBasicPipeline() {
    auto *type = llvm::FunctionType::get(
        llvm::Type::getVoidTy(builder.getContext()),
        {llvm::PointerType::get(builder.getContext(), 0)}, false);

    auto *fn = query.addPipeline<t>(type);
    scope = Scope::get(fn);
    assert(fn->arg_size() > 0);
    auto *entry = llvm::BasicBlock::Create(builder.getContext(), "entry", fn);
    builder.SetInsertPoint(entry);
    return query.getPipeline();
  }
};
}; // namespace p2cllvm
