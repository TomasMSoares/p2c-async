#include "IR/Binop.h"
#include "IR/Builder.h"
#include "IR/ColTypes.h"
#include "IR/Defs.h"
#include "IR/Expression.h"
#include "IR/Pipeline.h"
#include "IR/Scope.h"
#include "IR/SymbolManager.h"
#include "IR/Types.h"

#include "internal/basetypes.h"
#include "internal/Database.h"
#include "runtime/Runtime.h"
#include "runtime/hyperloglog.h"
#include "runtime/tuplebuffer.h"

#include <cassert>
#include <cstdint>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instruction.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <string_view>

namespace p2cllvm {

Pipeline &Builder::createPipeline() {
  return createBasicPipeline<PipelineType::Default>();
}

Pipeline &Builder::createScanPipeline(size_t tableIndex) {
  auto *Int64Ty = llvm::Type::getInt64Ty(builder.getContext());
  /// 1: begin, 2: end, 3: num_threads
  auto *type = llvm::FunctionType::get(
      llvm::Type::getVoidTy(builder.getContext()),
      {llvm::PointerType::get(builder.getContext(), 0), Int64Ty, Int64Ty,
       Int64Ty, llvm::PointerType::get(builder.getContext(), 0)},
      false);
  auto *fn = query.addPipeline<PipelineType::Scan>(type, tableIndex);
  scope = Scope::get(fn);
  auto *entry = llvm::BasicBlock::Create(builder.getContext(), "entry", fn);
  builder.SetInsertPoint(entry);
  return query.getPipeline();
}

Pipeline &Builder::createContinuationPipeline() {
  return createBasicPipeline<PipelineType::Continuation>();
}

void Builder::finishPipeline() { builder.CreateRetVoid(); }

llvm::Function *Builder::createCmpFunction() {
  static unsigned fi = 0;
  auto &context = builder.getContext();
  auto &module = *query.getModule();
  auto *ptr = llvm::PointerType::get(context, 0);
  auto *ftype = llvm::FunctionType::get(llvm::Type::getInt32Ty(context),
                                        {ptr, ptr}, false);
  BasicBlockRef bb = builder.GetInsertBlock();
  auto *cmpFn = llvm::Function::Create(ftype, llvm::Function::InternalLinkage,
                                       "cmp" + llvm::Twine(fi++), module);
  BasicBlockRef entry = createBasicBlock("entry", cmpFn);
  builder.SetInsertPoint(entry);
  return cmpFn;
}

ValueRef<> Builder::createExpEval(std::unique_ptr<Exp> &exp) {
  return exp->compile(*this);
}

ValueRef<> Builder::addAndCreatePipelineArg(void *ptr) {
  auto &pipeline = query.getPipeline();
  size_t idx = pipeline.addPipelineArg(ptr);
  return createAPipelineArgAccess(scope->getOperatorArgs(), idx);
}

ValueRef<llvm::AllocaInst> Builder::createAlloca(TypeRef<> type,
                                                 llvm::Twine name) {
  auto *cbb = builder.GetInsertBlock();
  builder.SetInsertPoint(scope->getCurrentEntryBlock(),
                         scope->getCurrentEntryBlock()->begin());
  auto *alloca = builder.CreateAlloca(type, nullptr, name);
  builder.SetInsertPoint(cbb);
  return alloca;
}

ValueRef<> Builder::createHLLEstimate(ValueRef<> sketch) {
  auto *module = query.getModule();
  auto fn = query.symbolManager.getOrInsertFunction(
      module, "hll_estimate", &hll_estimate,
      llvm::FunctionType::get(llvm::Type::getInt64Ty(builder.getContext()),
                              {llvm::PointerType::get(builder.getContext(), 0)},
                              false));
  return builder.CreateCall(fn, {sketch});
}

ValueRef<> Builder::createAPipelineArgAccess(ValueRef<> args, unsigned idx) {
  auto *cbb = builder.GetInsertBlock();
  builder.SetInsertPoint(scope->getCurrentEntryBlock(),
                         scope->getCurrentEntryBlock()->begin());

  auto *arg_type = llvm::PointerType::get(
      llvm::PointerType::get(builder.getContext(), 0), 0);
  auto *ptr = builder.CreateConstInBoundsGEP1_32(arg_type, args, idx);
  auto *val = builder.CreateLoad(arg_type, ptr);
  builder.SetInsertPoint(cbb);
  return val;
}

void Builder::createPackTuple(Tuple &type, llvm::Value *tuple,
                              std::vector<IU *> &ius,
                              ValueRef<> string_heap /*=nullptr*/) {
  auto &layout = type.getLayout();
  auto *ttype = type.getType();
  for (size_t i = 0; i < ius.size(); ++i) {
    const auto &offset = layout.lookup(ius[i]);
    auto *iu = ius[i];
    auto *elemptr = builder.CreateConstGEP2_32(
        llvm::ArrayType::get(llvm::Type::getInt8Ty(builder.getContext()), 0),
        tuple, 0, offset);
    createCopy(scope->lookupValue(iu), elemptr, iu->type, string_heap);
  }
}

ValueRef<> Builder::createHashTableAlloc(ValueRef<> ht, ValueRef<> size) {
  auto *module = query.getModule();
  auto fn = query.symbolManager.getOrInsertFunction(
      module, "hashtable_alloc", &hashtable_alloc,
      llvm::FunctionType::get(
          llvm::PointerType::get(llvm::Type::getInt8Ty(builder.getContext()),
                                 0),
          {llvm::PointerType::get(builder.getContext(), 0),
           llvm::Type::getInt64Ty(builder.getContext())},
          false));
  builder.CreateCall(fn, {ht, size});
  return ht;
}

ValueRef<> Builder::createTupleBufferInsert(ValueRef<> tb, size_t size) {
  auto *module = query.getModule();
  auto fn = query.symbolManager.getOrInsertFunction(
      module, "tb_insert", &tb_insert,
      llvm::FunctionType::get(llvm::PointerType::get(builder.getContext(), 0),
                              {llvm::PointerType::get(builder.getContext(), 0),
                               llvm::Type::getInt64Ty(getContext())},
                              false));
  return builder.CreateCall(
      fn,
      {tb, llvm::ConstantInt::get(llvm::Type::getInt64Ty(getContext()), size)});
}

ValueRef<> Builder::createHashTableInsert(ValueRef<> ht, ValueRef<> entry,
                                          ValueRef<> hash) {
  auto *module = query.getModule();
  auto fn = query.symbolManager.getOrInsertFunction(
      module, "hashtable_insert", &hashtable_insert,
      llvm::FunctionType::get(llvm::PointerType::get(builder.getContext(), 0),
                              {llvm::PointerType::get(builder.getContext(), 0),
                               llvm::PointerType::get(builder.getContext(), 0),
                               llvm::Type::getInt64Ty(builder.getContext())},
                              false));
  return builder.CreateCall(fn, {ht, entry, hash});
}

ValueRef<> Builder::createSketchAdd(ValueRef<> sketch, ValueRef<> hash) {
  auto *module = query.getModule();
  auto fn = query.symbolManager.getOrInsertFunction(
      module, "hll_add", &hll_add,
      llvm::FunctionType::get(
          llvm::Type::getVoidTy(builder.getContext()),
          {llvm::PointerType::get(Sketch::createType(builder.getContext()), 0),
           llvm::Type::getInt64Ty(builder.getContext())},
          false));
  return builder.CreateCall(fn, {sketch, hash});
}

llvm::SmallVector<ValueRef<llvm::BranchInst>, 8>
Builder::createCmpKeys(llvm::ArrayRef<ValueRef<>> left,
                       llvm::ArrayRef<ValueRef<>> right,
                       llvm::ArrayRef<IU *> ius) {
  llvm::SmallVector<llvm::BranchInst *, 8> branches;
  for (size_t i = 0; i < left.size(); ++i) {
    auto *res = createCompare(left[i], right[i], ius[i]->type);
    auto *cnt =
        llvm::BasicBlock::Create(builder.getContext(), "cnt", scope->pipeline);
    branches.push_back(builder.CreateCondBr(res, cnt, nullptr));
    builder.SetInsertPoint(cnt);
  }
  return branches;
}

BasicBlockRef Builder::createEndCmpKeys(
    llvm::SmallVector<ValueRef<llvm::BranchInst>, 8> &cmp) {
  auto *gcnt =
      llvm::BasicBlock::Create(builder.getContext(), "gcnt", scope->pipeline);
  for (auto *branch : cmp)
    branch->setSuccessor(1, gcnt);
  return gcnt;
}

void Builder::createColumnAccess(ValueRef<> index, ValueRef<> ptr, IU *column) {
  auto coltype = column->type.typeEnum;

  // ptr points to column data (either mmap'd array or decompressed Parquet buffer)
  // For StringView: ptr -> String* (slotted page format)
  // For fixed types: ptr -> T* (array of elements)
  
  if (coltype == TypeEnum::String) {
    auto &context = builder.getContext();
    auto *sv = createAlloca(StringTy::createType(context), "sv_");
    auto *module = query.getModule();
    
    auto fn = query.symbolManager.getOrInsertFunction(
        module, "load_from_slotted_page", &load_from_slotted_page,
        llvm::Type::getVoidTy(context), llvm::Type::getInt64Ty(context),
        llvm::PointerType::get(context, 0),
        llvm::PointerType::get(context, 0));
    
    builder.CreateCall(fn, {index, ptr, sv});
    
    scope->updatePtr(column, sv);
    scope->updateValue(column, sv); // StringView loaded by-reference
  } else {
    // Fixed-size column - direct array indexing
    auto* elem_llvm_type = column->type.createType(getContext());
    auto* elem_ptr_type = llvm::PointerType::get(elem_llvm_type, 0);
    auto* typed_data_ptr = builder.CreateBitCast(ptr, elem_ptr_type);
    
    ValueRef<> colptr = builder.CreateGEP(elem_llvm_type, typed_data_ptr, index);
    
    scope->updatePtr(column, colptr);
    scope->updateValue(column, column->type.createLoad(colptr, *this));
  }
}

ValueRef<> Builder::createCopy(ValueRef<> src, ValueRef<> dest, Type &column,
                               ValueRef<> string_heap /*= nullptr*/) {
  return column.createCopy(src, dest, *this, string_heap);
}

ValueRef<> Builder::createColumnLoad(ValueRef<> ptr, Type &column) {
  return column.createLoad(ptr, *this);
}

ValueRef<> Builder::createLoad(ValueRef<> ptr, Type &column) {
  return column.createLoad(ptr, *this);
}

ValueRef<> Builder::createCompare(ValueRef<> lhs, ValueRef<> rhs, Type &type) {
  return type.createBinOp(BinOp::CMPEQ, lhs, rhs, *this);
}

ValueRef<> Builder::createStackStore(ValueRef<> val) {
  auto *ptr = createAlloca(val->getType());
  builder.CreateStore(val, ptr);
  return ptr;
}

std::pair<ValueRef<>, ValueRef<>> Builder::createTupleBufferGet(ValueRef<> tb) {
  auto *module = query.getModule();
  auto fn = query.symbolManager.getOrInsertFunction(
      module, "getBuffers", &getBuffers,
      llvm::FunctionType::get(
          llvm::PointerType::get(Buffer::createType(builder.getContext()), 0),
          {llvm::PointerType::get(builder.getContext(), 0)}, false));
  auto *buffers = builder.CreateCall(fn, {tb});
  auto fnnumBuffers = query.symbolManager.getOrInsertFunction(
      module, "getNumBuffers", &getNumBuffers,
      llvm::FunctionType::get(llvm::Type::getInt64Ty(builder.getContext()),
                              {llvm::PointerType::get(builder.getContext(), 0)},
                              false));
  auto *numBuffers = builder.CreateCall(fnnumBuffers, {tb});
  return {buffers, numBuffers};
}

std::pair<ValueRef<>, ValueRef<>>
Builder::createAccessBuffer(ValueRef<> buffers, ValueRef<> idx) {
  auto *buffer =
      builder.CreateGEP(Buffer::createType(builder.getContext()), buffers, idx);
  auto *memptr = builder.CreateStructGEP(
      Buffer::createType(builder.getContext()), buffer, 2);
  auto *mem =
      builder.CreateLoad(llvm::PointerType::get(getContext(), 0), memptr);
  auto *size = builder.CreateStructGEP(Buffer::createType(builder.getContext()),
                                       buffer, 0);
  return {mem, builder.CreateLoad(llvm::Type::getInt64Ty(builder.getContext()),
                                  size, false)};
}

ValueRef<> Builder::createBufferElemAccess(ValueRef<> mem, ValueRef<> idx) {
  return builder.CreateGEP(llvm::Type::getInt8Ty(builder.getContext()), mem,
                           idx);
}

[[nodiscard]] llvm::IRBuilder<> &Builder::getBuilder() { return builder; }

[[nodiscard]] llvm::LLVMContext &Builder::getContext() {
  return builder.getContext();
}

ValueRef<> Builder::createHashTableLookUp(ValueRef<> ht, ValueRef<> hash) {
  auto *module = query.getModule();
  auto fn = query.symbolManager.getOrInsertFunction(
      module, "hashtable_lookup", &hashtable_lookup,
      llvm::FunctionType::get(llvm::PointerType::get(builder.getContext(), 0),
                              {llvm::PointerType::get(builder.getContext(), 0),
                               llvm::Type::getInt64Ty(builder.getContext())},
                              false));
  return builder.CreateCall(fn, {ht, hash});
}

void Builder::createBranch(BasicBlockRef bb, BasicBlockRef target) {
  auto *cbb = builder.GetInsertBlock();
  builder.SetInsertPoint(bb);
  builder.CreateBr(target);
  builder.SetInsertPoint(cbb);
}

ValueRef<> Builder::createCast(Type &mtype, ValueRef<> val, Type &type,
                               TypeRef<> target) {
  return mtype.createCast(val, type.typeEnum, target, *this);
}

void Builder::createPrints(llvm::ArrayRef<IU *> ius) {
#define PRINT_FUNC(type, name)                                                 \
  createPrint(type##Ty::createType(builder.getContext()),                      \
              scope->lookupValue(iu), &name, #name)
  for (auto *iu : ius) {
    switch (iu->type.typeEnum) {
    case TypeEnum::Bool:
      PRINT_FUNC(Bool, printBool);
      break;
    case TypeEnum::Integer:
      PRINT_FUNC(Integer, printInteger);
      break;
    case TypeEnum::Double:
      PRINT_FUNC(Double, printDouble);
      break;
    case TypeEnum::Char:
      PRINT_FUNC(Char, printChar);
      break;
    case TypeEnum::String:
      createPrint(llvm::PointerType::get(builder.getContext(), 0),
                  scope->lookupValue(iu), printStringView, "printStringView");
      break;
    case TypeEnum::BigInt:
      PRINT_FUNC(BigInt, printBigInt);
      break;
    case TypeEnum::Date:
      PRINT_FUNC(Date, printDate);
      break;
    default:
      throw std::runtime_error("Unsupported type");
    }
  }
#undef PRINT_FUNC
  auto fn = query.symbolManager.getOrInsertFunction(
      query.getModule(), "printNewline", &printNewline,
      llvm::FunctionType::get(llvm::Type::getVoidTy(builder.getContext()), {},
                              false));
  builder.CreateCall(fn, {});
}

ValueRef<> Builder::createColumnPtrLoad(ValueRef<> ptr, TypeRef<> coltype,
                                        Type &type) {
  auto *colptr = builder.CreateConstInBoundsGEP1_32(coltype, ptr, 0);
  return builder.CreateLoad(llvm::PointerType::get(getContext(), 0), colptr);
}

ValueRef<> Builder::createIULoad(IU *iu) { return scope->lookupValue(iu); }

[[nodiscard]] llvm::Function *Builder::getCurrentPipeline() {
  return scope->pipeline;
}

[[nodiscard]] Scope &Builder::getCurrentScope() {
  assert(scope && "No current scope");
  return *scope.get();
}

ValueRef<llvm::PHINode> Builder::createBeginIndexIter(ValueRef<> start,
                                                      ValueRef<> end) {
  auto *cbb = builder.GetInsertBlock();
  llvm::BasicBlock *begin =
      llvm::BasicBlock::Create(builder.getContext(), "begin", scope->pipeline);
  llvm::BasicBlock *body =
      llvm::BasicBlock::Create(builder.getContext(), "body", scope->pipeline);
  builder.CreateBr(begin);
  builder.SetInsertPoint(begin);
  auto *itervar =
      builder.CreatePHI(llvm::Type::getInt64Ty(builder.getContext()), 2);
  itervar->addIncoming(start, cbb);
  auto *cmp = builder.CreateICmpULT(itervar, end);
  builder.CreateCondBr(cmp, body, nullptr);
  builder.SetInsertPoint(body);
  scope->loopInfo.emplace_back(begin, itervar);
  return itervar;
}

void Builder::createEndIndexIter(size_t diff) {
  auto [begin, iter] = scope->loopInfo.back();
  llvm::BasicBlock *cnt =
      llvm::BasicBlock::Create(builder.getContext(), "cnt", scope->pipeline);
  auto *next = builder.CreateAdd(
      iter, llvm::ConstantInt::get(llvm::Type::getInt64Ty(builder.getContext()),
                                   diff));
  iter->addIncoming(next, builder.GetInsertBlock());
  builder.CreateBr(begin);
  auto *branch = llvm::dyn_cast<llvm::BranchInst>(begin->getTerminator());
  branch->setSuccessor(1, cnt);
  builder.SetInsertPoint(cnt);
  scope->loopInfo.pop_back();
}

ValueRef<> Builder::createBeginTupleBufferIter(ValueRef<> tupleBuffer) {
  auto [buffers, size] = createTupleBufferGet(tupleBuffer);
  auto oidx = createBeginIndexIter(
      llvm::ConstantInt::get(llvm::Type::getInt64Ty(builder.getContext()), 0),
      size);
  auto [buf, bfsize] = createAccessBuffer(buffers, oidx);
  auto iidx = createBeginIndexIter(
      llvm::ConstantInt::get(llvm::Type::getInt64Ty(builder.getContext()), 0),
      bfsize);
  return createBufferElemAccess(buf, iidx);
}

void Builder::createEndTupleBufferIter(size_t allocSize) {
  createEndIndexIter(allocSize);
  createEndIndexIter();
}

ValueRef<llvm::PHINode> Builder::createBeginForwardIter(ValueRef<> ptr) {
  auto *cbb = builder.GetInsertBlock();
  llvm::BasicBlock *begin =
      llvm::BasicBlock::Create(builder.getContext(), "begin", scope->pipeline);
  llvm::BasicBlock *body =
      llvm::BasicBlock::Create(builder.getContext(), "body", scope->pipeline);
  builder.CreateBr(begin);
  builder.SetInsertPoint(begin);
  auto *itervar =
      builder.CreatePHI(llvm::PointerType::get(builder.getContext(), 0), 2);
  itervar->addIncoming(ptr, cbb);
  auto *cmp = builder.CreateICmpNE(
      itervar, llvm::ConstantPointerNull::get(
                   llvm::PointerType::get(builder.getContext(), 0)));
  builder.CreateCondBr(cmp, body, nullptr);
  builder.SetInsertPoint(body);
  scope->loopInfo.emplace_back(begin, itervar);
  return itervar;
}

BasicBlockRef Builder::createBasicBlock(llvm::StringRef name,
                                        llvm::Function *fun) {
  return llvm::BasicBlock::Create(builder.getContext(), name, fun);
}

BasicBlockRef Builder::createBasicBlock(llvm::StringRef name) {
  return createBasicBlock(name, getCurrentPipeline());
}

BasicBlockRef Builder::getInsertBlock() const {
  return builder.GetInsertBlock();
}

void Builder::setInsertPoint(BasicBlockRef bb) {
  return builder.SetInsertPoint(bb);
}

TypeRef<llvm::PointerType> Builder::getPtrTy() { return builder.getPtrTy(); }
TypeRef<llvm::IntegerType> Builder::getInt64ty() {
  return builder.getInt64Ty();
}
TypeRef<llvm::IntegerType> Builder::getInt32ty() {
  return builder.getInt32Ty();
}
TypeRef<llvm::IntegerType> Builder::getInt8ty() { return builder.getInt8Ty(); }
TypeRef<llvm::IntegerType> Builder::getInt1ty() { return builder.getInt1Ty(); }
TypeRef<llvm::Type> Builder::getVoidTy() { return builder.getVoidTy(); }

ValueRef<> Builder::getInt64Constant(uint64_t val) {
  return builder.getInt64(val);
}
ValueRef<> Builder::getInt32Constant(uint32_t val) {
  return builder.getInt32(val);
}
ValueRef<> Builder::getInt8Constant(uint8_t val) {
  return builder.getInt8(val);
}
ValueRef<> Builder::getInt1Constant(bool val) {
  return val ? builder.getTrue() : builder.getFalse();
}
ValueRef<> Builder::getNullPtrConstant() {
  return llvm::ConstantPointerNull::get(getPtrTy());
}

void Builder::createMissingBranch(BasicBlockRef bb, BasicBlockRef tbb,
                                  BasicBlockRef fbb) {
  auto cbb = getInsertBlock();
  setInsertPoint(bb);
  ValueRef<llvm::Instruction> cond = &bb->back();
  builder.CreateCondBr(cond, tbb, fbb);
  setInsertPoint(cbb);
}

void Builder::createBranch(ValueRef<> cnd, BasicBlockRef tbb,
                           BasicBlockRef fbb) {
  builder.CreateCondBr(cnd, tbb, fbb);
}

void Builder::createBranch(BasicBlockRef bb) { builder.CreateBr(bb); }
} // namespace p2cllvm
