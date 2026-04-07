#pragma once

#include "Builder.h"
#include "IR/Binop.h"
#include "IR/Defs.h"
#include "Types.h"
#include "internal/basetypes.h"
#include "operators/Iu.h"
#include "runtime/Runtime.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstddef>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Value.h>
#include <llvm/Support/Casting.h>
#include <memory>
#include <stdexcept>
#include <string_view>

namespace p2cllvm {
static inline constexpr bool checkPrecedence(Type &t1, Type &t2) {
  return decayTypes[static_cast<int>(t1.typeEnum)] <
         decayTypes[static_cast<int>(t2.typeEnum)];
}

struct Exp {
  ValueRef<> compile(Builder &builder) {
    checkSemantics();
    return createEval(builder);
  }
  virtual ValueRef<> createEval(Builder &builder) = 0;
  virtual IUSet getIUs() = 0;
  virtual ~Exp() = default;

  /// check Semantics before compiling(might add new nodes to fix issues)
  virtual Type &checkSemantics() = 0;
  virtual Type &getType() = 0;
};

struct CastExp : public Exp {
  Type &from, to;
  std::unique_ptr<Exp> child;

  CastExp(Type &from, Type &to, std::unique_ptr<Exp> &&exp)
      : from(from), to(to), child(std::move(exp)) {}

  Type &checkSemantics() override { return to; }

  ValueRef<> createEval(Builder &builder) override {
    return from.createCast(child->createEval(builder), to.typeEnum,
                           to.createType(builder.getContext()), builder);
  }

  Type &getType() override { return to; }

  IUSet getIUs() override { return child->getIUs(); }
};

template <typename T> struct ConstExp : public Exp {
  T value;
  Type type;
  ConstExp(T value)
      : value(value),
        type(p2c_type_mixin<T>::type_enum, typename p2c_type_mixin<T>::type()) {
  }

  Type &checkSemantics() override { return type; }
  Type &getType() override { return type; }

  ValueRef<> createEval(Builder &builder) override {
    return p2c_type_mixin<T>::type::createConstant(builder, value);
  }

  IUSet getIUs() override { return IUSet{}; }
  ~ConstExp() override = default;
};

template <> struct ConstExp<StringView> : public Exp {
  std::string value;
  Type type;
  ConstExp(std::string_view value)
      : value(value), type(p2c_type_mixin<StringView>::type_enum,
                           typename p2c_type_mixin<StringView>::type()) {}

  ValueRef<> createEval(Builder &builder) override {
    ValueRef<> cnst = p2c_type_mixin<StringView>::type::createConstant(
        builder, StringView(value.data(), value.size()));
    TypeRef<> svType = StringTy::createType(builder.getContext());
    ValueRef<> ptr = builder.createAlloca(svType);
    ValueRef<> sptr = builder.builder.CreateStructGEP(svType, ptr, 0);
    ValueRef<> slen = builder.builder.CreateStructGEP(svType, ptr, 1);
    builder.builder.CreateStore(cnst, sptr);
    builder.builder.CreateStore(
        llvm::ConstantInt::get(BigIntTy::createType(builder.getContext()),
                               value.length()),
        slen);
    return ptr;
  }
  Type &getType() override { return type; }
  Type &checkSemantics() override { return type; }

  IUSet getIUs() override { return IUSet{}; }

  ~ConstExp() override = default;
};

struct IUExp : public Exp {
  IU *iu;
  IUExp(IU *iu) : iu(iu) {}
  ValueRef<> createEval(Builder &builder) override {
    assert(builder.getCurrentScope().lookupValue(iu) &&
           "IU not found in current scope");
    return builder.createIULoad(iu);
  }

  Type &checkSemantics() override { return iu->type; }
  Type &getType() override { return iu->type; }
  IUSet getIUs() override { return IUSet{{iu}}; }
  ~IUExp() override = default;
};

struct LikeExp : public Exp {
  std::unique_ptr<Exp> value;
  std::string pattern;
  Type type;
  LikeExp(std::unique_ptr<Exp> &&value, std::string_view pattern)
      : value(std::move(value)), pattern(pattern),
        type(TypeEnum::Bool, BoolTy()) {}

  ValueRef<> createEval(Builder &builder) override {
    ValueRef<> val = value->createEval(builder);
    auto &context = builder.getContext();
    auto *module = builder.query.getModule();
    bool ends = pattern.ends_with("%");
    bool begin = pattern.starts_with("%");
    ValueRef<> strptr = StringTy::createConstant(
        builder, {pattern.data() + begin, pattern.length() - ends});
    std::string fname;
    void *fptr;
    if (begin && ends) {
      fname = "like";
      fptr = std::bit_cast<void *>(&like);
    } else if (ends) {
      fname = "like_prefix";
      fptr = std::bit_cast<void *>(&like_prefix);
    } else if (begin) {
        fname = "like_suffix";
        fptr = std::bit_cast<void*>(&like_suffix);
    }else{
        throw std::runtime_error("Unsupported like-Expression");
    }
    return builder.createCall(
        fname, fptr, BoolTy::createType(builder.getContext()), val, strptr,
        builder.getInt64Constant(pattern.length() - begin - ends));
  }

  Type &checkSemantics() override { return type; }
  Type &getType() override { return type; }
  IUSet getIUs() override { return value->getIUs(); }

  ~LikeExp() override = default;
};

template <size_t N> struct CaseExp : public Exp {
  std::array<std::unique_ptr<Exp>, N> conds, trueExps;
  std::unique_ptr<Exp> falseExp;
  Type type;

  CaseExp(std::array<std::unique_ptr<Exp>, N> &&cond,
          std::array<std::unique_ptr<Exp>, N> &&trueExp,
          std::unique_ptr<Exp> falseExp, TypeEnum type)
      : conds(std::move(cond)), trueExps(std::move(trueExp)),
        falseExp(std::move(falseExp)), type(Type::get(type)) {}

  ValueRef<> createEval(Builder &builder) override {
    auto &context = builder.getContext();
    BasicBlockRef cbb = builder.getInsertBlock();
    std::array<std::pair<ValueRef<>, llvm::BasicBlock *>, N + 1> vals;
    llvm::BranchInst *lastBranch = builder.builder.CreateBr(cbb);
    for (size_t i = 0; i < N; ++i) {
      BasicBlockRef caseCond = builder.createBasicBlock("caseCond");
      builder.builder.SetInsertPoint(caseCond);
      ValueRef<> cnd = conds[i]->createEval(builder);
      BasicBlockRef caseBody = builder.createBasicBlock("caseBody");
      lastBranch->setSuccessor(lastBranch->getNumSuccessors() - 1, caseCond);
      lastBranch = builder.builder.CreateCondBr(cnd, caseBody, nullptr);
      builder.builder.SetInsertPoint(caseBody);
      vals[i] = {trueExps[i]->createEval(builder), caseBody};
      builder.builder.CreateBr(caseBody);
    }
    BasicBlockRef falseBB = builder.createBasicBlock("false");
    lastBranch->setSuccessor(lastBranch->getNumSuccessors() - 1, falseBB);
    builder.builder.SetInsertPoint(falseBB);
    vals.back() = {falseExp->createEval(builder), falseBB};
    BasicBlockRef mergeBB = builder.createBasicBlock("merge");
    builder.builder.CreateBr(falseBB);
    builder.builder.SetInsertPoint(mergeBB);
    ValueRef<llvm::PHINode> phi =
        builder.builder.CreatePHI(vals.back().first->getType(), N + 1);
    for (auto [val, bb] : vals) {
      phi->addIncoming(val, bb);
      ValueRef<llvm::BranchInst> br =
          llvm::dyn_cast<llvm::BranchInst>(bb->getTerminator());
      br->setSuccessor(0, mergeBB);
    }
    return phi;
  }

  Type &checkSemantics() override {
    for (auto &exp : trueExps) {
      auto &t = exp->checkSemantics();
      if (checkPrecedence(t, type))
        exp = std::make_unique<CastExp>(t, type, std::move(exp));
    }
    auto& t = falseExp->checkSemantics();
    if(checkPrecedence(t, type))
        falseExp = std::make_unique<CastExp>(t, type, std::move(falseExp));
    return type;
  }

  Type &getType() override { return type; }

  IUSet getIUs() override {
    return conds[0]->getIUs() | trueExps[0]->getIUs() | falseExp->getIUs();
  }

  ~CaseExp() override = default;
};

struct UnaryExp : public Exp {
  UnOp op;
  std::unique_ptr<Exp> exp;

  UnaryExp(UnOp op, std::unique_ptr<Exp> &&exp) : op(op), exp(std::move(exp)) {}
  ValueRef<> createEval(Builder &builder) override {
    auto &type = exp->getType();
    return type.createUnOp(op, exp->createEval(builder), builder);
  }
  Type &checkSemantics() override { return exp->checkSemantics(); }
  Type& getType() override {return exp->getType(); }
  IUSet getIUs() override { return exp->getIUs(); }
  ~UnaryExp() override = default;
};

struct BinOpExp : public Exp {
  BinOp op;
  std::unique_ptr<Exp> lhs, rhs;
  BinOpExp(BinOp op, std::unique_ptr<Exp> &&lhs, std::unique_ptr<Exp> &&rhs)
      : op(op), lhs(std::move(lhs)), rhs(std::move(rhs)) {}
  ValueRef<> createEval(Builder &builder) override {
    auto &rt = lhs->getType();
    ValueRef<> lhsVal = lhs->createEval(builder);
    ValueRef<> rhsVal = rhs->createEval(builder);
    return rt.createBinOp(op, lhsVal, rhsVal, builder);
  }

  Type &checkSemantics() override {
    auto &left = lhs->checkSemantics();
    auto &right = rhs->checkSemantics();
    if (left.typeEnum == right.typeEnum)
      return left;
    if (checkPrecedence(left, right)) {
      lhs = std::make_unique<CastExp>(left, right, std::move(lhs));
      return right;
    } else {
      rhs = std::make_unique<CastExp>(right, left, std::move(rhs));
      return left;
    }
  }

  Type &getType() override { return lhs->getType(); }

  IUSet getIUs() override { return lhs->getIUs() | rhs->getIUs(); }
  ~BinOpExp() override = default;
};

struct TypePreservingBinOp : public BinOpExp {
  TypePreservingBinOp(BinOp op, std::unique_ptr<Exp> &&lhs,
                      std::unique_ptr<Exp> &&rhs)
      : BinOpExp(op, std::move(lhs), std::move(rhs)) {}
};

struct ShortCirCutBinOp : public BinOpExp {
  ShortCirCutBinOp(BinOp op, std::unique_ptr<Exp> &&lhs,
                   std::unique_ptr<Exp> &&rhs)
      : BinOpExp(op, std::move(lhs), std::move(rhs)) {}
  ValueRef<> createEval(Builder &builder) override {
    ValueRef<> lval = lhs->createEval(builder);
    auto required = getIUs();
    BasicBlockRef lvalBB = builder.builder.GetInsertBlock();
    BasicBlockRef bbRhs = builder.createBasicBlock("rhs");
    builder.builder.SetInsertPoint(bbRhs);
    ValueRef<> rval = rhs->createEval(builder);
    BasicBlockRef rvalBB = builder.builder.GetInsertBlock();
    BasicBlockRef merge = builder.createBasicBlock("merge");
    builder.builder.CreateBr(merge);
    builder.builder.SetInsertPoint(lvalBB);
    builder.builder.CreateCondBr(lval, op == BinOp::And ? bbRhs : merge,
                                 op == BinOp::And ? merge : bbRhs);
    builder.builder.SetInsertPoint(merge);
    ValueRef<llvm::PHINode> phi = builder.builder.CreatePHI(lval->getType(), 2);
    phi->addIncoming(lval, lvalBB);
    phi->addIncoming(rval, rvalBB);
    return phi;
  }
  ~ShortCirCutBinOp() = default;
};

struct NonTypePreservingBinOp : public BinOpExp {
  NonTypePreservingBinOp(BinOp op, std::unique_ptr<Exp> &&lhs,
                         std::unique_ptr<Exp> &&rhs)
      : BinOpExp(op, std::move(lhs), std::move(rhs)),
        res(Type::get(TypeEnum::Bool)) {}
  Type res;

  Type &checkSemantics() override {
    BinOpExp::checkSemantics();
    return res;
  }

  Type &getType() override { return res; }
};

}; // namespace p2cllvm
