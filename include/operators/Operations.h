#pragma once
#include "IR/Binop.h"
#include "IR/Unop.h"
#include "IR/Expression.h"
#include <concepts>
#include <memory.h>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace p2cllvm {

template <typename... Args>
std::unique_ptr<BinOpExp> makeBinOpExp(BinOp op, Args &&...args) {
  switch (op) {
  case BinOp::Or:
  case BinOp::And:
      return std::make_unique<ShortCirCutBinOp>(op, std::forward<Args>(args)...);
  case BinOp::Add:
  case BinOp::Sub:
  case BinOp::Div:
  case BinOp::Mul:
    return std::make_unique<TypePreservingBinOp>(op,
                                                 std::forward<Args>(args)...);
  case BinOp::CMPEQ:
  case BinOp::CMPGE:
  case BinOp::CMPGT:
  case BinOp::CMPLE:
  case BinOp::CMPLT:
  case BinOp::CMPNE:
    return std::make_unique<NonTypePreservingBinOp>(
        op, std::forward<Args>(args)...);
  default:
    throw std::runtime_error("Unknown BinOp");
  }
}
template <typename E, typename... Args>
std::unique_ptr<E> makeExp(Args &&...args)
  requires std::constructible_from<E, Args...>
{
  if constexpr (std::is_same_v<E, BinOpExp>)
    return makeBinOpExp<E>(std::forward<Args>(args)...);
  else
    return std::make_unique<E>(std::forward<Args>(args)...);
}

template <typename... Args>
  requires(sizeof...(Args) > 1)
std::unique_ptr<Exp> makeCallExp(const std::string_view fname,
                                      Args &&...args) {
  const std::unordered_map<std::string_view, BinOp> mapping{
      {"std::plus()", BinOp::Add},
      {"std::minus()", BinOp::Sub},
      {"std::multiplies()", BinOp::Mul},
      {"std::divides()", BinOp::Div},
      {"std::equal_to()", BinOp::CMPEQ},
      {"equal_to()", BinOp::CMPEQ},
      {"std::less_equal()", BinOp::CMPLE},
      {"less_equal()", BinOp::CMPLE},
      {"std::greater_equal()", BinOp::CMPGE},
      {"greater_equal()", BinOp::CMPGE},
      {"std::less()", BinOp::CMPLT},
      {"std::greater()", BinOp::CMPGT},
      {"std::logical_or()", BinOp::Or},
      {"logical_or()", BinOp::Or},
      {"logical_and()", BinOp::And},
      {"std::logical_and()", BinOp::And}};
  const auto it = mapping.find(fname);
  if (it == mapping.end())
    throw std::runtime_error(std::string(fname));
  return makeBinOpExp(it->second, std::forward<Args>(args)...);
}

template <typename... Args>
  requires(sizeof...(Args) == 1)
std::unique_ptr<Exp> makeCallExp(const std::string_view fname,
                                      Args &&...args) {
  const std::unordered_map<std::string_view, UnOp> mapping{
      {"std::logical_not()", UnOp::NOT}, {"logical_not()", UnOp::NOT}, 
        {"date::extractYear", UnOp::EXTRACT_YEAR}};
  const auto it = mapping.find(fname);
  if (it == mapping.end()) 
      throw std::runtime_error("No unary" + std::string(fname)); 

  return std::make_unique<UnaryExp>(it->second, std::forward<Args>(args)...);
}
} // namespace p2cllvm
