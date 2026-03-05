#pragma once

#include "IR/Types.h"
#include "internal/basetypes.h"
#include "operators/OperatorContext.h"
#include <cassert>
#include <charconv>
#include <cstdlib>
#include <source_location>
#include <string>
#include <type_traits>
#include <vector>
namespace p2cllvm { 

#define assertCond(cond)                                                       \
  do {                                                                         \
    std::source_location cur = std::source_location::current();                \
    if (!(cond)) {                                                             \
      std::cerr << cur.file_name() << ":" << cur.line() << " "                 \
                << cur.function_name() << " " << #cond << "\n";                \
      std::exit(EXIT_FAILURE);                                                 \
    }                                                                          \
  } while (0);
    static size_t maxLineItemAssertLen = 6001215;

bool operator==(const StringView &lhs, const std::string &rhs);

template <typename T>
  requires std::is_integral_v<typename T::value_type>
constexpr void compare(const typename T::value_type &lhs,
                       const typename T::value_type &rhs) {
  assertCond(lhs == rhs);
}

template <typename T>
  requires std::is_floating_point_v<typename T::value_type>
constexpr void compare(const typename T::value_type &lhs,
                       const typename T::value_type &rhs) {
  assertCond(lhs > rhs ? (lhs / rhs < 1.01) : (rhs / lhs < 1.01));
}

constexpr void compareLength(size_t exp, AssertContext *ctx) {
  assertCond(ctx->i == exp);
}

template <typename T>
  requires std::is_arithmetic_v<typename T::value_type>
void compareFromString(const typename T::arg_type lhs, AssertContext *expIter) {
  typename T::value_type rhs;
  auto &exp = expIter->iter[expIter->i];
  if constexpr (!std::is_same_v<char, typename T::value_type>) {
    auto [_, ec] = std::from_chars(exp.data(), exp.data() + exp.size(), rhs);
    assert(ec == std::errc());
  } else {
    rhs = exp.front();
  }
  ++expIter->i;
  compare<T>(lhs, rhs);
}

template <typename T>
  requires std::is_same_v<typename T::value_type, StringView>
void compareFromString(const typename T::arg_type lhs, AssertContext *expIter) {
  assertCond(*lhs == expIter->iter[expIter->i++]);
}
template void compareFromString<BigIntTy>(const BigIntTy::arg_type lhs,
                                          AssertContext *exp);
template void compareFromString<DoubleTy>(const DoubleTy::arg_type lhs,
                                          AssertContext *exp);
template void compareFromString<IntegerTy>(const IntegerTy::arg_type lhs,
                                           AssertContext *expIter);
template void compareFromString<CharTy>(const CharTy::arg_type lhs,
                                        AssertContext *expIter);
template void compareFromString<DateTy>(const DateTy::arg_type lhs,
                                        AssertContext *expIter);
template void compareFromString<StringTy>(const StringTy::arg_type lhs,
                                          AssertContext *expIter);

bool skipTooLong(AssertLengthContext* ctx);
} // namespace p2cllvm
