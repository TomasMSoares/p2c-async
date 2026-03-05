#pragma once
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/LLVMContext.h"

#include "IR/Defs.h"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Module.h>
#include <type_traits>

namespace p2cllvm {

template <typename Fn>
  requires std::same_as<std::invoke_result_t<Fn>, llvm::StructType *>
TypeRef<llvm::StructType> getOrCreateType(llvm::LLVMContext &context,
                                  llvm::StringRef type, Fn create) {
  auto *typePtr = llvm::StructType::getTypeByName(context, type);
  if (typePtr)
    return typePtr;
  return create();
}

struct Date {
  using date = uint32_t;
  static void fromInt(unsigned date, unsigned &year, unsigned &month,
                      unsigned &day) {
    unsigned a = date + 32044;
    unsigned b = (4 * a + 3) / 146097;
    unsigned c = a - ((146097 * b) / 4);
    unsigned d = (4 * c + 3) / 1461;
    unsigned e = c - ((1461 * d) / 4);
    unsigned m = (5 * e + 2) / 153;

    day = e - ((153 * m + 2) / 5) + 1;
    month = m + 3 - (12 * (m / 10));
    year = (100 * b) + d - 4800 + (m / 10);
  }

  // Julian Day Algorithm from the Calendar FAQ
  static unsigned toInt(unsigned year, unsigned month, unsigned day) {
    unsigned a = (14 - month) / 12;
    unsigned y = year + 4800 - a;
    unsigned m = month + (12 * a) - 3;
    return day + ((153 * m + 2) / 5) + (365 * y) + (y / 4) - (y / 100) +
           (y / 400) - 32045;
  }

  static unsigned extractYear(date date) {
    unsigned year, month, day;
    fromInt(date, year, month, day);
    return year;
  }
};

struct String {
  struct StringData {
    size_t length;
    size_t offset;
    static TypeRef<llvm::StructType> createType(llvm::LLVMContext &context) {
      return getOrCreateType(context, "StringData", [&]() {
        return llvm::StructType::create(
            context,
            {llvm::Type::getInt64Ty(context), llvm::Type::getInt64Ty(context)},
            "StringData");
      });
    }
  };

  uint64_t count;
  StringData slot[];
  static TypeRef<llvm::StructType> createType(llvm::LLVMContext &context) {
    return getOrCreateType(context, "String", [&]() {
      return llvm::StructType::create(
          context,
          {llvm::Type::getInt64Ty(context), StringData::createType(context)},
          "String");
    });
  }
};

struct StringView {
  char *data;
  size_t length;

  static TypeRef<llvm::StructType> createType(llvm::LLVMContext &context) {
    return getOrCreateType(context, "StringView", [&]() {
      return llvm::StructType::create(
          context,
          {llvm::PointerType::get(context, 0), llvm::Type::getInt64Ty(context)},
          "StringView");
    });
  }
};

enum class TypeEnum : uint8_t {
  Integer = 0,
  Double = 1,
  Char = 2,
  String = 3,
  BigInt = 4,
  Bool = 5,
  Date = 6,
  Undefined = 7
};

constexpr std::array<std::string, 8> typeNames = {
    "Integer", "Double", "Char", "String",   "BigInt",
    "Bool",    "Date",   "Undefined"};

constexpr std::array<uint8_t, 8> decayTypes = {
    2, // Integer
    4, // Double
    1, // Char
    0, // String
    3, // BigInt
    1, // Bool
    2, // Date
    7  // Undefined
};

constexpr std::array<size_t, 8> typeSizes = {
    sizeof(int32_t),    // Integer
    sizeof(double),     // Double
    sizeof(char),       // Char
    sizeof(StringView), // String
    sizeof(int64_t),    // BigInt
    0,                  // Bool
    sizeof(Date),       // Date
    0                   // Undefined
};

constexpr std::array<size_t, 9> typeAlignments = {
    alignof(int32_t),    // Integer
    alignof(double),     // Double
    alignof(char),       // Char
    alignof(StringView), // String
    alignof(int64_t),    // BigInt
    0,                   // Bool
    alignof(Date),       // Date
    0                    // Undefined
};

constexpr inline size_t align(size_t size) {
  return (size + sizeof(void *) - 1) & ~(sizeof(void *) - 1);
}
}; // namespace p2cllvm
