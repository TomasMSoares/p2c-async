#pragma once

#include "IR/Defs.h"
#include "IR/Types.h"
#include "internal/basetypes.h"
#include <cstdint>
#include <cassert>
#include <cstddef>
#include <cmath>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Type.h>

namespace p2cllvm {
struct Sketch {
  static constexpr uint64_t REG_CNT = 128; // 1 << 7
  static constexpr uint64_t REST_SHIFT = 7;
  static constexpr uint64_t REG_SHIFT = 64 - REST_SHIFT;
  uint8_t registers[REG_CNT] = {0};

  void add(uint64_t hash) {
    auto &value = registers[hash >> REG_SHIFT];
    uint64_t rest = hash << REST_SHIFT;
    uint8_t rank = rest == 0 ? REG_SHIFT : (std::countl_zero(rest) + 1);
    value = std::max(value, rank);
    assert(value > 0);
  }

  void merge(const Sketch &o) {
    for (size_t i = 0; i < REG_CNT; ++i) {
      registers[i] = std::max(registers[i], o.registers[i]);
    }
  }

  uint64_t estimate() {
    double sum = 0;
    unsigned zero_cnt = 0;
    for (auto reg : registers) {
      zero_cnt += reg == 0;
      sum += 1.0 / (1ul << reg);
    }
    double est = 0.709 * REG_CNT * REG_CNT / sum;
    if (zero_cnt == 0) {
        return static_cast<uint64_t>(est);
      } else {
          return REG_CNT * std::log(static_cast<double>(REG_CNT) / zero_cnt);
    } 
  }

  static TypeRef<llvm::StructType> createType(llvm::LLVMContext& context) {
      return getOrCreateType(context, "Sketch", [&]() {
              return llvm::StructType::create(context, {llvm::ArrayType::get(CharTy::createType(context), REG_CNT)}, "Sketch");
              });
  }
};

}; // namespace p2cllvm
