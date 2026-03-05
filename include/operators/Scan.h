#pragma once

#include "IR/Builder.h"
#include "IR/ColTypes.h"
#include "IR/Defs.h"
#include "IR/Types.h"
#include "internal/Database.h"
#include "internal/Schema.h"
#include "Operator.h"

#include <cassert>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Type.h>
#include <llvm/Support/raw_ostream.h>

namespace p2cllvm {
class Scan : public Operator {
public:
  void produce(IUSet &required, Builder &builder, ConsumerFn consumer,
               InitFn fn) override {
    // Get table index and schema from metadata
    size_t table_idx = builder.query.dbref.getTableIndex(table_name);
    const auto& schema = SchemaManager::get().getSchema(table_name);
    
    auto &context = builder.getContext();
    
    builder.createScanPipeline(table_idx);
    auto &scope = builder.getCurrentScope();
    ValueRef<llvm::Function> fun = scope.pipeline;
    assert(builder.builder.GetInsertBlock());
    
    ValueRef<> column_array = fun->getArg(0);  // void** (array of column pointers)
    ValueRef<> begin = fun->getArg(1);
    ValueRef<> end = fun->getArg(2);
    fn(builder);
    
    std::vector<ValueRef<>> cols;
    cols.reserve(attributes.size());
    
    for (auto &col : required) {
      const auto& col_def = schema.getColumn(col->name);
      int idx = col_def.index;
      
      // Get pointer to column_array[idx] -> void**[idx]
      ValueRef<> col_ptr_addr = builder.builder.CreateConstInBoundsGEP1_64(
          builder.builder.getPtrTy(), column_array, idx);
      
      // Load the void* at column_array[idx]
      ValueRef<> col_data_ptr = builder.builder.CreateLoad(
          builder.builder.getPtrTy(), col_ptr_addr);
      
      cols.push_back(col_data_ptr);
      scope.updatePtr(col, cols.back());
    }
    
    ValueRef<llvm::PHINode> iterphi = builder.createBeginIndexIter(begin, end);
    size_t i = 0;
    for (auto &col : required) {
      builder.createColumnAccess(iterphi, cols[i++], col);
    }
    consumer(builder);
    builder.createEndIndexIter();
  }

  Scan(std::string_view table_name) : table_name(table_name) {
    // Get schema from SchemaManager
    const auto& schema = SchemaManager::get().getSchema(table_name);
    attributes.reserve(schema.columns.size());
    for (const auto& col : schema.columns) {
      attributes.emplace_back(col.name, col.type);
    }
  }

  IU *getIU(std::string_view name) {
    for (auto &attr : attributes) {
      if (attr.name == name) {
        return &attr;
      }
    }
    return nullptr;
  }

  IUSet availableIUs() override {
    IUSet iuSet;
    for (auto &attr : attributes) {
      iuSet.add(&attr);
    }
    return iuSet;
  }

private:
  std::string_view table_name;
  std::vector<IU> attributes;
};

}; // namespace p2cllvm
