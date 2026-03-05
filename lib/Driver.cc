#include "IR/Builder.h"
#include "IR/Pipeline.h"
#include "internal/Compiler.h"
#include "internal/QueryScheduler.h"
#include "internal/Database.h"
#include "internal/Config.h"
#include "internal/Schema.h"
#include "internal/CloudManager.h"
#include "operators/Driver.h"
#include "operators/Iu.h"
#include "operators/Operator.h"
#include "provided/perfevent.hpp"
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <memory>
#include <string>
#include <iostream>

namespace p2cllvm {

// Global bootstrapping function
void initialize() {
    static bool initialized = false;
    if (initialized) return;

    // std::cout << "[Driver] Bootstrapping Engine..." << std::endl;

    // load global config singleton
    std::string config_path = std::getenv("config") ? std::getenv("config") : "../config.json";
    Config::load(config_path);
    const auto& cfg = Config::get();

    // set max blockpool size
    if (cfg.blockpool_size_gb > 0) {
      size_t bytes = static_cast<size_t>(cfg.blockpool_size_gb) * 1024 * 1024 * 1024;
      BlockPool::get().setMaxPoolSize(bytes);
    }

    AnyBlobManager::connect(Config::get());

    std::string schema_path = Config::get().dataset_path + "/tpch_schema.json";
    try {
        SchemaManager::get().loadFromCloud(schema_path);
    } catch (const std::exception& e) {
        llvm::errs() << "Critical Error: Could not load schema from " << schema_path << ": " << e.what() << "\n";
        exit(1);
    }

    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    
    initialized = true;
    // std::cout << "[Driver] Engine Initialized Successfully." << std::endl;
}

static void produce_impl(Database &db, std::unique_ptr<Operator> &op,
                         std::vector<IU *> &outputs,
                         std::vector<std::string> &names,
                         std::unique_ptr<Sink> &sink,
                         bool use_async,
                         const Config& cfg) {
  Query query(db);
  auto builder = Builder(query);
  sink->produce(op, outputs, names, builder);
  QueryCompiler compiler;
  compiler.createJIT();
  compiler.addQuery(std::move(builder.query), builder.query.context);
  compiler.addSymbols(builder.query.symbolManager);
  
  size_t nthreads = std::thread::hardware_concurrency();
  if (use_async) {
    auto async_scheduler = std::make_unique<AsyncParquetScheduler>(db, cfg, nthreads);
    for (const auto &pipeline : builder.query.pipelines) {
      async_scheduler->execPipeline(*pipeline, compiler);
    }
  } else {
    auto sync_scheduler = std::make_unique<SyncParquetScheduler>(db, nthreads);
    for (const auto &pipeline : builder.query.pipelines) {
      sync_scheduler->execPipeline(*pipeline, compiler);
    }
  }
}

void produce(std::unique_ptr<Operator> op, std::vector<IU *> outputs,
             std::vector<std::string> names, std::unique_ptr<Sink> sink) {
  
  // ensure bootstrapped
  initialize();
  
  Config cfg = Config::get();
  Database db(cfg);
  
  bool use_async = std::getenv("use_async") ? std::atoi(std::getenv("use_async")) : false;
  uint32_t runs = std::getenv("runs") ? std::atoi(std::getenv("runs")) : 3;
  
  const size_t tuple_count = db.get_total_tuple_count("lineitem");
  const size_t scale_factor = tuple_count / 6'000'000;

  PerfEvent perf;
  BenchmarkParameters params;
  params.setParam("task", "codegen");
  params.setParam("workload", "SF" + std::to_string(scale_factor));
  params.setParam("impl", "p2cllvm");
  params.setParam("variant", use_async ? "async-parquet" : "sync-parquet");
  
  for (uint32_t run = 0; run < runs; ++run) {
    uint64_t tuple_count = db.get_total_tuple_count("lineitem");
    PerfEventBlock perfBlock(perf, tuple_count, params, run == 0);
    produce_impl(db, op, outputs, names, sink, use_async, cfg);
  }
}
} // namespace p2cllvm