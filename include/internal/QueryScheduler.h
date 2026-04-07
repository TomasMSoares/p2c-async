#pragma once
#include "IR/Pipeline.h"
#include "internal/Compiler.h"
#include "internal/Database.h"
#include "internal/Schema.h"
#include "internal/ParquetTable.h"
#include "internal/Config.h"

#include <concurrentqueue.h>
#include <blockingconcurrentqueue.h>

#include <memory>
#include <atomic>
#include <thread>
#include <vector>
#include <fstream>

// FORWARD DECLARATIONS
namespace anyblob {
    namespace network {
        class Transaction;
        class TaskedSendReceiverHandle;
    }
}

namespace p2cllvm {

// forward declaration
class QueryCompiler;

template <typename I> 
class QueryScheduler {
public:
    virtual ~QueryScheduler() = default;
    QueryScheduler(Database &db) : db(db) {}

    void execPipeline(Pipeline &pipeline, QueryCompiler &qc) {
        switch (pipeline.type) {
            case PipelineType::Default:
                static_cast<I &>(*this).execPipelineImpl(pipeline, qc);
                break;
            case PipelineType::Scan:
                static_cast<I &>(*this).execScanPipelineImpl(
                    static_cast<ScanPipeline &>(pipeline), qc);
                break;
            case PipelineType::Continuation:
                static_cast<I &>(*this).execContinuationPipelineImpl(pipeline, qc);
                break;
            }
    }

protected:
    Database &db;

    void execPipelineImpl(Pipeline& pipeline, QueryCompiler& qc) {
        auto fn = qc.getPipelineFunction(pipeline.name);
        if (!fn) llvm::report_fatal_error(fn.takeError());
        auto* fptr = (*fn).toPtr<void (*)(void**)>();
        fptr(pipeline.args.data());
    }

    // common logic to build column layout for the JIT kernel
    std::vector<std::string> prepareLayout(ParquetTable* table) {
        const auto& schema = SchemaManager::get().getSchema(table->getTableName());
        int max_idx = 0;
        for (const auto& col : schema.columns) max_idx = std::max(max_idx, col.index);

        std::vector<std::string> layout_map(max_idx + 1);
        for (const auto& col : schema.columns) {
            layout_map[col.index] = col.name;
            table->registerColumn(col.name);
        }
        return layout_map;
    }

    // common logic to map column data pointers to the JIT pointer array
    void populateColumnPtrs(ParquetTable* table, const std::vector<std::string>& layout, std::vector<void*>& ptrs) {
        for (size_t c = 0; c < layout.size(); ++c) {
            if(!layout[c].empty()) ptrs[c] = table->getColumnData(layout[c]);
        }
    }
};


class SyncParquetScheduler : public QueryScheduler<SyncParquetScheduler> {
public:
    SyncParquetScheduler(Database& db, size_t nthreads = std::thread::hardware_concurrency());
    void execScanPipelineImpl(ScanPipeline& pipeline, QueryCompiler& qc);
    void execContinuationPipelineImpl(Pipeline& p, QueryCompiler& qc) { execPipelineImpl(p, qc); }
private:
    size_t nthreads;
};


// signature: (column_ptrs, start_row, end_row, thread_id, pipeline_args)
using ScanFn = void (*)(void**, uint64_t, uint64_t, uint64_t, void**);

class AsyncParquetScheduler : public QueryScheduler<AsyncParquetScheduler> {
public:
    AsyncParquetScheduler(Database& db, const Config& cfg, size_t nthreads = std::thread::hardware_concurrency());
    ~AsyncParquetScheduler();

    void execScanPipelineImpl(ScanPipeline& pipeline, QueryCompiler& qc);
    void execContinuationPipelineImpl(Pipeline& p, QueryCompiler& qc) { execPipelineImpl(p, qc); }

private:
    enum class JobType { PREPARATION, RETRIEVAL, PROCESSING };

    struct EpochStats {
        std::atomic<size_t> retrieved_bytes{0};
        std::atomic<uint64_t> retrieval_time_ns{0};
        std::atomic<size_t> processed_bytes{0};
        std::atomic<uint64_t> processed_time_ns{0};
        struct Snapshot { size_t retrieved_bytes; size_t processed_bytes; uint64_t processed_time_ns; };
        Snapshot takeSnapshot();
    };

    struct SchedulerState {
        // targets set by main thread
        alignas(64) std::atomic<size_t> target_retriever_threads{0};
        alignas(64) std::atomic<size_t> max_outstanding_bytes{0}; 
        
        // live counters used by workers
        alignas(64) std::atomic<size_t> active_preparing{0};
        alignas(64) std::atomic<size_t> active_retrieving{0};
        alignas(64) std::atomic<size_t> active_processing{0};
        alignas(64) std::atomic<size_t> outstanding_bytes{0};
    } sched_state;

    void updateSchedulerStats();
    void prepareOneBlock(anyblob::network::Transaction& txn, ParquetTable::DownloadCallback cb);
    void processOneMorsel(ScanFn kernel_ptr, const std::vector<std::string>& layout_map, void** pipeline_args, size_t thread_id, std::shared_ptr<anyblob::network::TaskedSendReceiverHandle> io_handle);
    void driveIOHandle(std::shared_ptr<anyblob::network::TaskedSendReceiverHandle> handle);
    JobType pickJob();
    void log(size_t thread_id, const std::string& msg);

    size_t nthreads;
    ParquetTable* current_table = nullptr;
    const size_t EPOCH_DURATION_MS;
    const size_t WARM_UP_TIME_MS;
    const size_t MAX_RETRIEVERS;
    const double DAMPING_FACTOR;
    const double EMA_ALPHA;
    const double REASSIGNMENT_THRESHOLD;
    const double MAX_NETWORK_BANDWIDTH;
    const size_t MAX_PREPARED_BYTES;

    size_t total_chunks;
    std::atomic<size_t> chunks_prepared{0};
    std::atomic<size_t> chunks_downloaded{0};
    std::atomic<size_t> chunks_processed{0};
    std::atomic<size_t> chunk_misses{0};

    std::atomic<bool> stop_requested{false};
    moodycamel::BlockingConcurrentQueue<std::unique_ptr<FetchedChunk>> ready_queue;

    size_t epoch_count{0};
    double smoothed_ratio{0.0};
    EpochStats stats;

    std::ofstream debug_log;
    std::mutex log_mu;
};

} // namespace p2cllvm