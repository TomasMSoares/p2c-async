#pragma once
#include "IR/Pipeline.h"
#include "internal/Compiler.h"
#include "internal/Database.h"
#include "internal/ParquetTable.h"
#include "internal/Schema.h"
#include "internal/Config.h"
#include <llvm/Support/Error.h>
#include <cstdint>
#include <tuple>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <iostream>
#include <deque>
#include <chrono>
#include <fstream>
#include <cmath>
#include <parquet/properties.h>
#include <concurrentqueue.h>
#include <blockingconcurrentqueue.h>

namespace p2cllvm {

template <typename I> class QueryScheduler {
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
    SyncParquetScheduler(Database& db, size_t nthreads = std::thread::hardware_concurrency())
        : QueryScheduler(db), nthreads(nthreads) {}

    // standard pipelines (non-scan) run normally on the main thread
    void execPipelineImpl(Pipeline& pipeline, QueryCompiler& qc) {
        auto fn = qc.getPipelineFunction(pipeline.name);
        if (!fn) llvm::report_fatal_error(fn.takeError());
        auto* fptr = (*fn).toPtr<void (*)(void**)>();
        fptr(pipeline.args.data());
    }

    void execScanPipelineImpl(ScanPipeline& pipeline, QueryCompiler& qc) {
        auto fn = qc.getPipelineFunction(pipeline.name);
        if (!fn) llvm::report_fatal_error(fn.takeError());

        // signature: (column_ptrs, start_row, end_row, thread_id, pipeline_args)
        using ScanFn = void (*)(void**, uint64_t, uint64_t, uint64_t, void**);
        auto* kernel_ptr = (*fn).toPtr<ScanFn>();

        // Extract table from pipeline
        auto tableIdx = pipeline.tableIndex;
        auto* table = dynamic_cast<ParquetTable*>(db.getTable(tableIdx));
        if (!table) {
            llvm::report_fatal_error("SyncParquetScheduler requires ParquetTable");
        }
        
        auto layout_map = prepareLayout(table);

        // reset table state
        table->resetChunks();
        std::atomic<int> next_chunk_idx{0};
        int total_chunks = table->getTotalChunks();
        std::vector<std::thread> threads;

        // launch worker threads
        for (size_t i = 0; i < nthreads; ++i) {
            threads.emplace_back([&, i]() {
                while (true) {
                    int chunk_idx = next_chunk_idx.fetch_add(1);
                    if (chunk_idx >= total_chunks) break;

                    auto fetched_chunk = table->fetchChunkSync(chunk_idx);
                    
                    if (!fetched_chunk) {
                        llvm::errs() << "Error: Failed to fetch chunk " << chunk_idx << "\n";
                        continue; 
                    }

                    auto decompressed = table->decompressChunk(std::move(fetched_chunk));
                    
                    table->setThreadLocalChunk(decompressed);

                    // build Pointer Array for JIT Code
                    // The JIT code expects array[i] to be the pointer for column with schema index i
                    std::vector<void*> column_ptrs(layout_map.size(), nullptr);
                    populateColumnPtrs(table, layout_map, column_ptrs);

                    // exec kernel
                    kernel_ptr(column_ptrs.data(), 0, decompressed->row_count, i, pipeline.args.data());
                }
            });
        }
        for (auto& t : threads) t.join();
    }
    
    void execContinuationPipelineImpl(Pipeline& p, QueryCompiler& qc) { execPipelineImpl(p, qc); }

private:
    size_t nthreads;
};


// signature: (column_ptrs, start_row, end_row, thread_id, pipeline_args)
using ScanFn = void (*)(void**, uint64_t, uint64_t, uint64_t, void**);

class AsyncParquetScheduler : public QueryScheduler<AsyncParquetScheduler> {
public:
    AsyncParquetScheduler(Database& db, const Config& cfg, size_t nthreads = std::thread::hardware_concurrency()) :
        QueryScheduler(db),
        nthreads(nthreads),

        EPOCH_DURATION_MS(cfg.epoch_duration_ms),
        WARM_UP_TIME_MS(cfg.warm_up_time_ms),
        MAX_RETRIEVERS(static_cast<size_t>(cfg.max_retriever_proportion * nthreads)),
        DAMPING_FACTOR(cfg.damping_factor),
        EMA_ALPHA(cfg.ema_alpha),
        REASSIGNMENT_THRESHOLD(cfg.reassignment_threshold),
        MAX_NETWORK_BANDWIDTH(cfg.max_network_bandwidth_gb * 1024.0 * 1024.0 * 1024.0),
        MAX_PREPARED_BYTES(cfg.max_prepared_mb * 1024.0 * 1024.0)
    {
        sched_state.max_outstanding_bytes.store(MAX_PREPARED_BYTES, std::memory_order_relaxed);
        debug_log.open("scheduler_debug.log", std::ios::out | std::ios::trunc);
    }

    ~AsyncParquetScheduler() {
        if (debug_log.is_open()) debug_log.close();
    }

    void log(size_t thread_id, const std::string& msg) {
        std::lock_guard<std::mutex> lock(log_mu);
        if (debug_log.is_open()) {
            debug_log << "[Thread " << thread_id << "] " << msg << std::endl;
        }
    }

#ifndef NDEBUG
#define SCHED_LOG(id, msg) log(id, msg)
#else
#define SCHED_LOG(id, msg) ((void)0)
#endif


    // Jobs for the scheduler's threads
    enum class JobType { PREPARATION, RETRIEVAL, PROCESSING };

    struct EpochStats {
        std::atomic<size_t> retrieved_bytes{0};
        std::atomic<uint64_t> retrieval_time_ns{0};
        std::atomic<size_t> processed_bytes{0};
        std::atomic<uint64_t> processed_time_ns{0};

        struct Snapshot {
            size_t retrieved_bytes;
            size_t processed_bytes;
            uint64_t processed_time_ns;
        };

        Snapshot takeSnapshot() {
            return Snapshot{
                retrieved_bytes.exchange(0, std::memory_order_relaxed),
                processed_bytes.exchange(0, std::memory_order_relaxed),
                processed_time_ns.exchange(0, std::memory_order_relaxed)
            };
        }
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


    // standard pipelines (non-scan) run normally on the main thread
    void execPipelineImpl(Pipeline& pipeline, QueryCompiler& qc) {
        auto fn = qc.getPipelineFunction(pipeline.name);
        if (!fn) llvm::report_fatal_error(fn.takeError());
        auto* fptr = (*fn).toPtr<void (*)(void**)>();
        fptr(pipeline.args.data());
    }

    void execContinuationPipelineImpl(Pipeline& p, QueryCompiler& qc) { execPipelineImpl(p, qc); }

    void execScanPipelineImpl(ScanPipeline& pipeline, QueryCompiler& qc) {
        auto fn = qc.getPipelineFunction(pipeline.name);
        if (!fn) llvm::report_fatal_error(fn.takeError());

        auto* kernel_ptr = (*fn).toPtr<ScanFn>();

        // Extract table from pipeline
        auto tableIdx = pipeline.tableIndex;
        current_table = dynamic_cast<ParquetTable*>(db.getTable(tableIdx));
        if (!current_table) {
            llvm::report_fatal_error("SyncParquetScheduler requires ParquetTable");
        }

        // reset table state
        auto layout_map = prepareLayout(current_table);
        current_table->resetChunks();

        total_chunks = current_table->getTotalChunks();
        chunks_prepared = 0;
        chunks_downloaded = 0;
        chunks_processed = 0;

        stop_requested = false;
        epoch_count = 0;
        smoothed_ratio = 0.0;
        
        // Drain moodycamel queue
        std::unique_ptr<FetchedChunk> dummy;
        while (ready_queue.try_dequeue(dummy)) {}

        // Reset state for new query;
        sched_state.target_retriever_threads = std::max(
            size_t(1), static_cast<size_t>(0.5 * MAX_RETRIEVERS)
        );
        sched_state.active_preparing = 0;
        sched_state.active_retrieving = 0;
        sched_state.active_processing = 0;
        sched_state.outstanding_bytes = 0;
        stats.takeSnapshot(); // clear stats

        // main thread creates io handles for each thread to use individually
        std::vector<std::shared_ptr<anyblob::network::TaskedSendReceiverHandle>> handles;
        for (size_t i = 0; i < nthreads; ++i) {
            handles.push_back(AnyBlobManager::getInstance().createHandle());
        }

        SCHED_LOG(999, "####################################\nStarting Scan: Total Chunks " + std::to_string(total_chunks));

        std::vector<std::thread> workers;

        for (size_t i = 0; i < nthreads; ++i) {
            workers.emplace_back([&, i, io_handle = handles[i]] () { // thread function
                anyblob::network::Transaction thread_txn(AnyBlobManager::getInstance().getProvider());
                
                // callback marks column/block as ready when complete
                auto completion_cb = [this, i, io_handle](std::unique_ptr<FetchedChunk> chunk) {
                    size_t chunk_id = chunk->row_group_id;
                    size_t chunk_size_bytes = chunk->size;
                    
                    ready_queue.enqueue(std::move(chunk));

                    sched_state.outstanding_bytes.fetch_sub(chunk_size_bytes, std::memory_order_relaxed);
                    stats.retrieved_bytes.fetch_add(chunk_size_bytes, std::memory_order_relaxed);
                    chunks_downloaded.fetch_add(1, std::memory_order_release);
                    
                    io_handle->stop(); // force IO loop to return to pickJob() after chunk is processed
                };
                
                // main loop
                size_t iteration = 0;
                while (true) {
                    if (chunks_processed.load(std::memory_order_relaxed) >= total_chunks) {
                        break;
                    }

                    JobType current_job = pickJob();

                    switch (current_job) {
                        case JobType::PREPARATION:
                            sched_state.active_preparing.fetch_add(1, std::memory_order_relaxed);
                            prepareOneBlock(thread_txn, completion_cb);
                            sched_state.active_preparing.fetch_sub(1, std::memory_order_relaxed);
                            break;
                        
                        case JobType::RETRIEVAL:
                            sched_state.active_retrieving.fetch_add(1, std::memory_order_relaxed);
                            driveIOHandle(io_handle);
                            sched_state.active_retrieving.fetch_sub(1, std::memory_order_relaxed);
                            break;
                        
                        case JobType::PROCESSING: {
                            sched_state.active_processing.fetch_add(1, std::memory_order_relaxed);
                            processOneMorsel(kernel_ptr, layout_map, pipeline.args.data(), i, io_handle);
                            sched_state.active_processing.fetch_sub(1, std::memory_order_relaxed);
                            break;
                        }
                    }
                    iteration++;
                }
            });
        }

        auto start_time = std::chrono::steady_clock::now();
        while (chunks_processed.load(std::memory_order_relaxed) < total_chunks) {
            std::this_thread::sleep_for(std::chrono::milliseconds(EPOCH_DURATION_MS));
            
            auto end_time = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

            // warmup should allow for some processing to happen
            if (elapsed > WARM_UP_TIME_MS) {
                updateSchedulerStats();
            }
        }

        stop_requested = true;

        // join threads
        for (auto& t : workers) {
            if (t.joinable()) t.join();
        }
        updateSchedulerStats();
        SCHED_LOG(999, "Final Stats: Processed " + std::to_string(chunks_processed.load()) + "/" + std::to_string(total_chunks)
            + ", chunk misses: " + std::to_string(chunk_misses.load()));
        SCHED_LOG(999, "All workers joined. Query Finished.\n");
    }



private:
    size_t nthreads;
    ParquetTable* current_table;

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
    
    size_t epoch_count{0};
    std::atomic<bool> stop_requested{false};
    std::mutex ready_mu;
    // moodycamel::ConcurrentQueue<std::unique_ptr<FetchedChunk>> ready_queue;
    moodycamel::BlockingConcurrentQueue<std::unique_ptr<FetchedChunk>> ready_queue;

    double smoothed_ratio{0.0};
    EpochStats stats;
    
    std::ofstream debug_log;
    std::mutex log_mu;

    void updateSchedulerStats() {
        EpochStats::Snapshot snap = stats.takeSnapshot();
        if (snap.processed_time_ns == 0 || snap.retrieved_bytes == 0) {
            return;
        }

        epoch_count++;

        double epoch_ns = static_cast<double>(EPOCH_DURATION_MS * 1e6); // convert ms to ns
        double retrieveSpeed = snap.retrieved_bytes / epoch_ns;
        double processSpeedPerThread = static_cast<double>(snap.processed_bytes) / static_cast<double>(snap.processed_time_ns);
        
        size_t current_retrieving = sched_state.active_retrieving.load(std::memory_order_relaxed);
        size_t current_preparing = sched_state.active_preparing.load(std::memory_order_relaxed);

        double processSpeed = processSpeedPerThread * (nthreads - current_retrieving - current_preparing);

        double instant_ratio = processSpeed / retrieveSpeed;
        double alpha;

        // use bias-corrected exponential moving average, inspired by Adam Optimizer in Deep Learning
        // https://arxiv.org/pdf/1412.6980v8
        smoothed_ratio = (EMA_ALPHA * instant_ratio) + ((1.0 - EMA_ALPHA) * smoothed_ratio);
        double bias_correction = 1.0 - std::pow((1.0 - EMA_ALPHA), epoch_count);
        double corrected_ratio = smoothed_ratio / bias_correction;

        double req_retriever_threads = std::min(static_cast<double>(MAX_RETRIEVERS) * corrected_ratio,
                                                static_cast<double>(MAX_RETRIEVERS));

        size_t current_target = sched_state.target_retriever_threads.load();
        size_t new_target_retrieving = current_target;
        
        // check if the difference is above the specified threshold, prevent target jittering
        double diff = std::abs(req_retriever_threads - static_cast<double>(current_target));
        if (diff > (static_cast<double>(MAX_RETRIEVERS) * REASSIGNMENT_THRESHOLD)) {
            // apply damping factor to avoid overshooting
            new_target_retrieving = static_cast<size_t>(
                std::round((1.0 - DAMPING_FACTOR) * current_target + DAMPING_FACTOR * req_retriever_threads)
            );
        }
        
        new_target_retrieving = std::max(size_t(1), std::min(new_target_retrieving, MAX_RETRIEVERS));
        sched_state.target_retriever_threads.store(new_target_retrieving);

        // calculate required bandwidth
        double required_bandwidth = std::min(MAX_NETWORK_BANDWIDTH, MAX_NETWORK_BANDWIDTH * corrected_ratio);
        
        double epoch_seconds = static_cast<double>(EPOCH_DURATION_MS) / 1000.0;
        // overpreparation (2x Rule)
        size_t target_outstanding_bytes = static_cast<size_t>(required_bandwidth * 2.0 * epoch_seconds); 
        target_outstanding_bytes = std::min(target_outstanding_bytes, MAX_PREPARED_BYTES);
        
        sched_state.max_outstanding_bytes.store(target_outstanding_bytes);

        // Debug
        SCHED_LOG(999, "Stats Update:\nP./R. Speed Instant Ratio: " + std::to_string(instant_ratio)
            + "\tP./R. Speed Smoothed Ratio (Corrected): " + std::to_string(corrected_ratio)
            + "\nTarget retrieving: " + std::to_string(new_target_retrieving)
            + "\tActive retrieving: " + std::to_string(sched_state.active_retrieving.load(std::memory_order_relaxed))
            + "\tActive preparing: " + std::to_string(sched_state.active_preparing.load(std::memory_order_relaxed))
            + "\tActive processing: " + std::to_string(sched_state.active_processing.load(std::memory_order_relaxed))
            + "\nRequired Bandwidth: " + std::to_string(required_bandwidth / 1e6) + "MB"
            + "\tMax Outstanding MB: " + std::to_string(sched_state.max_outstanding_bytes.load(std::memory_order_relaxed) / 1024 / 1024)
            + "\tOutstanding MB: " + std::to_string(sched_state.outstanding_bytes.load(std::memory_order_relaxed) / 1024 / 1024)
            + "\tChunks downloaded: " + std::to_string(chunks_downloaded.load(std::memory_order_relaxed)) + "/"
            + "\tChunks processed: " + std::to_string(chunks_processed.load(std::memory_order_relaxed)) + "/"
            + std::to_string(total_chunks)
            + "\nProcessed Bytes: " + std::to_string(snap.processed_bytes)
            + "\tProcessing Time: " + std::to_string(snap.processed_time_ns)
            + "\tRetrieved Bytes: " + std::to_string(snap.retrieved_bytes)  + "\n"
        );
    }

    void prepareOneBlock(anyblob::network::Transaction& txn, ParquetTable::DownloadCallback cb) {
        size_t idx = chunks_prepared.load(std::memory_order_relaxed);
        if (idx >= total_chunks) return;

        if (chunks_prepared.compare_exchange_strong(idx, idx + 1)) {
            size_t compressed_size = current_table->getCompressedChunkSize(idx);
            sched_state.outstanding_bytes.fetch_add(compressed_size, std::memory_order_relaxed);
            current_table->scheduleFetchAsync(idx, txn, cb);
        }
    }

    void processOneMorsel(ScanFn kernel_ptr, const std::vector<std::string>& layout_map, void** pipeline_args, size_t thread_id, std::shared_ptr<anyblob::network::TaskedSendReceiverHandle> io_handle) {
        std::unique_ptr<FetchedChunk> chunk;
        // ready_queue.try_dequeue(chunk);

        // if (!chunk) {
        //     if (!stop_requested && chunks_processed < total_chunks) {
        //         chunk_misses.fetch_add(1, std::memory_order_relaxed);
        //     }
        //     return;
        // }

        bool found = ready_queue.wait_dequeue_timed(chunk, std::chrono::milliseconds(1));
        if (!found) {
            if (!stop_requested && chunks_processed < total_chunks) {
                chunk_misses.fetch_add(1, std::memory_order_relaxed);
            }
            return;
        }

        auto start = std::chrono::steady_clock::now();

        size_t chunk_size_bytes = chunk->size;
        auto decompressed = current_table->decompressChunk(std::move(chunk));
        current_table->setThreadLocalChunk(decompressed);
        
        std::vector<void*> column_ptrs(layout_map.size(), nullptr);
        populateColumnPtrs(current_table, layout_map, column_ptrs);
        // run llvm kernel
        kernel_ptr(column_ptrs.data(), 0, decompressed->row_count, thread_id, pipeline_args);
        
        auto end = std::chrono::steady_clock::now();
        
        chunks_processed.fetch_add(1, std::memory_order_release);
        stats.processed_bytes.fetch_add(chunk_size_bytes, std::memory_order_relaxed);
        stats.processed_time_ns.fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(),
            std::memory_order_relaxed
        );
    }
    
    void driveIOHandle(std::shared_ptr<anyblob::network::TaskedSendReceiverHandle> handle) {
        auto start = std::chrono::steady_clock::now();
        handle->process(true);
        auto end = std::chrono::steady_clock::now();

        stats.retrieval_time_ns.fetch_add(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count(), 
            std::memory_order_relaxed
        );
    }

    JobType pickJob() {
        size_t active_ret = sched_state.active_retrieving.load(std::memory_order_relaxed);
        size_t target_ret = sched_state.target_retriever_threads.load(std::memory_order_relaxed);
        size_t active_prep = sched_state.active_preparing.load(std::memory_order_relaxed);
        size_t outstanding = sched_state.outstanding_bytes.load(std::memory_order_relaxed);
        
        bool all_scheduled = (chunks_prepared.load(std::memory_order_relaxed) >= total_chunks);
        bool all_downloaded = (chunks_downloaded.load(std::memory_order_acquire) >= total_chunks);
        
        // Priority 1: Balance Retrieval Threads
        if (active_ret < target_ret && outstanding > 0) {
            return JobType::RETRIEVAL;
        }

        // Priority 2: Prepare more data (Scheduling) - ensure outstanding requests saturate bandwidth
        if (!all_scheduled) {
            size_t prepared_idx = chunks_prepared.load(std::memory_order_relaxed);
            size_t next_chunk_compressed = current_table->getCompressedChunkSize(prepared_idx);
            size_t next_chunk_uncompressed = current_table->getUncompressedChunkSize(prepared_idx);

            size_t current_outstanding = sched_state.outstanding_bytes.load(std::memory_order_relaxed);
            size_t max_outstanding = sched_state.max_outstanding_bytes.load(std::memory_order_relaxed);

            bool bw_allowed = (current_outstanding + next_chunk_compressed) <= max_outstanding;
            bool mem_allowed = BlockPool::get().canAllocate(next_chunk_uncompressed);

            if (bw_allowed && mem_allowed) return JobType::PREPARATION;
            // if (!bw_allowed) SCHED_LOG(999, "Bandwidth pressure: Can't assign preparation job because outstanding byte limit will be exceeded.");
            // if (!mem_allowed) SCHED_LOG(999, "Memory pressure: Can't assign preparation job because BlockPool memory limit will be exceeded.");
        }

        // safeguard: don't let all threads fall into processing if there are chunks to prepare or download
        size_t active_proc = sched_state.active_processing.load(std::memory_order_relaxed);
        if (!all_downloaded && active_proc >= (nthreads - 1)) {
            if (outstanding > 0) return JobType::RETRIEVAL;
            if (!all_scheduled) return JobType::PREPARATION;
        }

        // Priority 3: Processing
        return JobType::PROCESSING;
    }
};

} // namespace p2cllvm