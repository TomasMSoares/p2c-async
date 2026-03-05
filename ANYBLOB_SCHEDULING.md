# AnyBlob async object retrieval

## Database Engine Design
Tasks and scheduling of worker threads. The overall design of our cloud storage-optimized DBMS centers around the table scan operator.
Like most database systems, our system Umbra uses a pool of worker threads to process queries in parallel. In our design, worker threads do not only perform (i) regular query processing, but can also (ii) prepare new object store requests or (iii) serve as network threads.
Our object scheduler, which we present in Section 4.3, dynamically determines each worker’s job (i-iii) depending on network bandwidth saturation and processing progress.
### Task adaptivity
To overcome issues with long-running queries that block resources, many database systems use tasks to process queries. These tasks can either be suspended or run only for a small amount of time.
Both concepts lead to a query engine that is able to adapt to changing workloads quickly.
Regardless of the specific task system, our asynchronous retrieval integration only requires the mechanism to switch tasks of workers during query runtime.
### Columnar format
The raw data is organized in a column-major
relation format chunked in immutable blocks of columns. The meta-data of a block, e.g., column types and offsets, are stored in the block header. The database schema information is also stored on cloud storage, which requires fetching at start-up.
### Table metadata retrieval
In the following, we describe the flow of information during a table scan operation.
In steps 1 and 2, the scan operator first requests table metadata, i.e., the list of blocks.
Afterward, all relevant block metadata is downloaded as a requirement to start the table scan’s data retrieval.
### Worker thread scheduling
After initializing the table scan, we dedicate multiple worker threads to this operation.
Because partitioning worker threads into retrieval and processing threads is difficult and requires adaptations over the duration of the query, we implement an object scheduler to solve this problem. 
Step3 shows that each scanning thread asks the scheduler which job to work on. If enough data is retrieved, the worker thread proceeds to process data.
Otherwise, we dedicate the thread to preparing blocks for retrieval. Since we only execute jobs for a short time, this decision can be quickly adapted.
### Download preparation
To saturate the network bandwidth, it is important to continuously download with enough retrieval threads and many outstanding requests.
The preparation worker creates new requests that allow the retrieval threads to execute their event loop without interruption.
The object manager holds metadata of tables, blocks, and their column chunk data.
The column chunk data is managed by our variable-sized buffer manager.
If the data is not in memory, we create a new request and schedule it for
retrieval, shown in 5B . Finally, retrieval threads fetch the data.

## Table Scan Operator
### Scan design preliminaries
We carefully integrate AnyBlob into our RDBMS Umbra, which compiles SQL to machine-code and supports efficient memory and buffer management.
Umbra uses worker threads to parallelize operators, such as table scans, and schedules as many worker threads as there are hardware threads available on the instance.
If there is only one active query, all workers are used to process that specific query.
Umbra’s tasks consist of morsels which describe a small chunk of data of the task.
Worker threads are assigned to tasks and process morsels until the task is finished or the thread is assigned to a different task.
### Morsel picking
After Umbra initializes the table scan, the worker threads start calling the pickMorsel method.
This function assigns chunks of the task’s data to worker threads.
This is repeated after each morsel completion as long as the thread continues to work on this table scan task.
The only difference in our approach is that our workers do not only need to process data but also prepare new blocks or retrieve blocks from storage servers.
Our object scheduler, which we explain further down, decides the job of a worker thread based on past processing and retrieval statistics.
Note that similar to our pickMorsel, every task-based system has a method that determines the next task of a worker thread.
### Worker jobs
If a thread is assigned to process data, a morsel is picked from the currently active block in pickMorsel.
In contrast to the processing job, the other jobs (preparation and retrieval) do not pick a morsel for scanning.
Instead, these jobs start routines that are required to prepare or retrieve blocks.
Regardless of the job, all workers return to pickMorsel to get a new job assigned after finishing their current work.
### Processing job
After receiving a morsel for processing, the thread scans and filters the data according to the semantics of the table scan.
When all morsels of an active block (global or thread-local with stealing) are taken, the thread picks the morsel from a new, already retrieved block.
In the example, each block is divided into 4 non-overlapping morsels. Each thread works on its unique morsel range.
### Preparation job
With the already retrieved table metadata, threads prepare new blocks and register unknown blocks in the object manager.
If the data of all columns currently resides in physical memory, the preparing thread marks the block as ready.
Otherwise, the preparing thread gets free space from the buffer manager for each unfixed column.
With the block metadata (column type, offset,and size), HTTP messages for fetching columns from cloud storage are created.
After that, the block is queued for retrieval, where the data is downloaded.
### Retrieval job
In the example, three threads are scheduled to act as AnyBlob retrieval threads.
After finishing the download of a block’s column chunk, a callback is invoked and marks this column as ready. 
Only if all columns have been retrieved, we mark the block as ready.
Note that different retrieval threads may download column chunks from the same block concurrently.
The worker finishes when AnyBlob’s request queue gets empty.
Because threads always try to keep the queue at its maximum request length, unnecessary retrieval threads will eventually encounter an empty queue and stop downloading.
These threads can then be reused to work on different jobs, such as processing or preparing new blocks.
As long as enough requests are in the queue, the threads constantly retrieve data.

## 4.3 Object Scheduler
### Balance of retrieval and processing performance
The main goal of the object scheduler is to strike a balance between processing and retrieval performance.
It assigns different jobs to the available worker threads to achieve this balance.
If the retrieval performance is lower than the scan performance, it increases the amount of retrieval and preparation threads.
On the other hand, reducing the number of retrieval threads results in higher processing throughput.
Note that the retrieval performance is limited by the network bandwidth, which the object scheduler considers.
### Processing and retrieval estimations
The decision process requires performance statistics during retrieval and processing.
Each processing thread tracks the execution time and the amount of data processed.
The aggregated data allow us to compute the mean processing throughput per thread.
For the network throughput, we aggregate the overall retrieved bytes during our current time epoch.
### Balancing retrieval threads and requests
Sections 2.8 and 3.4 (NOT PRESENT HERE) analyze how many concurrent requests are needed to achieve our throughput goal and the corresponding number of AnyBlob retrievers.
We track the number of threads used for retrieval and limit it according to the instance bandwidth specification.
By counting the number of outstanding requests (e.g., column chunks), we compute an upper bound on the outstanding network bandwidth.
An outstanding request is a prepared HTTP request currently downloaded or awaiting retrieval.
Because the number of threads and the outstanding requests limit the network bandwidth, our object scheduler always requires that the outstanding bandwidth is at least as high as the maximum bandwidth possible according to the current number of retrieval threads.
Hence, it schedules enough preparation jobs to match the number of retrieval threads.
### Performance adaptivity
The scheduler computes the global ratio between processing and retrieval to balance the retrieval and processing performance.
This ratio is used to adapt the number of retrieval threads and the outstanding bandwidth.
If processing is slower, fewer blocks are prepared, and fewer retrieval threads are scheduled.
Some of the running retrieval threads will stop due to fewer outstanding requests.
These threads are then scheduled as processing workers, increasing the global processing performance.

Algorithm 1: Scheduler: Adaptivity Computation
1. retrieveSpeed = statistics\[epoch\].retrievedBytes / statistics\[epoch\].elapsed
2. processSpeed = (workerThreads - currentRetriever) * statistics\[epoch\].processedBytes
/ statistics\[epoch\].processedTime
3. ratio = processSpeed / retrieveSpeed
4. requiredBandwidth = min(bandwidth, bandwidth * ratio)
5. requiredRetrieverThreads = min(maxRetrievers * ratio, maxRetrievers)

### Overpreparation
Because it is undesirable to stall retrieval threads due to unprepared columns, overpreparation is encouraged.
Our scheduler ensures that up to 2× of the required bandwidth is outstanding and schedules preparation jobs accordingly.
### Fast statistics aggregation
Lock-free atomic values for statistics and global counters provide fast object scheduler decisions.
For every new scan request, we update the epoch to store representative statistics of the current workload.