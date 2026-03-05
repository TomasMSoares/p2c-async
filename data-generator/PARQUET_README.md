# Parquet Data Generator for Remote Query Engine

This directory contains tools to generate TPC-H data in Parquet format with metadata for the remote query engine.

## Overview

The workflow consists of three steps:

1. **Generate TPC-H CSV data** (using tpch-dbgen)
2. **Convert to Parquet with metadata** (using `parquet-generator`)

## Files Generated

For each TPC-H table (e.g., `lineitem`), the following files are generated:

### Parquet Data Files
- `lineitem.parquet` - Columnar storage with row groups (chunks)

### Parquet Metadata JSON
- `lineitem_metadata.json` - Required by `ParquetTable` class
  ```json
  {
    "object_key": "tpch/lineitem.parquet",
    "size": 123456789,
    "row_groups": [
      {
        "id": 0,
        "num_rows": 1000000,
        "offset": 4,
        "length": 45678901
      },
      ...
    ]
  }
  ```

### Table Schema JSON
- `tpch_schema.json` - Required by `CloudSchemaManager` (single file for all tables)
  ```json
  {
    "tables": [
      {
        "name": "lineitem",
        "columns": [
          {"name": "l_orderkey", "type": "BIGINT"},
          {"name": "l_partkey", "type": "INTEGER"},
          {"name": "l_extendedprice", "type": "DOUBLE"},
          {"name": "l_shipdate", "type": "DATE"},
          {"name": "l_comment", "type": "STRING"},
          ...
        ]
      },
      ...
    ]
  }
  ```

## Prerequisites

### System Dependencies
```bash
# Arrow/Parquet C++ libraries
sudo apt-get install libarrow-dev libparquet-dev
```
## Step-by-Step Instructions

### 1. Generate TPC-H CSV Data

```bash
cd tpch-dbgen
make
./dbgen -s 1  # Scale factor 1 = 1GB dataset
cd ..
```

This creates `.tbl` files (CSV format) in `tpch-dbgen/`.

### 2. Generate Parquet Files and Metadata

```bash
# Build the generator
make parquet-generator

# Generate Parquet files with 1M rows per row group
./parquet-generator tpch-dbgen/ parquet-output/ 1000000

# Output structure:
# parquet-output/
#   ├── lineitem.parquet
#   ├── lineitem_metadata.json
#   ├── orders.parquet
#   ├── orders_metadata.json
#   ├── ...
#   └── tpch_schema.json
```

**Row Group Size Recommendation:**
- Small dataset (SF 1): 500K-1M rows
- Medium dataset (SF 10): 1M-2M rows  
- Large dataset (SF 100): 2M-5M rows

## Using the Generated Data in C++

### Initialize Cloud Manager

```cpp
#include "internal/CloudManager.h"
#include "internal/Schema.h"
#include "internal/ParquetTable.h"

// Load schema (once at startup)
CloudSchemaManager::get().loadFromCloud("path/to/tpch_schema.json");

// Get schema for a table
const auto& schema = CloudSchemaManager::get().getSchema("lineitem");
```

### Create ParquetTable

```cpp
// Provider URI format: minio://host:port/bucket
std::string provider_uri = "minio://localhost:9000/test-bucket";

// Create table with metadata
ParquetTable lineitem_table(
    "path/to/lineitem_metadata.json",
    provider_uri
);

// Register columns needed by query (optimization)
lineitem_table.registerColumn("l_orderkey");
lineitem_table.registerColumn("l_extendedprice");
lineitem_table.registerColumn("l_discount");
```

### Access Column Data (Inside LLVM Pipeline)

```cpp
// Scan operator uses ITable interface
void* col_data = table->getColumnData("l_orderkey");
uint64_t row_count = table->getRowCount();

// Fixed-size types (int32_t, int64_t, double, date)
int64_t* orderkeys = static_cast<int64_t*>(col_data);

// Strings (slotted page format)
String* strings = static_cast<String*>(col_data);
for (uint64_t i = 0; i < row_count; i++) {
    auto& slot = strings->slot[i];
    const char* str_data = reinterpret_cast<const char*>(strings) + slot.offset;
    size_t length = slot.length;
    // Use str_data, length...
}
```

## Verifying the Setup

### Check MinIO Upload
```bash
# Using mc (MinIO Client)
mc ls myminio/test-bucket/tpch/

# Should show:
# [DATE] [TIME]  123MB lineitem.parquet
# [DATE] [TIME]   4KB lineitem_metadata.json
# ...
```

### Inspect Parquet Metadata
```bash
# Using parquet-tools (Python)
pip install parquet-tools
parquet-tools show lineitem.parquet

# Or using Arrow
python3 -c "
import pyarrow.parquet as pq
file = pq.ParquetFile('parquet-output/lineitem.parquet')
print(f'Num row groups: {file.num_row_groups}')
print(f'Schema: {file.schema}')
for i in range(file.num_row_groups):
    rg = file.metadata.row_group(i)
    print(f'Row group {i}: {rg.num_rows} rows')
"
```

## Performance Tuning

### Row Group Size
- **Smaller row groups** (100K-500K rows):
  - Lower memory usage per chunk
  - Higher metadata overhead
  
- **Larger row groups** (2M-5M rows):
  - Better compression ratio
  - Higher memory usage

### Compression
The generator uses **No** compression by default (good balance of speed/ratio). To change:
```cpp
// In parquet-generator.cpp, modify:
auto props = parquet::WriterProperties::Builder()
    .compression(parquet::Compression::ZSTD)  // Better ratio, slower
    // .compression(parquet::Compression::SNAPPY)  // Faster
    // .compression(parquet::Compression::UNCOMPRESSED)  // Fastest
    ->build();
```

## Other Usage

### Subset of Tables
To generate only specific tables, modify `parquet-generator.cpp` and comment out unwanted tables in `main()`.

### Custom Row Group Boundaries
For workload-specific optimizations (e.g., partition by date), modify the chunking logic in `writeParquet()`.

## Integration with Query Engine
See `ParquetTable.h` and `ParquetTable.cc` for implementation details.
