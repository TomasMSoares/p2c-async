# Using Parquet
## High level purpose
Parquet provides a way to store data efficiently in remote, thus saving on cloud costs.
It is a standardized format for which there are reader/processing libraries (`parquet/file_reader.h`)

## Data format
Parquet splits data across row groups (chunks of rows).
Inside each row group, data is stored in column-major format, which is good since the JIT query engine expected columnar format (in the old in memory DB).
For __chunk-based processing__, two options came up:
1. Store _one_ file __per table__ with multiple row groups
2. Store _one_ file __per row group (chunk)__

I decided to go with option 2 for a few reasons...
### CRITICAL REASON:
Parquet stores metadata in the _footer_, not the header; to get information on the layout of the file (row grp offsets) I would need to:
1. Fetch the size of the object, to __calculate the range of the request__ for the footer metadata (not the actual metadata, this one is fixed-size).
2. Fetch the footer (metadata) _itself_, read the offsets of the row groups
3. Start fetching chunks based on this calculation

Other than large initial overhead, the AnyBlob library doesn't expose an API to fetch object sizes (commonly HEAD requests) - how to perform initial fetch if size of object is not able to be determined remotely? I would have to figure that out

### SECOND REASON:
Larger complexity in the implementation to support true chunk parallelism. No need to worry about ranged requests/object sizes, since we can enforce optimal chunk sizing strategy at __data generation stage__.

However, this simplicity (in request and subsequent processing logic) comes with tradeoffs:
1. Potentially a LOT of objects per table, all stored in one directory (each with small overhead of footer, non negligible)
2. Need to store metadata externally (one `json` file per directory) - reliant on custom data generation/interpretation logic, and can't avoid initial request for metadata.

## TODOS
- Check compression schemes, optimize processing