
# StringHeap

## High level purpose
### Independent lifetime/persistence
"Upstream" operators such as __Aggregation__ or __Join__ need to manage their own __string lifetimes__, since the data needs to live __longer__ than the original `DecompressedChunk` (for _HastTables_, etc.)
The `DecompressedChunk`'s memory is supposed to be released once it's been processed.
Before, since the data all lived in memory, the operators could just point at the strings in mem for the duration of the query - not possible anymore with remote data fetch

### Thread Safety
Since multiple threads' hash tables need the strings, this can lead to race conditions (especially on release of the `DecompressedChunk`s) - safer to let each thread manage its own `stringHeap`.

## Changes Made:
### Memory allocation
Replaced `mmap` calls in TupleBuffer's `alloc` with custom `BlockPool` logic (unified memory management)
Added String Heap to `JoinContext` && `AggregationContext` (`OperatorContext.h`):
Added TupleBuffer string_heap{64} to store materialized string data

### Implemented __DEEP STRING COPY__ (`Types.cc`, `Runtime.h`, `Runtime.cc`):
- Modified StringTy::createCopy() to detect when a string heap is passed as argument
- When available (in joins/aggregations), it copies the actual string bytes to the heap
- Updates the StringView pointer to reference the string heap instead of source chunks
- Falls back to shallow copy for other operators (maybe has to be changed - Sort?)
- Added allocStringHeap() function to allocate from TupleBuffer

### __TRADEOFF FOR SORT__:
- Sort should also set the stringHeap and perform deep string copy if isolated.
- However, if Aggregation is upstream, it is not necessary - performance hit
- _Question:_ Do we always rely on an upstream Aggregation/(Join) or should we double deep-copy for robustness?

### Minor, but necessary modifications to Hashing:
- `Murmur.h` and `Hash.h` had to be adapted to handle _unaligned_ strings (since they have potentially been deep-copied to string_heap, and those are tight-packed)
- Tight packing ensures no memory is wasted, but objects must be handled as unaligned.

Also fixed small error in `lib/runtime/Runtime.cc`, specifically incorrect `sv_length` accessing in the method `bool string_gt()`

### Per-thread context aggregation
Added blocks in InnerJoin to loop over all thread contexts and merge  HT's

## TODOS:
- Enable `jemalloc` for `BlockPool`, improve (?)
- Check why async is not working
- For atomic llvm instructions on floating points: https://groups.google.com/g/llvm-dev/c/T3dSFZTwl2U/m/lAlJPXNdHGAJ?pli=1 - might be necessary for robustness