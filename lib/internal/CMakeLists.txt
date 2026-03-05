file(GLOB_RECURSE INTERNAL_HEADERS
    ${CMAKE_SOURCE_DIR}/include/internal/*.h)

set(INTERNAL_SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/ParquetTable.cc
    )

add_library(internal STATIC ${INTERNAL_SOURCES} ${INTERNAL_HEADERS})

target_include_directories(internal
    PUBLIC 
        ${ANYBLOB_ROOT}/include
        ${LLVM_INCLUDE_DIRS}
)

target_link_libraries(internal
    PUBLIC 
        Arrow::arrow_shared
        Parquet::parquet_shared
)
