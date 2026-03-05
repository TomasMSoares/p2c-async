if(DEFINED ENV{ANYBLOB_ROOT})
    set(ANYBLOB_ROOT "$ENV{ANYBLOB_ROOT}")
else()
    set(ANYBLOB_ROOT "$ENV{HOME}/opt/anyblob-install")
endif()
message(STATUS "Using AnyBlob root: ${ANYBLOB_ROOT}")

if(DEFINED ENV{CONCURRENTQUEUE_ROOT})
    set(CONCURRENTQUEUE_ROOT "$ENV{CONCURRENTQUEUE_ROOT}")
else()
    set(CONCURRENTQUEUE_ROOT "$ENV{HOME}/opt/concurrentqueue")
endif()
message(STATUS "Using ConcurrentQueue root: ${CONCURRENTQUEUE_ROOT}")

# Verify header exists
if(NOT EXISTS "${CONCURRENTQUEUE_ROOT}/concurrentqueue.h")
    message(FATAL_ERROR "ConcurrentQueue header not found at ${CONCURRENTQUEUE_ROOT}/concurrentqueue.h")
endif()

# Find packages before adding subdirectories that need them
find_package(LLVM REQUIRED CONFIG)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/IR)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/runtime)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/internal)

target_include_directories(ir
    PUBLIC 
        ${ANYBLOB_ROOT}/include
        ${CONCURRENTQUEUE_ROOT}
)
# --------------------

file(GLOB_RECURSE HPQPLLVM_HEADERS
    ${CMAKE_SOURCE_DIR}/include/*.h)

set(HPQPLLVM_SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/Driver.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/IU.cc
    )

message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")

separate_arguments(LLVM_DEFINITIONS_LIST NATIVE_COMMAND ${LLVM_DEFINITIONS})
add_definitions(${LLVM_DEFINITIONS_LIST})

llvm_map_components_to_libnames(llvm_libs support core irreader)

# AnyBlob dependencies
find_library(ANYBLOB_LIBRARY NAMES AnyBlob PATHS ${ANYBLOB_ROOT}/lib NO_DEFAULT_PATH)

if (NOT ANYBLOB_LIBRARY)
    message(FATAL_ERROR "AnyBlob library not found. Checked in: ${ANYBLOB_ROOT}/lib")
endif()

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

find_library(LIBURING_LIBRARY NAMES uring)
if (NOT LIBURING_LIBRARY)
message(FATAL_ERROR "liburing not found. Please install it.")
endif()
### End AnyBlob deps

add_library(hpqpllvm_lib STATIC ${HPQPLLVM_SOURCES} ${HPQPLLVM_HEADERS})

target_include_directories(hpqpllvm_lib
    PUBLIC 
        ${ANYBLOB_ROOT}/include
)

target_link_libraries(hpqpllvm_lib PUBLIC LLVM)
target_link_libraries(hpqpllvm_lib PUBLIC ir)
target_link_libraries(runtime PRIVATE ir)
target_link_libraries(hpqpllvm_lib PUBLIC runtime)
target_link_libraries(hpqpllvm_lib PUBLIC internal)

target_link_libraries(hpqpllvm_lib
    PUBLIC 
        ${ANYBLOB_LIBRARY}          
        ${LIBURING_LIBRARY}
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
        jemalloc
        Arrow::arrow_shared
        Parquet::parquet_shared
)
