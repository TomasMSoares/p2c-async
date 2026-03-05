cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 20)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()
    
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
set(LLVM_TARGETS_TO_BUILD "all")

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-march=native)
else()
    add_compile_options(-msse4.2)
endif()

project(HPQPLLVM VERSION 0.0.1)

# jemalloc library for better mem allocation management
add_definitions(-DUSE_JEMALLOC)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(QUERIES "tpch-q01" "tpch-q05" "tpch-q06" "tpch-q07" "tpch-q08" "tpch-q09" "tpch-q12" "tpch-q14" "tpch-q17" "tpch-q19")

include_directories(include)
add_subdirectory(${CMAKE_SOURCE_DIR}/lib)
add_subdirectory(${CMAKE_SOURCE_DIR}/tools/qaas/)

foreach(QUERY ${QUERIES})
    add_executable(${QUERY} ${CMAKE_SOURCE_DIR}/queries/definitions/${QUERY}/definition.cpp)
    target_link_libraries(${QUERY} PUBLIC hpqpllvm_lib)
    add_custom_command(
        OUTPUT "${CMAKE_SOURCE_DIR}/queries/definitions/${QUERY}/definition.cpp"
        PRE_BUILD
        COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/build/tools/qaas/optimizer" ${QUERY} 
        ">" "${CMAKE_SOURCE_DIR}/queries/definitions/${QUERY}/definition.cpp"
        COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/build/tools/qaas/result_fetcher" ${CMAKE_SOURCE_DIR}/queries/definitions/${QUERY} 
        ">" "${CMAKE_SOURCE_DIR}/queries/definitions/${QUERY}/result.hpp"
        DEPENDS optimizer result_fetcher
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
endforeach()
add_executable(hpqpllvm main.cc)
target_link_libraries(hpqpllvm PUBLIC hpqpllvm_lib)

# Test program for ParquetTable
# add_executable(test-parquet-table test-parquet-table.cc)
# target_link_libraries(test-parquet-table PUBLIC hpqpllvm_lib)

include(gtest)
enable_testing()
add_subdirectory(${CMAKE_SOURCE_DIR}/unittests)




