file(GLOB_RECURSE IR_HEADERS
    ${CMAKE_SOURCE_DIR}/include/IR/*.h)

set(IR_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/BinOp.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/Builder.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/ColTypes.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/Tuple.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/Types.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/UnOp.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/Pipeline.cc
    )

add_library(ir STATIC ${IR_SOURCES} ${IR_HEADERS})

target_include_directories(ir
    PUBLIC 
        ${LLVM_INCLUDE_DIRS}
)

