file(GLOB_RECURSE RT_HEADERS
    ${CMAKE_SOURCE_DIR}/include/runtime/*.h)

set(RT_SOURCES 
    ${CMAKE_CURRENT_SOURCE_DIR}/Hashtable.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/Tuplebuffer.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/Runtime.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/Test.cc
    )

add_library(runtime STATIC ${RT_SOURCES} ${RT_HEADERS})

