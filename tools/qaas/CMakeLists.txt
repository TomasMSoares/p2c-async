find_package(fmt REQUIRED)

add_executable(optimizer planConverter.cpp)
add_executable(result_fetcher resultMaterializer.cpp)

target_link_libraries(optimizer PRIVATE fmt)
target_link_libraries(result_fetcher PRIVATE fmt)
