include(GoogleTest)

add_executable(run_tests
    test_main.cc
    load_test.cc
    sv_test.cc
    tpch_test.cc
    materialization_test.cc
    hll_test.cc
    threadlocal_test.cc
    sort_test.cc
)

target_link_libraries(run_tests
    PRIVATE
        hpqpllvm_lib
        GTest::gtest_main
)

gtest_discover_tests(run_tests)
