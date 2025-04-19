# Test executable
add_executable(
  localdb_test
  value_test.cc
  table_test.cc
  database_test.cc
  transaction_test.cc
  test_main.cc
)

# Include the main source directory to access implementation files
target_include_directories(localdb_test PRIVATE ${CMAKE_SOURCE_DIR}/src/include)

# Link against gtest and localdb implementation
target_link_libraries(
  localdb_test
  PRIVATE
  gtest
  Threads::Threads
)

# Add source files from main implementation to test executable (excluding main.cc)
target_sources(
  localdb_test
  PRIVATE
  ${CMAKE_SOURCE_DIR}/src/localdb.cc
)

# Register tests
include(GoogleTest)
gtest_discover_tests(localdb_test) 