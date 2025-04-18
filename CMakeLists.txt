cmake_minimum_required(VERSION 3.10)
project(localdb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler options
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

# Add pthread library for multi-threading
find_package(Threads REQUIRED)

# Add filesystem library if needed (for GCC < 9.0)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
    set(FILESYSTEM_LIBRARIES stdc++fs)
else()
    set(FILESYSTEM_LIBRARIES "")
endif()

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)

# Enable testing
enable_testing()

# Main demo executable
add_executable(localdb
    src/main.cc
    src/localdb.cc
)

# CLI executable 
add_executable(localdb_cli
    src/cli.cc
    src/localdb.cc
)

# Link against pthread and filesystem
target_link_libraries(localdb PRIVATE Threads::Threads ${FILESYSTEM_LIBRARIES})
target_link_libraries(localdb_cli PRIVATE Threads::Threads ${FILESYSTEM_LIBRARIES})

# Google Test
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Add tests subdirectory
add_subdirectory(test)
