cmake_minimum_required(VERSION 3.20)
project(ores_durable_objects VERSION 0.1.0 LANGUAGES CXX)
include(CTest)
find_package(Threads REQUIRED)
find_package(SQLite3 3.35 REQUIRED)

# Standalone SQLite storage reference retained from main.
add_executable(sqlite_durable_storage_test tests/SqliteDurableStoreTest.cpp)
target_include_directories(sqlite_durable_storage_test PRIVATE src/native)
target_compile_features(sqlite_durable_storage_test PRIVATE cxx_std_17)
target_compile_options(sqlite_durable_storage_test PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic -Werror -UNDEBUG>)
target_link_libraries(sqlite_durable_storage_test PRIVATE SQLite::SQLite3 Threads::Threads)

# Tenant-scoped persisted keyed FIFO runtime introduced by PR #2.
add_library(ores_durable_objects STATIC native/tenant_runtime.cpp)
target_include_directories(ores_durable_objects PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/native/include")
target_compile_features(ores_durable_objects PUBLIC cxx_std_17)
target_link_libraries(ores_durable_objects PUBLIC SQLite::SQLite3 Threads::Threads)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
  target_compile_options(ores_durable_objects PRIVATE -Wall -Wextra -Wpedantic -Werror)
endif()

if(BUILD_TESTING)
  add_test(NAME sqlite_durable_storage_test COMMAND sqlite_durable_storage_test)
  add_executable(durable_isolate_queues_test native/tests/durable_isolate_queues_test.cpp)
  target_link_libraries(durable_isolate_queues_test PRIVATE ores_durable_objects)
  target_compile_options(durable_isolate_queues_test PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Werror>)
  add_executable(durable_persisted_fence_guard_test native/tests/durable_persisted_fence_guard_test.cpp)
  target_link_libraries(durable_persisted_fence_guard_test PRIVATE ores_durable_objects)
  target_compile_options(durable_persisted_fence_guard_test PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Werror>)
  add_test(NAME durable_persisted_fence_guard COMMAND durable_persisted_fence_guard_test)
  add_test(NAME durable_isolate_queues COMMAND durable_isolate_queues_test)
  set_tests_properties(durable_isolate_queues PROPERTIES TIMEOUT 30)
endif()

# Optional, authorized LLVM IsolateVM integration check. The separate LLVM
# repository is NOT fetched or mirrored here. Provide its include root from a
# locally available, separately authorized exact checkout.
option(ORES_DURABLE_TEST_LLVM_ADAPTER "Build LLVM OresVM adapter lifecycle test" OFF)
set(ORESVM_INCLUDE_DIR "" CACHE PATH "Authorized oreslang-source.llvm/src/main/cpp include root")
if(BUILD_TESTING AND ORES_DURABLE_TEST_LLVM_ADAPTER)
  if(NOT EXISTS "${ORESVM_INCLUDE_DIR}/oreslang/runtime/OresVM.hpp")
    message(FATAL_ERROR "ORESVM_INCLUDE_DIR must point to an authorized OresVM header checkout")
  endif()
  add_executable(durable_llvm_adapter_lifecycle_test
    native/tests/durable_llvm_adapter_lifecycle_test.cpp)
  target_compile_features(durable_llvm_adapter_lifecycle_test PRIVATE cxx_std_17)
  target_include_directories(durable_llvm_adapter_lifecycle_test PRIVATE "${ORESVM_INCLUDE_DIR}")
  target_link_libraries(durable_llvm_adapter_lifecycle_test PRIVATE ores_durable_objects)
  target_compile_options(durable_llvm_adapter_lifecycle_test PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic;-Werror;-UNDEBUG>)
  add_test(NAME durable_llvm_adapter_lifecycle COMMAND durable_llvm_adapter_lifecycle_test)
  set_tests_properties(durable_llvm_adapter_lifecycle PROPERTIES TIMEOUT 30)
endif()
