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

# Host-only workflow effect-command replay oracle; no AttoLang guest proof.
add_executable(durable_workflow_replay_oracle_test tests/WorkflowReplayOracleTest.cpp)
target_include_directories(durable_workflow_replay_oracle_test PRIVATE src/native)
target_compile_features(durable_workflow_replay_oracle_test PRIVATE cxx_std_17)
target_compile_options(durable_workflow_replay_oracle_test PRIVATE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic -Werror -UNDEBUG>)

if(BUILD_TESTING)
  add_test(NAME durable_workflow_replay_oracle COMMAND durable_workflow_replay_oracle_test)
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
