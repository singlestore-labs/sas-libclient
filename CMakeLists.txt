cmake_minimum_required(VERSION 3.13)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

set(LIBCLIENT_SOURCES
    hdat/chunk_reader.cpp
    hdat/chunk_writer.cpp
    hdat/super_chunk.hpp
    parallel_read.cpp
    result_table_reader.cpp
    s2_connection.cpp
    s2_client.cpp
    queue/chunk_queue.hpp
    queue/chunks_info.hpp
    queue/multi_pass_queue.cpp
    queue/streaming_queue.cpp
    queue/thread_safe_queue.hpp
    queue/thread_safe_batch_queue.hpp
    queue/thread_safe_simple_queue.hpp
    table_writer.cpp
    utils.cpp)

add_library(s2client SHARED ${LIBCLIENT_SOURCES})

target_include_directories(s2client PUBLIC $ENV{PATH_TO_LIBCLIENT}
                                           libmariadb/include)

target_link_libraries(
  s2client
  mariadb
  dl
  pthread
  crypto
  ssl
  z
  rt
  m)
