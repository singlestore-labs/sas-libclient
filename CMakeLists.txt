cmake_minimum_required(VERSION 3.13)
project("libs2client")
set(CMAKE_CXX_COMPILER "/usr/bin/clang")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

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
    queue/multi_pass_queue.cpp
    queue/streaming_queue.cpp
    queue/thread_safe_queue.hpp
    queue/thread_safe_batch_queue.hpp
    queue/thread_safe_simple_queue.hpp
    avro_table_writer.hpp
    avro_table_writer.cpp
    tsv_table_writer.hpp
    tsv_table_writer.cpp
    utils.cpp)

add_library(s2client SHARED ${LIBCLIENT_SOURCES})

target_include_directories(
  s2client PUBLIC $ENV{PATH_TO_LIBCLIENT} vendor/libmariadb/include
                  vendor/libavro/include vendor/libjansson/include)

target_link_directories(
  s2client PUBLIC $ENV{PATH_TO_LIBCLIENT}/vendor/libmariadb
  $ENV{PATH_TO_LIBCLIENT}/vendor/libavro
  $ENV{PATH_TO_LIBCLIENT}/vendor/libjansson)

target_link_libraries(
  s2client
mariadb
  avro
  jansson
  dl
  pthread
  crypto
  ssl
  z
  rt
  m)
