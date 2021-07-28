CMAKE_MINIMUM_REQUIRED(VERSION 3.13)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

IF(COMMAND cmake_policy)
  CMAKE_POLICY(SET CMP0003 NEW)
ENDIF(COMMAND cmake_policy)

SET(LIBCLIENT_SOURCES
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
    utils.cpp
)

ADD_LIBRARY(s2client SHARED ${LIBCLIENT_SOURCES})

TARGET_INCLUDE_DIRECTORIES(s2client PUBLIC $ENV{PATH_TO_LIBCLIENT})

TARGET_LINK_LIBRARIES(s2client ${CMAKE_SOURCE_DIR}/libmysql/libmysqlclient.a dl
                      pthread z rt)
