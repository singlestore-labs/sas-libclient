CMAKE_MINIMUM_REQUIRED(VERSION 3.13)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

IF(COMMAND cmake_policy)
    cmake_policy(SET CMP0003 NEW)
ENDIF(COMMAND cmake_policy)

SET(LIBCLIENT_SOURCES
    chunk_queue.cpp
    hdat/chunk_reader.cpp
    hdat/chunk_writer.cpp
    hdat/super_chunk.cpp
    parallel_read.cpp
    result_table_reader.cpp
    s2_connection.cpp
    s2_client.cpp
    thread_safe_queue.hpp
    utils.cpp
)

ADD_LIBRARY(s2client SHARED ${LIBCLIENT_SOURCES})

TARGET_INCLUDE_DIRECTORIES(s2client PUBLIC $ENV{PATH_TO_LIBCLIENT})

TARGET_LINK_LIBRARIES(
    s2client
    ${CMAKE_SOURCE_DIR}/libmysql/libmysqlclient.a
    dl
    pthread
    z
    rt
)
