#ifndef PARALLEL_READ_HPP
#define PARALLEL_READ_HPP

#include "chunk_extern.h"
#include "queue/chunk_queue.hpp"
#include "s2_client.hpp"
#include "utils.hpp"

extern "C"
{
    void
    ParallelReadInit(
        S2Client* client,
        const char* resultTableName,
        const char* selectQuery,
        bool materialized);

    void
    ParallelReadFree(
        S2Client* con,
        const char* resultTableName);

    ChunkQueue*
    ParallelReadGetQueue(
        S2Client* client,
        const char* resultTableName,
        uint64_t chunkSize,
        int queueCapacity,
        bool isMultiPass);

    ChunkQueue*
    QueryGetQueue(
        S2Client* client,
        const char* query,
        uint64_t chunkSize,
        int queueCapacity);

    bool
    GetNextChunk(
        ChunkQueue* queue,
        uint32_t* partitionId /*out*/,
        Chunk* chunk /*out*/,
        S2ErrorCallback* cb);

    bool
    GetChunkMulti(
        ChunkQueue* queue,
        uint32_t partitionId,
        uint32_t chunkId,
        Chunk* chunk /*out*/,
        S2ErrorCallback* cb);

    void ChunkQueueFree(ChunkQueue* queue);
}

#endif  // PARALLEL_READ_HPP
