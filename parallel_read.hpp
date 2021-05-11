#ifndef PARALLEL_READ_HPP
#define PARALLEL_READ_HPP

#include "s2_client.hpp"
#include "chunk_queue.hpp"

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
        int queueCapacity);

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
        int* err);

    void ChunkQueueFree(ChunkQueue* queue);
}

#endif  // PARALLEL_READ_HPP