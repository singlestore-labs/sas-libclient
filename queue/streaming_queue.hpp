#ifndef STREAMING_QUEUE_HPP
#define STREAMING_QUEUE_HPP

#include <vector>

#include "queue/chunk_queue.hpp"
#include "result_table_reader.hpp"
#include "queue/thread_safe_queue.hpp"
#include "queue/thread_safe_simple_queue.hpp"

class StreamingQueue : public ChunkQueue
{
  private:
    StreamingQueue()
        {};

  public:
    ~StreamingQueue();

    // CreateChunkQueue creates a StreamingQueue object,
    // creates Readers and starts them.
    // - capacity is the maximum number of chunks that can be stored in the queue
    // - chunkSize is the size of one Chunk in bytes
    static std::unique_ptr<StreamingQueue>
    CreateChunkQueue(
        S2Client *client,
        const char *resultTableName,
        uint32_t capacity,
        uint64_t chunkSize,
        int nReaderThreads,
        bool doesParallelRead);

    Chunk *
    GetById(
        int partiotionId,
        int chunkId,
        S2ClientError &error);

    Chunk *
    GetSingleRow(
        uint32_t partitionId,
        uint32_t chunkId,
        int64_t rowNum,
        int threadId,
        S2ClientError &error);
};

#endif  // STREAMING_QUEUE_HPP
