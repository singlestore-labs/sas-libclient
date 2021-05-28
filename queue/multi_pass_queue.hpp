#ifndef MULTI_PASS_QUEUE_HPP
#define MULTI_PASS_QUEUE_HPP

#include <vector>

#include "queue/chunk_queue.hpp"

#include "result_table_reader.hpp"
#include "queue/thread_safe_batch_queue.hpp"


class MultiPassQueue : public ChunkQueue
{
  private:
    MultiPassQueue()
        {};

  public:
    // CreateChunkQueue creates a MultiPassQueue object,
    // creates Readers and starts them.
    // - capacity is the maximum number of chunks that can be saved in the queue
    // by each of the result table readers. This is different from StreamingQueue
    // where capacity denotes the total number of chunks in the queue.
    // - chunkSize is the size of one Chunk in bytes
    static std::unique_ptr<MultiPassQueue>
    CreateChunkQueue(
        S2Client *client,
        const char *resultTableName,
        uint32_t capacity,
        uint64_t chunkSize);

    // Get retrieves the Chunk number chunkId read from partiotionId
    Chunk *
    GetById(
        int partiotionId,
        int chunkId,
        S2ClientError &error);
};

#endif  // MULTI_PASS_QUEUE_HPP
