#ifndef CHUNK_QUEUE_HPP
#define CHUNK_QUEUE_HPP

#include <vector>

#include "result_table_reader.hpp"
#include "thread_safe_queue.hpp"

using super_chunk::structs::PartitionChunk;

// ChunkQueue is responsible for reading data in chunks from S2
// Instance of this class is created for each worker
// ChunkQueue creates none or several readers
// Each reader reads chunks from its partition
// and adds them to the global thread safe queue
class ChunkQueue
{
  public:
    ~ChunkQueue();

    // CreateChunkQueue creates a ChunkQueue object,
    // creates Readers and starts them
    // capacity is a maximum number of chunks that ca be saved in the queue
    // chunkSize is a maximum size of one Chunk in bytes
    static std::unique_ptr<ChunkQueue>
    CreateChunkQueue(
        S2Client *client,
        const char *resultTableName,
        uint32_t capacity,
        uint64_t chunkSize,
        bool doesParallelRead);

    // Get retrieves one PartitionChunk from the thread safe queue
    PartitionChunk *Get(S2ClientError &error);

    RowSchema *GetRowSchema()
    {
        return this->m_row_schema;
    }

    void SetError(S2ClientError err)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = std::move(err);
    }

  private:
    ChunkQueue()
        :
        m_error(0, "")
    {
    }

    std::vector<std::unique_ptr<ResultTableReader>> m_readers;
    ThreadSafeQueue<PartitionChunk *> *m_queue;
    RowSchema *m_row_schema = nullptr;
    S2ClientError m_error;
    std::mutex m_error_mutex;

};

#endif  // CHUNK_QUEUE_HPP
