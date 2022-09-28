#ifndef QUEUE_MULTI_PASS_QUEUE_HPP
#define QUEUE_MULTI_PASS_QUEUE_HPP

#include <vector>

#include "queue/chunk_queue.hpp"

#include "result_table_reader.hpp"
#include "queue/thread_safe_batch_queue.hpp"

struct ConnAndWriter
{
    std::unique_ptr<S2Connection> conn;
    std::unique_ptr<SuperChunkWriter> writer;

    // this constructor is used to initialize a vector of ConnAndWriter objects
    ConnAndWriter(std::unique_ptr<S2Connection> c, std::unique_ptr<SuperChunkWriter> w)
    {
        conn = std::move(c);
        writer = std::move(w);
    }
};

class MultiPassQueue : public ChunkQueue
{
  private:
    MultiPassQueue() = default;

    int m_consumer_threads_num;
    std::vector<ConnAndWriter> m_consumers;

    std::string m_serverVersion;

  public:
    ~MultiPassQueue();

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
        const char *selectQuery,
        const char *sourceTable,
        const char *keyColumnName,
        ParallelReadType readType,
        const char *const *const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        uint32_t capacity,
        uint64_t chunkSize,
        int nReaderThreads,
        S2ErrorCallback *cb);

    Chunk *
    GetById(
        int partiotionId,
        int chunkId,
        S2ClientError &error);

    Chunk *
    GetSingleRow(
        uint32_t partitionId,
        int64_t rowId,
        int threadId,
        S2ClientError &error);

    Chunk *
    GetMultipleRows(
        uint64_t chunkSize,
        uint32_t partitionId,
        int64_t *rowIds,
        int64_t rowIdsNum,
        int threadId,
        S2ClientError &error);
};

#endif  // QUEUE_MULTI_PASS_QUEUE_HPP
