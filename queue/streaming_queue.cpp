#include <chrono>

#include "queue/streaming_queue.hpp"

std::unique_ptr<StreamingQueue>
StreamingQueue::CreateChunkQueue(
    S2Client *client,
    const char *resultTableName,
    uint32_t capacity,
    uint64_t chunkSize,
    int nConsumers,
    bool doesParallelRead)
{
    // allocate a ChunkQueue object
    std::unique_ptr<StreamingQueue> chunkQueue(new StreamingQueue);
    std::shared_ptr<std::condition_variable> row_schema_cv(new std::condition_variable);
    std::shared_ptr<std::mutex> row_schema_mutex(new std::mutex);

    // we use a single dummy partition 0 in case of non-parallel read
    std::vector<int> partitions{0};
    if (doesParallelRead)
    {
        partitions =
            super_chunk::utils::WorkerPartitions(client->m_numWorkers, client->m_workerId, client->m_numPartitions);
    }
    chunkQueue->m_partition_consumer = std::vector<int>(client->m_numPartitions, -1);

    // create ThreadSafeQueue for each consumer
    for (int consumer_id = 0; consumer_id < nConsumers; ++consumer_id)
    {
        std::vector<int> consumer_partitions = super_chunk::utils::ConsumerPartitions(
            nConsumers,
            consumer_id,
            partitions);
        chunkQueue->m_consumer_queues.push_back(
            new ThreadSafeSimpleQueue<Chunk *>(capacity, consumer_partitions.size()));
        for (auto p : consumer_partitions)
        {
            chunkQueue->m_partition_consumer[p] = consumer_id;
        }
    }

    // we acquire lock before creating readers
    std::unique_lock<std::mutex> row_schema_lock(*row_schema_mutex.get());
    // create Readers
    chunkQueue->m_readers.reserve(partitions.size());

    if (doesParallelRead)
    {
        for (int i = 0; i < partitions.size(); ++i)
        {
            // client->m_conn is used to find out connection parameters.
            // ResultTableReader will create its own connection to run queries
            chunkQueue->m_readers.push_back(
                ResultTableReader::CreateReader(
                    client->m_conn,
                    chunkQueue->m_consumer_queues[chunkQueue->m_partition_consumer[partitions[i]]],
                    nullptr,
                    resultTableName,
                    partitions[i],
                    chunkSize,
                    row_schema_mutex,
                    row_schema_cv,
                    i == 0 /*row_schema_responsible*/));
        }
    }
    else
    {
        chunkQueue->m_readers.push_back(
            ResultTableReader::CreateReaderNonParallel(
                client->m_conn,
                chunkQueue->m_consumer_queues[0],
                resultTableName /*query*/,
                chunkSize,
                row_schema_mutex,
                row_schema_cv,
                true /*row_schema_responsible*/));
    }
    // start Readers
    for (auto &reader : chunkQueue->m_readers)
    {
        reader->StartReading();
    }

    if (chunkQueue->m_readers.empty())
    {
        return chunkQueue;
    }

    while ((chunkQueue->m_row_schema = chunkQueue->m_readers.front()->GetRowSchema()) == nullptr &&
           !chunkQueue->m_readers.front()->GetError().m_errorCode)
    {
        row_schema_cv->wait_for(row_schema_lock, std::chrono::seconds(10));
    }
    if (chunkQueue->m_row_schema == nullptr)
    {
        if (chunkQueue->m_readers.front()->GetError().m_errorCode)
        {
            client->SetError(chunkQueue->m_readers.front()->GetError());
        }
        else
        {
            client->SetError(
                S2ClientError(S2C_ERROR_READER_FAILED, "Failed to fetch row schema from the table reader"));
        }
    }
    return chunkQueue;
}

Chunk *
StreamingQueue::GetById(
    int partiotionId,
    int chunkId,
    S2ClientError &error)
{
    SetError(S2ClientError(S2C_ERROR_INV_ARG, "Cannot use streaming queue in multi-pass mode"));
    return nullptr;
};

Chunk *
StreamingQueue::GetSingleRow(
    uint32_t partitionId,
    uint32_t chunkId,
    int64_t rowNum,
    int threadId,
    S2ClientError &error)
{
    SetError(S2ClientError(S2C_ERROR_INV_ARG, "Cannot use streaming queue in random read mode"));
    return nullptr;
}
