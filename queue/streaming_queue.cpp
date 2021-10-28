#include <chrono>

#include "queue/streaming_queue.hpp"
#include "utils.hpp"

std::unique_ptr<StreamingQueue>
StreamingQueue::CreateChunkQueue(
    S2Client *client,
    const char *resultTableName,
    const char *selectQuery,
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
    // get the RowSchema and create Readers
    chunkQueue->m_readers.reserve(partitions.size());
    if (doesParallelRead)
    {
        try
        {
            chunkQueue->m_row_schema = client->m_conn->ExplainRowSchema(selectQuery);
        }
        catch (S2ClientError &s2err)
        {
            client->SetError(s2err);
        }
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
                    chunkQueue->m_row_schema,
                    partitions[i],
                    chunkSize));
        }
    }
    else
    {
        try
        {
            client->m_conn->Prepare(selectQuery, false);
            chunkQueue->m_row_schema = client->m_conn->GetRowSchema();
        }
        catch (S2ClientError &s2err)
        {
            client->SetError(s2err);
        }
        chunkQueue->m_readers.push_back(
            ResultTableReader::CreateReaderNonParallel(
                client->m_conn,
                chunkQueue->m_consumer_queues[0],
                selectQuery,
                chunkQueue->m_row_schema,
                chunkSize));
    }
    // start Readers
    for (auto &reader : chunkQueue->m_readers)
    {
        reader->StartReading();
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

StreamingQueue::~StreamingQueue()
{
    StopReaders();
    for (int consumer_id = 0; consumer_id < m_consumer_queues.size(); ++consumer_id)
    {
        // delete all chunks that are saved in the queue
        S2ClientError err(0, "");
        while (Chunk *c = Get(consumer_id, err))
        {
            super_chunk::utils::ChunkFree(c);
            delete c;
        }
        delete m_consumer_queues[consumer_id];
        m_consumer_queues[consumer_id] = nullptr;
    }
    super_chunk::utils::RowSchemaFree(m_row_schema);
    m_row_schema = nullptr;
}
