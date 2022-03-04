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
    bool doesParallelRead,
    S2ErrorCallback *cb)
{
    // allocate a ChunkQueue object
    std::unique_ptr<StreamingQueue> chunkQueue(new StreamingQueue);

    chunkQueue->m_credentials.db = client->m_conn->m_db;
    chunkQueue->m_credentials.host = client->m_conn->m_host;
    chunkQueue->m_credentials.port = client->m_conn->m_port;
    chunkQueue->m_credentials.user = client->m_conn->m_user;
    chunkQueue->m_credentials.password = client->m_conn->m_password;
    chunkQueue->m_credentials.ssl_ca = client->m_conn->m_ssl_ca;

    // we use a single dummy partition 0 in case of non-parallel read
    std::vector<int> partitions{0};
    if (doesParallelRead)
    {
        partitions = utils::WorkerPartitions(client->m_numWorkers, client->m_workerId, client->m_numPartitions);
    }
    chunkQueue->m_partition_consumer = std::vector<int>(client->m_numPartitions, -1);

    // create ThreadSafeQueue for each consumer
    for (int consumer_id = 0; consumer_id < nConsumers; ++consumer_id)
    {
        std::vector<int> consumer_partitions = utils::ConsumerPartitions(
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
        catch (S2ClientError &s2_err)
        {
            chunkQueue->m_error_before_read = true;
            client->SetError(s2_err, cb);
            return nullptr;
        }
        std::vector<AggregatorNode> aggregators;
        try
        {
            aggregators = client->m_conn->GetAggregators();
        }
        catch (S2ClientError &s2_err)
        {
            chunkQueue->m_error_before_read = true;
            client->SetError(s2_err, cb);
            return nullptr;
        }
        const Credentials masterCreds = chunkQueue->m_credentials;
        Credentials creds = masterCreds;  // we need this to fill user, password, db

        for (int partition : partitions)
        {
            utils::FillCredentials(aggregators, partition, &creds);
            // ResultTableReader will create its own connection to run queries
            chunkQueue->m_readers.push_back(
                ResultTableReader::CreateReader(
                    creds,
                    masterCreds,
                    chunkQueue->m_consumer_queues[chunkQueue->m_partition_consumer[partition]],
                    nullptr,
                    resultTableName,
                    chunkQueue->m_row_schema,
                    partition,
                    chunkSize,
                    cb));
        }
    }
    else
    {
        try
        {
            client->m_conn->Prepare(selectQuery, false);
            chunkQueue->m_row_schema = client->m_conn->GetRowSchema();
        }
        catch (S2ClientError &s2_err)
        {
            chunkQueue->m_error_before_read = true;
            client->SetError(s2_err, cb);
            return nullptr;
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
        if (!reader)
        {
            return nullptr;
        }
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
}

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
    for (uint64_t consumer_id = 0; consumer_id < m_consumer_queues.size(); ++consumer_id)
    {
        S2ClientError err(0, "");
        if (!m_error_before_read)
        {
            // delete all chunks that are saved in the queue
            while (Chunk *c = Get(consumer_id, err))
            {
                utils::ChunkFree(c);
                delete c;
            }
        }
        delete m_consumer_queues[consumer_id];
        m_consumer_queues[consumer_id] = nullptr;
    }
    utils::RowSchemaFree(m_row_schema);
    m_row_schema = nullptr;
}
