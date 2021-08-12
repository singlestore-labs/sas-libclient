#include <vector>

#include "queue/multi_pass_queue.hpp"

#include "result_table_reader.hpp"
#include "queue/thread_safe_batch_queue.hpp"

std::unique_ptr<MultiPassQueue>
MultiPassQueue::CreateChunkQueue(
    S2Client *client,
    const char *resultTableName,
    uint32_t capacity,
    uint64_t chunkSize,
    int nConsumers)
{
    // allocate a ChunkQueue object
    std::unique_ptr<MultiPassQueue> chunkQueue(new MultiPassQueue);
    std::shared_ptr<std::condition_variable> row_schema_cv(new std::condition_variable);
    std::shared_ptr<std::mutex> row_schema_mutex(new std::mutex);

    chunkQueue->m_credentials.db = client->m_conn->m_db;
    chunkQueue->m_credentials.host = client->m_conn->m_host;
    chunkQueue->m_credentials.port = client->m_conn->m_port;
    chunkQueue->m_credentials.user = client->m_conn->m_user;
    chunkQueue->m_credentials.password = client->m_conn->m_password;

    chunkQueue->m_result_table = resultTableName;

    std::vector<int> partitions = super_chunk::utils::WorkerPartitions(
        client->m_numWorkers,
        client->m_workerId,
        client->m_numPartitions);

    // initialize the object to store chunk sizes
    std::shared_ptr<ChunksInfo> chunks_info(new ChunksInfo(partitions));
    chunkQueue->m_chunks_info = chunks_info;
    chunkQueue->m_partition_consumer = std::vector<int>(client->m_numPartitions, -1);

    // create ThreadSafeBatchQueue for each consumer
    for (int consumer_id = 0; consumer_id < nConsumers; ++consumer_id)
    {
        std::vector<int> consumer_partitions = super_chunk::utils::ConsumerPartitions(
            nConsumers,
            consumer_id,
            partitions);
        chunkQueue->m_consumer_queues.push_back(new ThreadSafeBatchQueue<Chunk *>(capacity, consumer_partitions));
        for (auto p : consumer_partitions)
        {
            chunkQueue->m_partition_consumer[p] = consumer_id;
        }
    }
    // we acquire lock before creating readers
    std::unique_lock<std::mutex> row_schema_lock(*row_schema_mutex.get());
    // create Readers
    chunkQueue->m_readers.reserve(partitions.size());
    for (int i = 0; i < partitions.size(); ++i)
    {
        // client->m_conn is used to find out connection parameters.
        // ResultTableReader will create its own connection to run queries
        chunkQueue->m_readers.push_back(
            ResultTableReader::CreateReader(
                client->m_conn,
                chunkQueue->m_consumer_queues[chunkQueue->m_partition_consumer[partitions[i]]],
                chunkQueue->m_chunks_info,
                resultTableName,
                partitions[i],
                chunkSize,
                row_schema_mutex,
                row_schema_cv,
                i == 0 /*row_schema_responsible*/));
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

    chunkQueue->m_consumer_threads_num = nConsumers;
    chunkQueue->m_consumers.reserve(nConsumers);
    for (int i = 0; i < nConsumers; ++i)
    {
        std::unique_ptr<S2Connection> tmp = std::unique_ptr<S2Connection>(nullptr);
        chunkQueue->m_consumers.emplace_back(std::move(tmp), std::make_unique<SuperChunkWriter>());
    }

    return chunkQueue;
};

// GetById retrieves the Chunk number chunkId read from partiotionId
Chunk *
MultiPassQueue::GetById(
    int partitionId,
    int chunkId,
    S2ClientError &err)
{
    err = S2ClientError(0, "");

    try
    {
        return m_consumer_queues[m_partition_consumer[partitionId]]->Get(partitionId, chunkId);
    }
    catch (std::out_of_range &ex)
    {
        for (std::unique_ptr<ResultTableReader> &r : m_readers)
        {
            S2ClientError curError = r->GetError();
            if (curError.m_errorCode)
            {
                err.m_errorMessage += " - " + curError.m_errorMessage + "\n";
                err.m_errorCode = S2C_ERROR_READER_FAILED;
            }
        }

        if (err.m_errorCode)
        {
            err.m_errorMessage = "Some readers failed:\n" + err.m_errorMessage;
            SetError(err);
        }
        return nullptr;
    }
}

Chunk *
MultiPassQueue::GetSingleRow(
    uint32_t partitionId,
    uint32_t chunkId,
    int64_t rowNum,
    int threadId,
    S2ClientError &err)
{
    err = S2ClientError(0, "");
    if (threadId >= m_consumer_threads_num)
    {
        err.m_errorCode = S2C_ERROR_INV_ARG;
        err.m_errorMessage = "threadId " + std::to_string(threadId) + " is too large";
        return nullptr;
    }
    if (!m_consumers[threadId].conn)
    {
        m_consumers[threadId].conn = S2Connection::Connect(m_credentials);
        m_consumers[threadId].writer = std::make_unique<SuperChunkWriter>();
    }

    try
    {
        int partitionRowId = m_chunks_info->PartitionRowId(partitionId, chunkId, rowNum);
        return m_consumers[threadId].conn->GetSingleRow(
            m_consumers[threadId].writer.get(),
            m_row_schema,
            m_result_table,
            partitionId,
            partitionRowId);
    }
    catch (S2ClientError &s2_err)
    {
        err.m_errorCode = s2_err.m_errorCode;
        err.m_errorMessage = s2_err.m_errorMessage;
    }
    return nullptr;
}
