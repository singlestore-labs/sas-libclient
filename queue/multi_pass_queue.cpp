#include <vector>

#include "result_table_reader.hpp"
#include "queue/multi_pass_queue.hpp"
#include "queue/thread_safe_batch_queue.hpp"
#include "utils.hpp"

std::unique_ptr<MultiPassQueue>
MultiPassQueue::CreateChunkQueue(
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
    int nConsumers,
    S2ErrorCallback *cb)
{
    // allocate a ChunkQueue object
    std::unique_ptr<MultiPassQueue> chunkQueue(new MultiPassQueue);

    chunkQueue->m_credentials.db = client->m_conn->m_db;
    chunkQueue->m_credentials.host = client->m_conn->m_host;
    chunkQueue->m_credentials.port = client->m_conn->m_port;
    chunkQueue->m_credentials.user = client->m_conn->m_user;
    chunkQueue->m_credentials.password = client->m_conn->m_password;
    chunkQueue->m_credentials.ssl_ca = client->m_conn->m_ssl_ca;

    chunkQueue->m_result_table = resultTableName;
    chunkQueue->m_query = selectQuery;
    chunkQueue->m_key_column = keyColumnName;
    chunkQueue->m_read_type = readType;
    chunkQueue->m_serverVersion = client->m_serverVersion;

    std::vector<int> partitions = utils::WorkerPartitions(
        client->m_numWorkers,
        client->m_workerId,
        client->m_numPartitions);

    chunkQueue->m_partition_consumer = std::vector<int>(client->m_numPartitions, -1);

    // create ThreadSafeBatchQueue for each consumer
    for (int consumer_id = 0; consumer_id < nConsumers; ++consumer_id)
    {
        std::vector<int> consumer_partitions = utils::ConsumerPartitions(
            nConsumers,
            consumer_id,
            partitions);
        chunkQueue->m_consumer_queues.push_back(new ThreadSafeBatchQueue<Chunk *>(capacity, consumer_partitions));
        for (auto p : consumer_partitions)
        {
            chunkQueue->m_partition_consumer[p] = consumer_id;
        }
    }
    // get the RowSchema
    try
    {
        chunkQueue->m_row_schema = client->m_conn->ExplainRowSchema(selectQuery);
    }
    catch (S2ClientError &s2_err)
    {
        client->SetError(s2_err, cb);

        return nullptr;
    }
    try
    {
        chunkQueue->m_aggregators = client->m_conn->GetAggregators();
    }
    catch (S2ClientError &s2_err)
    {
        client->SetError(s2_err, cb);

        return nullptr;
    }
    const Credentials masterCreds = chunkQueue->m_credentials;
    Credentials creds = masterCreds;  // we need this to fill user, password, db

    // create Readers
    chunkQueue->m_readers.reserve(partitions.size());
    std::string readQuery;
    TableKeys columnstoreKeys;
    if (readType == ReadTypeOriginalTable)
    {
        columnstoreKeys = client->m_conn->GetTableKeys(sourceTable);
    }
    for (int partition : partitions)
    {
        switch (readType)
        {
            case ReadTypeResultTable:
                readQuery = sql::MakeReadResultTableQuery(resultTableName, partition, client->m_serverVersion);
                break;
            case ReadTypeColumnStoreTable:
                readQuery = sql::MakeReadColumnStoreTableQuery(
                    resultTableName,
                    keyColumnName,
                    partitionOrderByCols,
                    partitionOrderByColsNumber,
                    partition);
                break;
            case ReadTypeOriginalTable:
                readQuery = sql::MakeReadOriginalTableQuery(
                    selectQuery,
                    &(columnstoreKeys.sort_key),
                    partitionOrderByCols,
                    partitionOrderByColsNumber,
                    partition);
                break;
            default:
                return nullptr;
        }
        utils::FillCredentials(chunkQueue->m_aggregators, partition, &creds);
        // ResultTableReader will create its own connection to read from partition
        auto reader = ResultTableReader::CreateReader(
            creds,
            masterCreds,
            chunkQueue->m_consumer_queues[chunkQueue->m_partition_consumer[partition]],
            readQuery,
            chunkQueue->m_row_schema,
            partition,
            chunkSize,
            cb);
        if (!reader) return nullptr;
        chunkQueue->m_readers.push_back(std::move(reader));
    }
    chunkQueue->m_error_before_read = false;
    // start Readers
    for (auto &reader : chunkQueue->m_readers)
    {
        reader->StartReading();
    }
    if (chunkQueue->m_readers.empty())
    {
        return chunkQueue;
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

// GetById retrieves the Chunk number chunkId read from partitionId
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
    int64_t rowId,
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
        Credentials creds = m_credentials;
        utils::FillCredentials(m_aggregators, partitionId, &creds);
        m_consumers[threadId].conn = S2Connection::ConnectWithRetryMA(creds, m_credentials, err);
        if (err.m_errorCode)
        {
            return nullptr;
        }
        m_consumers[threadId].writer = std::make_unique<SuperChunkWriter>();
    }

    try
    {
        Chunk *res = m_consumers[threadId].conn->GetSingleRow(
            m_consumers[threadId].writer.get(),
            m_row_schema,
            m_result_table,
            m_query,
            m_key_column,
            partitionId,
            rowId,
            m_read_type,
            m_serverVersion);
        return res;
    }
    catch (S2ClientError &s2_err)
    {
        err.m_errorCode = s2_err.m_errorCode;
        err.m_errorMessage = s2_err.m_errorMessage;
    }
    return nullptr;
}

Chunk *
MultiPassQueue::GetMultipleRows(
    uint64_t chunkSize,
    uint32_t partitionId,
    int64_t *rowIds,
    int64_t rowIdsNum,
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
        Credentials creds = m_credentials;
        utils::FillCredentials(m_aggregators, partitionId, &creds);
        m_consumers[threadId].conn = S2Connection::ConnectWithRetryMA(creds, m_credentials, err);
        if (err.m_errorCode)
        {
            return nullptr;
        }
        m_consumers[threadId].writer = std::make_unique<SuperChunkWriter>();
    }

    try
    {
        // initialize the chunk to fill
        char *ptr = (char *)malloc(chunkSize);
        if (!ptr)
        {
            err.m_errorCode = S2C_ERROR_MEMORY_ALLOCATION;
            err.m_errorMessage = "GetMultipleRows cannot allocate chunk size: " + std::to_string(chunkSize);
            return nullptr;
        }
        Chunk *chunk = new Chunk();
        chunk->m_ptr = ptr;
        chunk->m_size = chunkSize;
        chunk->row_count = 0;
        chunk->id = 0;
        chunk->partition_id = partitionId;
        m_consumers[threadId].conn->GetMultipleRows(
            m_consumers[threadId].writer.get(),
            chunk,
            m_row_schema,
            m_result_table,
            m_query,
            m_key_column,
            partitionId,
            rowIds,
            rowIdsNum,
            m_read_type);
        return chunk;
    }
    catch (S2ClientError &s2_err)
    {
        err.m_errorCode = s2_err.m_errorCode;
        err.m_errorMessage = s2_err.m_errorMessage;
    }
    return nullptr;
}

MultiPassQueue::~MultiPassQueue()
{
    StopReaders();
    for (uint64_t consumer_id = 0; consumer_id < m_consumer_queues.size(); ++consumer_id)
    {
        // delete all chunks that are saved in the queue
        if (!m_error_before_read)
        {
            m_consumer_queues[consumer_id]->FreeBatchData(&utils::ChunkFree);
        }
        delete m_consumer_queues[consumer_id];
        m_consumer_queues[consumer_id] = nullptr;
    }
    utils::RowSchemaFree(m_row_schema);
    m_row_schema = nullptr;
}
