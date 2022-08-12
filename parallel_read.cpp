#include "queue/multi_pass_queue.hpp"
#include "queue/streaming_queue.hpp"
#include "queue/chunk_queue.hpp"

extern "C"
{
    ParallelReadType
    ParallelReadInit(
        S2Client *client,
        const char *resultTableName,
        const char *selectQuery,
        const char *sourceTable,
        const char *keyColumnName,
        ParallelReadType readType,
        bool materialized,
        const char *const *const partitionByCols,
        int partitionByColsNumber,
        const char *const *const partitionOrderByCols,
        const int partitionOrderByColsNumber)
    {
        // clear the previous error if any
        client->SetError(S2ClientError(0, ""), nullptr);
        try
        {
            readType = client->m_conn->GetParallelReadType(
                selectQuery,
                sourceTable,
                keyColumnName,
                materialized,
                partitionByCols,
                partitionByColsNumber,
                partitionOrderByCols,
                partitionOrderByColsNumber,
                readType);
            std::string newQuery;
            TableType tableType;
            if (readType == ReadTypeOriginalTable)
            {
                // if we read from original table, we don't need to create result or columnstore table
                return ReadTypeOriginalTable;
            }
            if (readType == ReadTypeColumnStoreTable)
            {
                tableType = TableType::RegularTable;
            }
            else
            {
                tableType = materialized ? TableType::AggResultTableMaterialized : TableType::AggResultTable;
            }
            // TODO: future remove error below when Agg Materialized Result Table is allowed
            if (tableType == TableType::AggResultTableMaterialized)
            {
                client->SetError(S2ClientError(S2C_ERROR_INV_ARG, "Cannot use Materialized Result Table"), nullptr);
            }
            newQuery = sql::MakeCreateTableQuery(
                resultTableName,
                selectQuery,
                keyColumnName,
                tableType,
                partitionByCols,
                partitionByColsNumber,
                partitionOrderByCols,
                partitionOrderByColsNumber);
            client->m_conn->ExecuteDDL(newQuery);
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err, nullptr);
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"), nullptr);
        }
        return readType;
    }

    void
    ParallelReadFree(
        S2Client *client,
        const char *resultTableName,
        ParallelReadType readType,
        S2ErrorCallback *cb)
    {
        // clear the previous error if any
        client->SetError(S2ClientError(0, ""), nullptr);
        try
        {
            std::string dropQuery;
            if (readType == ReadTypeResultTable)
            {
                dropQuery = sql::MakeDropResultQuery(resultTableName);
            }
            else if (readType == ReadTypeColumnStoreTable)
            {
                dropQuery = sql::MakeDropTableQuery(resultTableName);
            }
            else
            {
                return;
            }
            client->m_conn->ExecuteDDL(dropQuery);
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err, cb);
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"), cb);
        }
    }

    ChunkQueue *
    ParallelReadGetQueue(
        S2Client *client,
        const char *resultTableName,
        const char *selectQuery,
        const char *sourceTable,
        const char *keyColumnName,
        ParallelReadType readType,
        const char *const *const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        uint64_t chunkSize,
        int queueCapacity,
        int nReaderThreads,
        bool isMultiPass,
        S2ErrorCallback *cb)
    {
        // clear the previous error if any
        client->SetError(S2ClientError(0, ""), nullptr);
        try
        {
            if (isMultiPass)
            {
                return (ChunkQueue *)MultiPassQueue::CreateChunkQueue(
                           client,
                           resultTableName,
                           selectQuery,
                           sourceTable,
                           keyColumnName,
                           readType,
                           partitionOrderByCols,
                           partitionOrderByColsNumber,
                           queueCapacity,
                           chunkSize,
                           nReaderThreads,
                           cb)
                    .release();
            }
            return (ChunkQueue *)StreamingQueue::CreateChunkQueue(
                       client,
                       resultTableName,
                       selectQuery,
                       sourceTable,
                       keyColumnName,
                       readType,
                       partitionOrderByCols,
                       partitionOrderByColsNumber,
                       queueCapacity,
                       chunkSize,
                       nReaderThreads,
                       true /* doesParallelRead */,
                       cb)
                .release();
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err, cb);
            return nullptr;
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"), cb);
            return nullptr;
        }
    }

    ChunkQueue *
    QueryGetQueue(
        S2Client *client,
        const char *query,
        uint64_t chunkSize,
        int queueCapacity,
        S2ErrorCallback *cb)
    {
        // clear the previous error if any
        client->SetError(S2ClientError(0, ""), nullptr);
        try
        {
            return StreamingQueue::CreateChunkQueue(
                       client,
                       NULL,
                       query,
                       NULL,
                       NULL,
                       ReadTypeResultTable,  /* this is not used when doesParallelRead is false */
                       NULL,
                       0,
                       queueCapacity,
                       chunkSize,
                       1,
                       false /* doesParallelRead */,
                       cb)
                .release();
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err, cb);
            return nullptr;
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"), cb);
            return nullptr;
        }
    }

    bool
    GetNextChunk(
        ChunkQueue *queue,
        int consumerId,
        uint32_t *partitionId /*out*/,
        Chunk *chunk /*out*/,
        S2ErrorCallback *cb)
    {
        S2ClientError err(0, "");
        Chunk *res = queue->Get(consumerId, err);
        if (err.m_errorCode)
        {
            cb->setError(cb, err.m_errorCode, std::move(err.m_errorMessage).c_str(), S2C_SEVERITY_ERROR);
        }
        if (!res)
        {
            return false;
        }

        *partitionId = res->partition_id;
        utils::MoveChunk(chunk, res);

        return true;
    }

    bool
    GetChunkMulti(
        ChunkQueue *queue,
        uint32_t partitionId,
        uint32_t chunkId,
        Chunk *chunk /*out*/,
        S2ErrorCallback *cb)
    {
        S2ClientError err(0, "");
        Chunk *res = queue->GetById(partitionId, chunkId, err);

        if (err.m_errorCode)
        {
            cb->setError(cb, err.m_errorCode, std::move(err.m_errorMessage).c_str(), S2C_SEVERITY_ERROR);
        }
        if (!res)
        {
            return false;
        }

        utils::MoveChunk(chunk, res);

        return true;
    }

    bool
    GetChunkRow(
        ChunkQueue *queue,
        uint32_t partitionId,
        int64_t rowId,
        int threadId,
        Chunk *chunk /*out*/,
        S2ErrorCallback *cb)
    {
        S2ClientError err(0, "");
        Chunk *res = queue->GetSingleRow(partitionId, rowId, threadId, err);

        if (err.m_errorCode)
        {
            cb->setError(cb, err.m_errorCode, std::move(err.m_errorMessage).c_str(), S2C_SEVERITY_ERROR);
        }
        if (!res)
        {
            return false;
        }

        utils::MoveChunk(chunk, res);

        return true;
    }

    bool
    GetChunkMultipleRows(
        ChunkQueue *queue,
        uint32_t partitionId,
        int64_t* rowIds,
        int64_t rowIdsNum,
        int threadId,
        Chunk *chunk /*out*/,
        uint64_t chunkSize,
        S2ErrorCallback *cb)
    {
        S2ClientError err(0, "");
        Chunk *res = queue->GetMultipleRows(chunkSize, partitionId, rowIds, rowIdsNum, threadId, err);

        if (err.m_errorCode)
        {
            cb->setError(cb, err.m_errorCode, std::move(err.m_errorMessage).c_str(), S2C_SEVERITY_ERROR);
        }
        if (!res)
        {
            return false;
        }

        utils::MoveChunk(chunk, res);

        return true;
    }

    RowSchema *GetRowSchema(ChunkQueue *queue)
    {
        return queue->GetRowSchema();
    }

    void ChunkQueueFree(ChunkQueue *queue)
    {
        delete queue;
    }
};
