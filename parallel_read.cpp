#include "queue/multi_pass_queue.hpp"
#include "queue/streaming_queue.hpp"
#include "queue/chunk_queue.hpp"

extern "C"
{
    void
    ParallelReadInit(
        S2Client *client,
        const char *resultTableName,
        const char *selectQuery,
        bool materialized,
        const char *const *const partitionByCols,
        int partitionByColsNumber,
        const char *const *const partitionOrderByCols,
        const int orderByColsNumber)
    {
        // clear the previous error if any
        client->SetError(S2ClientError(0, ""), nullptr);
        try
        {
            std::string newQuery = sql::MakeCreateResultTableQuery(
                resultTableName,
                selectQuery,
                materialized,
                partitionByCols,
                partitionByColsNumber,
                partitionOrderByCols,
                orderByColsNumber);
            client->m_conn->ExecuteDDL(std::move(newQuery));
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err, nullptr);
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"), nullptr);
        }
    }

    void
    ParallelReadFree(
        S2Client *client,
        const char *resultTableName,
        S2ErrorCallback *cb)
    {
        // clear the previous error if any
        client->SetError(S2ClientError(0, ""), nullptr);
        try
        {
            std::string dropQuery = sql::MakeDropQuery(resultTableName);
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
                       nullptr,
                       query,
                       queueCapacity,
                       chunkSize,
                       1,  // TODO: check if we need thread affinity here
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
        uint32_t chunkId,
        int64_t rowNum,
        int threadId,
        Chunk *chunk /*out*/,
        S2ErrorCallback *cb)
    {
        S2ClientError err(0, "");
        Chunk *res = queue->GetSingleRow(partitionId, chunkId, rowNum, threadId, err);

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
