#include "parallel_read.hpp"

extern "C"
{
    void
    ParallelReadInit(
        S2Client *client,
        const char *resultTableName,
        const char *selectQuery,
        bool materialized)
    {
        client->SetError(S2ClientError(0, ""));
        try
        {
            std::string newQuery =
                super_chunk::sql::MakeCreateResultTableQuery(resultTableName, selectQuery, materialized);
            client->m_conn->ExecuteDDL(std::move(newQuery));
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err);
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"));
        }
    }

    void
    ParallelReadFree(
        S2Client *client,
        const char *resultTableName)
    {
        client->SetError(S2ClientError(0, ""));
        try
        {
            std::string dropQuery = super_chunk::sql::MakeDropQuery(resultTableName);
            client->m_conn->ExecuteDDL(dropQuery);
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err);
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"));
        }
    }

    ChunkQueue *
    ParallelReadGetQueue(
        S2Client *client,
        const char *resultTableName,
        uint64_t chunkSize,
        int queueCapacity)
    {
        client->SetError(S2ClientError(0, ""));
        try
        {
            return ChunkQueue::CreateChunkQueue(client, resultTableName, queueCapacity, chunkSize, true /* doesParallelRead */).release();
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err);
            return nullptr;
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"));
            return nullptr;
        }
    }

    ChunkQueue*
    QueryGetQueue(
        S2Client* client,
        const char* query,
        uint64_t chunkSize,
        int queueCapacity)
    {
        client->SetError(S2ClientError(0, ""));
        try
        {
            return ChunkQueue::CreateChunkQueue(client, query, queueCapacity, chunkSize, false /* doesParallelRead */).release();
        }
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err);
            return nullptr;
        }
        catch (std::bad_alloc &)
        {
            client->SetError(S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Failed to allocate memory"));
            return nullptr;
        }
    }

    bool
    GetNextChunk(
        ChunkQueue *queue,
        uint32_t *partitionId /*out*/,
        Chunk *chunk /*out*/,
        S2ErrorCallback *cb)
    {
        S2ClientError err(0, "");
        PartitionChunk *res = queue->Get(err);
        if (err.m_errorCode)
        {
            cb->setError(cb, err.m_errorCode, err.m_errorMessage.c_str());
        }
        if (!res)
        {
            return false;
        }

        *partitionId = res->partitionId;
        chunk->m_ptr = res->chunk->m_ptr;
        chunk->m_size = res->chunk->m_size;
        chunk->row_count = res->chunk->row_count;

        delete res;

        return true;
    }

    void ChunkQueueFree(ChunkQueue *queue)
    {
        delete queue;
    }
};
