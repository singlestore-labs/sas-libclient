#include "chunk_queue.hpp"
#include <chrono>

std::unique_ptr<ChunkQueue>
ChunkQueue::CreateChunkQueue(
    S2Client *client,
    const char *resultTableName,
    uint32_t capacity,
    uint64_t chunkSize)
{
    // allocate a ChunkQueue object
    std::unique_ptr<ChunkQueue> chunkQueue(new ChunkQueue());
    std::shared_ptr<std::condition_variable> row_schema_cv(new std::condition_variable);
    std::shared_ptr<std::mutex> row_schema_mutex(new std::mutex);

    // create ThreadSafeQueue
    std::vector<int> partitions =
        super_chunk::utils::AssignedPartitions(client->m_numWorkers, client->m_workerId, client->m_numPartitions);
    chunkQueue->m_queue = new ThreadSafeQueue<PartitionChunk *>(capacity, partitions.size());

    // we acquire lock before creating readers
    std::unique_lock<std::mutex> row_schema_lock(*row_schema_mutex.get());
    // create Readers
    chunkQueue->m_readers.reserve(partitions.size());
    for (int i=0; i < partitions.size(); ++i)
    {
        // client->m_conn is used to find out connection parameters.
        // ResultTableReader will create its own connection to run queries
        chunkQueue->m_readers.push_back(
            ResultTableReader::CreateReader(
                client->m_conn,
                chunkQueue->m_queue,
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

    if (chunkQueue->m_readers.empty()) {
        return chunkQueue;
    }

    while ((chunkQueue->m_row_schema = chunkQueue->m_readers.front()->GetRowSchema()) == nullptr)
    {
        row_schema_cv->wait_for(row_schema_lock, std::chrono::seconds(10));
    }
    if (chunkQueue->m_row_schema == nullptr)
    {
        client->SetError(
            S2ClientError(S2C_ERROR_READER_FAILED, "Failed to fetch RowSchema from table reader after 10 seconds")
            );
    }
    return chunkQueue;
}

PartitionChunk *ChunkQueue::Get(S2ClientError &err)
{
    err = S2ClientError(0, "");
    try
    {
        return m_queue->Pop();
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

ChunkQueue::~ChunkQueue()
{
    for (auto &reader : m_readers)
    {
        reader->StopReading();
    }
    if (m_queue)
    {
        // delete all chunks that are saved in queue
        S2ClientError err(0, "");
        while (PartitionChunk *pc = Get(err))
        {
            if (pc)
            {
                if (pc->chunk)
                {
                    super_chunk::utils::ChunkFree(pc->chunk.get());
                }
                delete pc;
            }
        }
        delete m_queue;
        m_queue = nullptr;
    }
};

extern "C"
{
    RowSchema *GetRowSchema(ChunkQueue *cq)
    {
        return cq->GetRowSchema();
    }
}
