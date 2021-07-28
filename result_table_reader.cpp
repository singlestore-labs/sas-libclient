#include <memory>

#include "result_table_reader.hpp"

std::unique_ptr<ResultTableReader>
ResultTableReader::CreateReader(
    std::unique_ptr<S2Connection> &conn,
    ThreadSafeQueue<Chunk *> *q,
    std::shared_ptr<ChunksInfo> chunks_info,
    const char *resultTableName,
    uint32_t id,
    uint32_t partition,
    uint64_t size,
    std::shared_ptr<std::mutex> mu,
    std::shared_ptr<std::condition_variable> cv,
    bool row_schema_responsible)
{
    // allocate a ResultTableReader object
    std::unique_ptr<ResultTableReader> reader(new ResultTableReader(q, id, partition, size));

    // create a new connection
    reader->m_conn = S2Connection::Connect(conn->m_host, conn->m_port, conn->m_db, conn->m_user, conn->m_password);
    // prepare a query that will be executed
    reader->m_query = super_chunk::sql::MakeReadResultTableQuery(resultTableName, partition);

    reader->m_chunk_writer = std::make_unique<SuperChunkWriter>();
    reader->m_row_schema_mutex = mu;
    reader->m_row_schema_cv = cv;
    reader->m_row_schema_responsible = row_schema_responsible;
    reader->m_chunks_info = chunks_info;

    return reader;
}

std::unique_ptr<ResultTableReader>
ResultTableReader::CreateReaderNonParallel(
    std::unique_ptr<S2Connection> &conn,
    ThreadSafeQueue<Chunk *> *q,
    const char *query,
    uint64_t size,
    std::shared_ptr<std::mutex> mu,
    std::shared_ptr<std::condition_variable> cv,
    bool row_schema_responsible)
{
    // allocate a ResultTableReader object
    std::unique_ptr<ResultTableReader> reader(new ResultTableReader(q, 0 /*reader_id*/, 0 /*partition*/, size));

    // create a new connection
    reader->m_conn = S2Connection::Connect(conn->m_host, conn->m_port, conn->m_db, conn->m_user, conn->m_password);
    // prepare a query that will be executed
    reader->m_query = query;

    reader->m_chunk_writer = std::make_unique<SuperChunkWriter>();
    reader->m_row_schema_mutex = mu;
    reader->m_row_schema_cv = cv;
    reader->m_row_schema_responsible = row_schema_responsible;

    return reader;
}

void ResultTableReader::StartReading()
{
    m_reading_thread = std::thread(&ResultTableReader::Read, this);
}

void ResultTableReader::StopReading()
{
    m_stopReading = true;
}

void ResultTableReader::NotifyConnUnfinishedStmt()
{
    m_conn->DiscardStmtClose();
}

void ResultTableReader::Read()
{
    try
    {
        m_conn->Prepare(m_query.c_str());
    }
    catch (S2ClientError &s2_err)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = s2_err;
    }

    int has_row_schema_failed = 0;

    if (m_row_schema_responsible)
    {
        // this mutex is released when CreateChunkQueue enters "wait" state
        std::unique_lock<std::mutex> row_schema_lock(*m_row_schema_mutex.get());
        // get the row schema
        m_row_schema = m_conn->GetRowSchema(&has_row_schema_failed);
        m_row_schema_cv->notify_one();
        row_schema_lock.unlock();
    }
    else
    {
        m_row_schema = m_conn->GetRowSchema(&has_row_schema_failed);
    }

    if (m_error.m_errorCode || has_row_schema_failed)
    {
        if (!m_error.m_errorCode && has_row_schema_failed)
        {
            std::unique_lock<std::mutex> lock(m_error_mutex);
            m_error = S2ClientError(S2C_ERROR_READER_FAILED, "Failed to get row schema from result table metadata");
        }
        this->m_queue->DeleteProducer(m_reader_id);
        SetActive(false);
        return;
    }

    try
    {
        int chunkId = 0;
        while (!m_stopReading && this->m_conn->HasNextRow())
        {
            // initialize the chunk to fill
            char *ptr = (char *)malloc(m_chunk_size);
            Chunk *chunk = new Chunk();
            chunk->m_ptr = ptr;
            chunk->m_size = m_chunk_size;
            chunk->row_count = 0;
            chunk->id = chunkId;
            chunk->partition_id = m_partition;
            chunk->producer_id = m_reader_id;

            this->m_conn->NextChunk(m_chunk_writer, chunk, m_row_schema);

            if (chunk->row_count == 0)
            {
                // in case chunk is not pushed to the queue, we need to
                // free it here
                free(ptr);
                delete chunk;
                break;
            }
            this->m_queue->Push(chunk);
            // we need to store chunks info only for multi-pass queue, for streaming queue
            // m_chunks_info is null
            if (m_chunks_info)
            {
                this->m_chunks_info->Put(chunk);
            }
            ++chunkId;
        }
    }
    catch (std::invalid_argument &err)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = S2ClientError(S2C_ERROR_UNKNOWN_FAILURE, err.what());
    }
    catch (std::bad_alloc &e)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = S2ClientError(S2C_ERROR_MEMORY_ALLOCATION, "Memory allocation error");
    }
    catch (S2ClientError &s2_err)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = s2_err;
    }
    this->m_queue->DeleteProducer(m_reader_id);
    SetActive(false);
}

S2ClientError ResultTableReader::GetError()
{
    std::unique_lock<std::mutex> lock(m_error_mutex);
    return m_error;
}
