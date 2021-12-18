#include <memory>

#include "result_table_reader.hpp"

std::unique_ptr<ResultTableReader>
ResultTableReader::CreateReader(
    const Credentials &creds,
    ThreadSafeQueue<Chunk *> *q,
    std::shared_ptr<ChunksInfo> chunks_info,
    const char *resultTableName,
    RowSchema *schema,
    uint32_t partition,
    uint64_t size)
{
    // allocate a ResultTableReader object
    std::unique_ptr<ResultTableReader> reader(new ResultTableReader(q, partition, size));

    // create a new connection
    reader->m_conn = S2Connection::Connect(creds);
    // prepare a query that will be executed
    reader->m_query = sql::MakeReadResultTableQuery(resultTableName, partition);
    reader->m_row_schema = schema;
    reader->m_chunk_writer = std::make_unique<SuperChunkWriter>();
    reader->m_chunks_info = chunks_info;
    return reader;
}

std::unique_ptr<ResultTableReader>
ResultTableReader::CreateReaderNonParallel(
    std::unique_ptr<S2Connection> &conn,
    ThreadSafeQueue<Chunk *> *q,
    const char *query,
    RowSchema *schema,
    uint64_t size)
{
    // allocate a ResultTableReader object
    std::unique_ptr<ResultTableReader> reader(new ResultTableReader(q, 0 /*partition*/, size));

    // create a new connection
    reader->m_conn = S2Connection::Connect(conn->m_host, conn->m_port, conn->m_db, conn->m_user, conn->m_password);
    // prepare a query that will be executed
    reader->m_query = query;
    reader->m_row_schema = schema;
    reader->m_chunk_writer = std::make_unique<SuperChunkWriter>();

    return reader;
}

void ResultTableReader::StartReading()
{
    m_reading_thread = std::thread(&ResultTableReader::Read, this);
}

void ResultTableReader::StopReading()
{
    m_stop_reading = true;
}

void ResultTableReader::NotifyConnUnfinishedStmt()
{
    m_conn->DiscardStmtClose();
}

void ResultTableReader::Read()
{
    // if row_schema has not been provided by the queue that created the reader,
    // we stop the reading right away
    if (!m_row_schema)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = S2ClientError(S2C_ERROR_UNKNOWN_FAILURE, "Failed to get RowSchmema from the queue");

        m_queue->DeleteProducer(m_partition);
        SetActive(false);
        return;
    }

    try
    {
        m_conn->Prepare(m_query.c_str(), true);
    }
    catch (S2ClientError &s2_err)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = s2_err;
    }

    if (m_error.m_errorCode)
    {
        this->m_queue->DeleteProducer(m_partition);
        SetActive(false);
        return;
    }

    try
    {
        int chunkId = 0;
        while (!m_stop_reading && m_conn->HasNextRow())
        {
            // initialize the chunk to fill
            char *ptr = (char *)malloc(m_chunk_size);
            Chunk *chunk = new Chunk();
            chunk->m_ptr = ptr;
            chunk->m_size = m_chunk_size;
            chunk->row_count = 0;
            chunk->id = chunkId;
            chunk->partition_id = m_partition;

            m_conn->NextChunk(m_chunk_writer, chunk, m_row_schema);

            if (chunk->row_count == 0)
            {
                // in this case chunk is not pushed to the queue, we need to
                // free it here
                free(ptr);
                delete chunk;
                break;
            }
            // we need to store chunks info only for multi-pass queue, for streaming queue
            // m_chunks_info is null
            if (m_chunks_info)
            {
                m_chunks_info->Put(chunk);
            }
            m_queue->Push(chunk);
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
    m_queue->DeleteProducer(m_partition);
    SetActive(false);
}

S2ClientError ResultTableReader::GetError()
{
    std::unique_lock<std::mutex> lock(m_error_mutex);
    return m_error;
}
