#include <memory>

#include "result_table_reader.hpp"

std::unique_ptr<ResultTableReader>
ResultTableReader::CreateReader(
    const Credentials &creds,
    const Credentials &masterCreds,
    ThreadSafeQueue<Chunk *> *q,
    std::string readQuery,
    RowSchema *schema,
    uint32_t partition,
    uint64_t size,
    S2ErrorCallback *cb)
{
    // allocate a ResultTableReader object
    std::unique_ptr<ResultTableReader> reader(new ResultTableReader(q, partition, size));

    // create a new connection
    try
    {
        reader->m_conn = S2Connection::Connect(creds);
    }
    catch (S2ClientError &s2_err)
    {
        std::string error = "Cannot connect using data from information_schema.mv_nodes: ";
        error += std::string(s2_err.what()) + " partition: " + std::to_string(partition) + "\n";
        cb->setError(cb, S2C_ERROR_BAD_CONNECTION, std::move(error).c_str(), S2C_SEVERITY_WARNING);
        try
        {
            reader->m_conn = S2Connection::Connect(masterCreds);
        }
        catch (S2ClientError &s2_err)
        {
            std::string error = "Cannot connect to server using data from CAS worker's S2Client: ";
            error += s2_err.what();
            error += "\n";
            cb->setError(cb, S2C_ERROR_BAD_CONNECTION, std::move(error).c_str(), S2C_SEVERITY_ERROR);
            return nullptr;
        }
    }
    // prepare a query that will be executed
    reader->m_query = readQuery;
    reader->m_row_schema = schema;
    reader->m_chunk_writer = std::make_unique<SuperChunkWriter>();
    return reader;
}

std::unique_ptr<ResultTableReader>
ResultTableReader::CreateReaderNonParallel(
    std::unique_ptr<S2Connection> &conn,
    ThreadSafeQueue<Chunk *> *q,
    const char *query,
    RowSchema *schema,
    uint64_t size,
    bool usePreparedProtocol)
{
    // allocate a ResultTableReader object
    std::unique_ptr<ResultTableReader> reader(new ResultTableReader(q, 0 /*partition*/, size));

    // create a new connection
    reader->m_conn =
        S2Connection::Connect(conn->m_host, conn->m_port, conn->m_db, conn->m_user, conn->m_password, conn->m_ssl_ca);
    // prepare a query that will be executed
    reader->m_query = query;
    reader->m_row_schema = schema;
    reader->m_use_prepared_protocol = usePreparedProtocol;
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
        if (m_use_prepared_protocol)
        {
            m_conn->Prepare(m_query.c_str(), true);
        }
        else
        {
            m_conn->Execute(m_query.c_str());
        }
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
            if (!ptr)
            {
                std::unique_lock<std::mutex> lock(m_error_mutex);
                m_error = S2ClientError(
                    S2C_ERROR_MEMORY_ALLOCATION,
                    "ResultTableReader cannot allocate chunk size: " + std::to_string(m_chunk_size));
                break;
            }
            Chunk *chunk = NewChunk(ptr, m_chunk_size, chunkId, m_partition);

            m_conn->NextChunk(m_chunk_writer, chunk, m_row_schema);

            if (chunk->row_count == 0)
            {
                // in this case chunk is not pushed to the queue, we need to
                // free it here
                free(ptr);
                delete chunk;
                break;
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
