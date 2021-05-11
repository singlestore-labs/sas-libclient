#include <memory>

#include "result_table_reader.hpp"

std::unique_ptr<ResultTableReader>
ResultTableReader::CreateReader(
    std::unique_ptr<S2Connection> &conn,
    ThreadSafeQueue<PartitionChunk *> *q,
    const char *resultTableName,
    uint32_t partition,
    uint64_t size,
    std::shared_ptr<std::mutex> mu,
    std::shared_ptr<std::condition_variable> cv,
    bool row_schema_responsible)
{
    // allocate a ResultTableReader object
    std::unique_ptr<ResultTableReader> reader(new ResultTableReader(q, partition, size));

    // create a new connection
    reader->m_conn = S2Connection::Connect(conn->m_host, conn->m_port, conn->m_db, conn->m_user, conn->m_password);
    // prepare a query that will be executed
    reader->m_query = super_chunk::sql::MakeReadResultTableQuery(resultTableName, partition);

    reader->m_chunk_writer = std::make_unique<SuperChunkWriter>();
    reader->m_row_schema_mutex = mu;
    reader->m_row_schema_cv = cv;
    reader->row_schema_responsible = row_schema_responsible;

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
void ResultTableReader::Read()
{
    m_conn->Prepare(m_query.c_str());

    if (row_schema_responsible)
    {
        // this mutex is released when CreateChunkQueue enters "wait" state
        std::unique_lock<std::mutex> row_schema_lock(*m_row_schema_mutex.get());
        // get the row schema
        m_row_schema = m_conn->GetRowSchema();
        m_row_schema_cv->notify_one();
        row_schema_lock.unlock();
    }
    else
    {
        m_row_schema = m_conn->GetRowSchema();
    }

    try
    {
        while (!m_stopReading && this->m_conn->HasNextRow())
        {
            // initialize the chunk to fill
            char *ptr = (char *)malloc(m_chunk_size);
            std::unique_ptr<Chunk> chunk = std::make_unique<Chunk>();
            chunk->m_ptr = ptr;
            chunk->m_size = m_chunk_size;
            chunk->row_count = 0;

            this->m_conn->NextChunk(m_chunk_writer, chunk.get(), m_row_schema);

            if (chunk->row_count == 0)
            {
                break;
            }

            PartitionChunk *pc = new PartitionChunk
                {
                    this->m_partition,
                    std::move(chunk)
                };
            if (pc->chunk->row_count == 0)
            {
                break;
            }
            this->m_queue->Push(pc);
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
    catch (S2ClientError &err)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = err;
    }
    this->m_queue->DeleteProducer();
}

S2ClientError ResultTableReader::GetError()
{
    std::unique_lock<std::mutex> lock(m_error_mutex);
    return m_error;
}