#ifndef READER_HPP
#define READER_HPP

#include <thread>

#include "s2_client.hpp"
#include "thread_safe_queue.hpp"

// ResultTableReader is responsible for reading chunks from one partition and add them to the thread safe queue
class ResultTableReader
{
  public:
    // CreateReader creates a ResultTableReader object with a new connection
    static std::unique_ptr<ResultTableReader>
    CreateReader(
        std::unique_ptr<S2Connection> &conn,
        ThreadSafeQueue<PartitionChunk *> *q,
        const char *resultTableName,
        uint32_t partition,
        uint64_t size,
        std::shared_ptr<std::mutex> mu,
        std::shared_ptr<std::condition_variable> cv,
        bool row_schema_responsible);

    ~ResultTableReader()
    {
        m_stopReading = true;
        if (m_reading_thread.joinable())
        {
            m_reading_thread.join();
        }

        super_chunk::utils::RowSchemaFree(m_row_schema);
        m_row_schema = nullptr;
    }

    // StartReading starts a new thread which gets chunks from connection
    // and adds them to the queue
    // If this thread receives the error, it saves it in the m_error variable
    // This error can be retrieved by GetError method
    void StartReading();

    void StopReading();

    // GetError returns errors of the reading thread
    // If no error occurred, it returns an error with zero code
    S2ClientError GetError();

    RowSchema *GetRowSchema()
    {
        return this->m_row_schema;
    }

  private:
    ResultTableReader(
        ThreadSafeQueue<PartitionChunk *> *q,
        uint32_t partition,
        uint64_t size)
        :
        m_queue(q),
        m_partition(partition),
        m_query(""),
        m_chunk_size(size),
        m_error(0, "")
    {
    }

    std::thread m_reading_thread;
    std::unique_ptr<S2Connection> m_conn = nullptr;
    ThreadSafeQueue<PartitionChunk *> *m_queue;

    uint32_t m_partition;
    std::string m_query;
    const uint64_t m_chunk_size;

    RowSchema *m_row_schema = nullptr;
    // m_row_schema-related variables are shared among the readers of the same chunk queue
    std::shared_ptr<std::mutex> m_row_schema_mutex;
    std::shared_ptr<std::condition_variable> m_row_schema_cv;
    bool row_schema_responsible = false;

    std::unique_ptr<SuperChunkWriter> m_chunk_writer = nullptr;

    S2ClientError m_error;
    std::mutex m_error_mutex;

    std::atomic<bool> m_stopReading = ATOMIC_VAR_INIT(false);

    void Read();
};

#endif  // READER_HPP
