#ifndef READER_HPP
#define READER_HPP

#include <thread>

#include "s2_client.hpp"
#include "queue/thread_safe_queue.hpp"
#include "utils.hpp"

// ResultTableReader is responsible for reading chunks from one partition
// and adding them to the thread safe queue
class ResultTableReader
{
  public:
    // CreateReader creates a ResultTableReader object with a new connection
    static std::unique_ptr<ResultTableReader>
    CreateReader(
        const Credentials &creds,
        const Credentials &externalCreds,
        ThreadSafeQueue<Chunk *> *q,
        std::string query,
        RowSchema *schema,
        uint32_t partition,
        uint64_t size,
        S2ErrorCallback *cb);

    static std::unique_ptr<ResultTableReader>
    CreateReaderNonParallel(
        std::unique_ptr<S2Connection> &conn,
        ThreadSafeQueue<Chunk *> *q,
        const char *query,
        RowSchema *schema,
        uint64_t size,
        bool usePreparedProtocol);

    ~ResultTableReader()
    {
        m_stop_reading = true;
        if (m_reading_thread.joinable())
        {
            m_reading_thread.join();
        }
    }

    void PrepareRead(bool prefetch);

    // UpdateRowSchema is called to update the reader row schema with
    // the schema correponding to the query that is active on m_conn
    RowSchema *UpdateRowSchema();

    // Prefetch is called after PrepareRead with prefetch=false
    void Prefetch();

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

    bool IsActive() const
    {
        return m_active;
    }

    void SetActive(const bool active)
    {
        m_active = active;
    }

    void NotifyConnUnfinishedStmt();

  private:
    ResultTableReader(
        ThreadSafeQueue<Chunk *> *q,
        uint32_t partition,
        uint64_t size)
        :
        m_queue(q),
        m_is_parallel(true),
        m_partition(partition),
        m_query(""),
        m_chunk_size(size),
        m_error(0, "")
    {
    }

    std::thread m_reading_thread;
    std::unique_ptr<S2Connection> m_conn = nullptr;
    ThreadSafeQueue<Chunk *> *m_queue;

    bool m_is_parallel;
    uint32_t m_partition;

    std::atomic<bool> m_active = ATOMIC_VAR_INIT(true);

    std::string m_query;
    const uint64_t m_chunk_size;

    RowSchema *m_row_schema = nullptr;
    bool m_use_prepared_protocol = true; 

    std::unique_ptr<SuperChunkWriter> m_chunk_writer = nullptr;

    S2ClientError m_error;
    std::mutex m_error_mutex;

    std::atomic<bool> m_stop_reading = ATOMIC_VAR_INIT(false);

    void Read();
};

#endif  // READER_HPP
