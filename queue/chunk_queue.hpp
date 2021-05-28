#ifndef CHUNK_QUEUE_HPP
#define CHUNK_QUEUE_HPP

#include <vector>

#include "result_table_reader.hpp"
#include "queue/thread_safe_queue.hpp"

// ChunkQueue is responsible for reading data in chunks from S2.
// Instance of this class is created by SAS controller.
// Items are retrieved by SAS workers.
// ChunkQueue creates none or several S2 result table readers.
// Each reader reads chunks from its partition
// and adds them to the global thread safe queue.
class ChunkQueue
{
  public:
    virtual ~ChunkQueue()
    {
        for (auto &reader : m_readers)
        {
            reader->StopReading();
        }
        if (m_queue)
        {
            // delete all chunks that are saved in queue
            S2ClientError err(0, "");
            while (Chunk *c = Get(err))
            {
                if (c)
                {
                    super_chunk::utils::ChunkFree(c);
                }
                delete c;
            }
            delete m_queue;
            m_queue = nullptr;
        }
    };

    // Get retrieves one Chunk from the queue
    Chunk *Get(S2ClientError &err)
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

    // GetById retrieves the Chunk number chunkId read from partiotionId
    virtual Chunk *
    GetById(
        int partiotionId,
        int chunkId,
        S2ClientError &error) = 0;

    RowSchema *GetRowSchema()
    {
        return this->m_row_schema;
    }

    void SetError(S2ClientError err)
    {
        std::unique_lock<std::mutex> lock(m_error_mutex);
        m_error = std::move(err);
    }

  protected:
    ChunkQueue()
    {}

    RowSchema *m_row_schema = nullptr;
    S2ClientError m_error;
    std::mutex m_error_mutex;

    std::vector<std::unique_ptr<ResultTableReader>> m_readers;
    ThreadSafeQueue<Chunk *> *m_queue;
};

#endif  // CHUNK_QUEUE_HPP
