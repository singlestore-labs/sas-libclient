#ifndef QUEUE_CHUNK_QUEUE_HPP
#define QUEUE_CHUNK_QUEUE_HPP

#include <unordered_map>
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
        bool is_read_finished = true;
        for (auto &reader : m_readers)
        {
            if (reader->IsActive())
            {
                is_read_finished = false;
            }
            reader->StopReading();
        }

        if (!is_read_finished)
        {
            for (auto &reader : m_readers)
            {
                reader->NotifyConnUnfinishedStmt();
            }
        }
        for (int consumer_id = 0; consumer_id < m_consumer_queues.size(); ++consumer_id)
        {
            // delete all chunks that are saved in the queue
            S2ClientError err(0, "");
            while (Chunk *c = Get(consumer_id, err))
            {
                super_chunk::utils::ChunkFree(c);
                delete c;
            }
            delete m_consumer_queues[consumer_id];
            m_consumer_queues[consumer_id] = nullptr;
        }
    };

    // Get retrieves one Chunk from the queue
    Chunk *
    Get(
        int consumerId,
        S2ClientError &err)
    {
        err = S2ClientError(0, "");
        try
        {
            return m_consumer_queues[consumerId]->Pop();
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

    // GetSingleRow retrieves the row identified by (chunkId, rowNum) from partiotionId
    virtual Chunk *
    GetSingleRow(
        uint32_t partitionId,
        uint32_t chunkId,
        int64_t rowNum,
        int threadId,
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
    ChunkQueue() = default;

    RowSchema *m_row_schema = nullptr;
    S2ClientError m_error;
    std::mutex m_error_mutex;

    std::string m_result_table;
    std::vector<std::unique_ptr<ResultTableReader>> m_readers;

    int m_consumers;
    std::vector<ThreadSafeQueue<Chunk *> *> m_consumer_queues;
    // m_partition_consumer stores partition -> cas_reader_thread_id correspondence
    // in a vector indexed by partition_id. Partitions assigned to another worker
    // store the value of -1
    // reader thread ids are required to be 0, 1 ,..., m_consumers - 1
    std::vector<int> m_partition_consumer;
};

#endif  // QUEUE_CHUNK_QUEUE_HPP
