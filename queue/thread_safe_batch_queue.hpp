#ifndef THREAD_SAFE_BATCH_QUEUE_HPP
#define THREAD_SAFE_BATCH_QUEUE_HPP

#include <cassert>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>

#include "queue/thread_safe_queue.hpp"
#include "utils.hpp"

template<typename T>
struct DataContainer
{
    T valuePtr;
    bool isSet = false;
};

template<typename T>
class ThreadSafeBatchQueue : public ThreadSafeQueue<T>
{
  private:
    const int m_totalProducers;
    int m_activeProducers;
    // m_producerActiveArray contains the state of producers
    std::vector<bool> m_producerActiveArray;
    // m_dataArray stores the data that is written to/read from the queue
    std::vector<DataContainer<T>> m_dataArray;
    // m_dataQueue stores m_dataArray elements that are used
    // in the first pass of multi-pass
    std::queue<T> m_dataQueue;

    // m_batchSize indicates how many items from each producer can push
    // at a single stage of processing
    const int m_batchSize;
    // m_currentBatch indicates which batch is being processed
    int m_currentBatch;
    // m_readInBatch contains the number of items pushed by each producer
    // that have been processed in the current batch
    std::vector<int> m_readInBatch;
    int m_itemsToRead;

    mutable std::mutex m_mutex;
    // m_emptyCV is used to block access of consumers to an empty queue
    std::condition_variable m_emptyCV;
    // m_busyCV is used to block producers while certain batch is processed by consumers
    std::condition_variable m_busyCV;
    // m_dataNotReadyCV is used to block consumers asking for the data that
    // has not been pushed yet
    std::vector<std::unique_ptr<std::condition_variable>> m_dataNotReadyCV;

    bool isBatchFinished()
    {
        for (int i = 0; i < m_totalProducers; ++i)
        {
            if (m_producerActiveArray[i] && m_readInBatch[i] != m_batchSize)
            {
                return false;
            }
        }
        return !m_itemsToRead;
    }

    int
    targetIndex(
        int producerId,
        int valueId)
    {
        return producerId * m_batchSize + valueId % m_batchSize;
    }

    void resetBatch()
    {
        for (int i = 0; i < m_totalProducers; ++i)
        {
            m_readInBatch[i] = 0;
        }
        m_currentBatch += 1;
        m_busyCV.notify_all();
    }

    T readValue(int idx)
    {
        --m_itemsToRead;
        m_dataArray[idx].isSet = false;
        ++m_readInBatch[idx / m_batchSize];
        return m_dataArray[idx].valuePtr;
    }

  public:
    ThreadSafeBatchQueue(
        uint32_t batch_size,
        uint32_t producers)
        :
        m_batchSize(batch_size),
        m_totalProducers(producers),
        m_activeProducers(producers)
    {
        m_currentBatch = 0;
        m_itemsToRead = 0;

        m_dataArray.reserve(producers * batch_size);
        m_producerActiveArray.reserve(producers);
        m_readInBatch.reserve(producers);
        m_dataNotReadyCV.reserve(producers);

        for (int i = 0; i < m_totalProducers; ++i)
        {
            m_producerActiveArray.push_back(true);
            m_readInBatch.push_back(0);
            for (int j = 0; j < m_batchSize; ++j)
            {
                m_dataArray.push_back(DataContainer<T>());
            }
            m_dataNotReadyCV.push_back(std::move(std::make_unique<std::condition_variable>()));
        }
    }

    void Push(T const& val)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        int batchNum = val->id / m_batchSize;
        int valueIndex = targetIndex(val->partition_id, val->id);

        while (batchNum > m_currentBatch)
        {
            m_busyCV.wait(lock);
        }

        assert(!m_dataArray[valueIndex].isSet && "pushing to non-empty place");

        m_dataArray[valueIndex].valuePtr = val;
        m_dataArray[valueIndex].isSet = true;
        m_dataQueue.push(val);

        ++m_itemsToRead;
        m_emptyCV.notify_one();
        m_dataNotReadyCV[val->partition_id]->notify_all();
    }

    T Pop()
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        while (!m_itemsToRead && m_activeProducers)
        {
            m_emptyCV.wait(lock);
        }

        if (!m_itemsToRead && !m_activeProducers)
        {
            throw std::out_of_range("Queue is empty");
        }

        T res = m_dataQueue.front();
        int valueIndex = targetIndex(res->partition_id, res->id);
        readValue(valueIndex);
        m_dataQueue.pop();

        if (isBatchFinished())
        {
            resetBatch();
        }

        return res;
    }

    T
    Get(
        int producerId,
        int valueId)
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        if (!m_itemsToRead && !m_activeProducers)
        {
            throw std::out_of_range("Queue is empty");
        }

        int valueIndex = targetIndex(producerId, valueId);
        while (!m_dataArray[valueIndex].isSet || valueId / m_batchSize != m_currentBatch)
        {
            m_dataNotReadyCV[producerId]->wait(lock);
        }

        T res = readValue(valueIndex);

        if (isBatchFinished())
        {
            resetBatch();
        }

        return res;
    }

    void DeleteProducer(int producerId)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_producerActiveArray[producerId] = false;
        --m_activeProducers;

        if (!m_activeProducers)
        {
            m_emptyCV.notify_all();
        }
    }
};

#endif  // THREAD_SAFE_BATCH_QUEUE_HPP
