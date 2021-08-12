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
    std::queue<T> m_queue;

    const int m_totalProducers;
    int m_activeProducers;
    // m_producerActiveArray contains the state of producers
    std::vector<bool> m_producerActiveArray;
    std::vector<int> m_producerIds;
    std::unordered_map<int, int> m_partitionIdToNumber;

    // m_dataArray stores the data that is written to/read from the queue
    std::vector<DataContainer<T>> m_dataArray;

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
        return m_partitionIdToNumber[producerId] * m_batchSize + valueId % m_batchSize;
    }

    void resetBatch()
    {
        for (int i = 0; i < m_totalProducers; ++i)
        {
            m_readInBatch[i] = 0;
        }
        ++m_currentBatch;

        m_busyCV.notify_all();
        for (int i = 0; i < m_totalProducers; ++i)
        {
            m_dataNotReadyCV[i]->notify_all();
        }
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
        std::vector<int> producers)
        :
        m_batchSize(batch_size),
        m_totalProducers(producers.size()),
        m_activeProducers(producers.size()),
        m_producerIds(producers)
    {
        m_currentBatch = 0;
        m_itemsToRead = 0;

        m_dataArray.reserve(m_producerIds.size() * batch_size);
        m_producerActiveArray.reserve(m_producerIds.size());
        m_readInBatch.reserve(m_producerIds.size());
        m_dataNotReadyCV.reserve(m_producerIds.size());

        for (int i = 0; i < m_totalProducers; ++i)
        {
            m_partitionIdToNumber[m_producerIds[i]] = i;
            m_producerActiveArray.push_back(true);
            m_readInBatch.push_back(0);
            for (int j = 0; j < m_batchSize; ++j)
            {
                m_dataArray.push_back(DataContainer<T>());
            }
            auto cv = std::make_unique<std::condition_variable>();
            m_dataNotReadyCV.push_back(std::move(cv));
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
        m_queue.push(val);

        ++m_itemsToRead;
        m_emptyCV.notify_one();
        m_dataNotReadyCV[m_partitionIdToNumber[val->partition_id]]->notify_all();
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

        T res = m_queue.front();
        int valueIndex = targetIndex(res->partition_id, res->id);
        readValue(valueIndex);
        m_queue.pop();

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

        // we should wait until:
        // - the corresponding value has been pushed
        // - m_currentBatch is the batch to which valueId belongs
        // if producerId is not active, we should not wait anymore
        while ((!m_dataArray[valueIndex].isSet ||
                valueId / m_batchSize != m_currentBatch) &&
               m_producerActiveArray[m_partitionIdToNumber[producerId]])
        {
            m_dataNotReadyCV[m_partitionIdToNumber[producerId]]->wait(lock);
        }
        if (!m_dataArray[valueIndex].isSet)
        {
            throw std::out_of_range("Asking for non-existsent chunk");
        }

        T res = readValue(valueIndex);

        // we pop the value from the queue in order not to accumulate all values there
        // m_dataQueue is not used in subsequent passes of multi-pass
        m_queue.pop();

        if (isBatchFinished())
        {
            resetBatch();
        }

        return res;
    }

    void DeleteProducer(int producerId)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_producerActiveArray[m_partitionIdToNumber[producerId]] = false;
        --m_activeProducers;

        if (isBatchFinished())
        {
            resetBatch();
        }
        if (!m_activeProducers)
        {
            m_emptyCV.notify_all();
        }
    }
};

#endif  // THREAD_SAFE_BATCH_QUEUE_HPP
