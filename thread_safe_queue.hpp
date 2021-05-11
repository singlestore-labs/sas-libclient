#ifndef THREAD_SAFE_QUEUE_HPP
#define THREAD_SAFE_QUEUE_HPP

#include <mutex>
#include <condition_variable>
#include <queue>

#include "utils.hpp"

using super_chunk::structs::PartitionChunk;

// ThreadSafeQueue is a wrapper around the non-thread-safe std queue
// it allows multiple consumers and multiple producers
//
template<typename T>
class ThreadSafeQueue
{
  private:
    std::queue<T> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_emptyCV;
    std::condition_variable m_fullCV;
    uint32_t m_capacity;
    uint32_t m_producers;

  public:
    ThreadSafeQueue(
        uint32_t capacity,
        uint32_t producers)
        :
        m_capacity(capacity),
        m_producers(producers)
    {
    }

    // Push waits while the number of elements in a queue will be less then capacity
    // then it adds one instance of T to the queue head
    //
    void Push(T const& value)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_queue.size() >= m_capacity)
        {
            m_fullCV.wait(lock);
        }

        m_queue.push(value);
        m_emptyCV.notify_one();
    }

    // Pop waits while queue will have at least one element
    // then it deletes one element from queue tail and returns it
    // if the queue doesn't have producers and elements, it throws exception
    //
    T Pop()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_queue.empty() && m_producers != 0)
        {
            m_emptyCV.wait(lock);
        }

        if (m_queue.empty() && m_producers == 0)
        {
            throw std::out_of_range("Queue is empty");
        }

        T res = m_queue.front();
        m_queue.pop();
        m_fullCV.notify_one();
        return res;
    }

    // DeleteProducer decreases the number of producers by one
    // when no producers left and queue is empty, Pop will throw an exception
    void DeleteProducer()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_producers--;
        if (m_producers == 0)
        {
            m_emptyCV.notify_all();
        }
    }
};

#endif  // THREAD_SAFE_QUEUE_HPP
