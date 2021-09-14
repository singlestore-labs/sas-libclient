#ifndef THREAD_SAFE_QUEUE_HPP
#define THREAD_SAFE_QUEUE_HPP

#include <mutex>
#include <condition_variable>
#include <queue>

#include "utils.hpp"

// ThreadSafeQueue is a wrapper around the non-thread-safe std queue
// it allows multiple consumers and multiple producers
//
template<typename T>
class ThreadSafeQueue
{
  public:
    virtual ~ThreadSafeQueue()
        {};
    // Push waits while the number of elements in a queue will be less than capacity
    // then it adds one instance of T to the queue head
    //
    virtual void Push(T const& value) = 0;

    // Pop waits until the queue has at least one element.
    // Then it deletes one element from the tail of the queue tail and returns it.
    // If the queue doesn't have producers and elements, it throws exception
    //
    virtual T Pop() = 0;

    // Get waits until the element number valueId
    // is added by producerId. When it's added,
    // it returns this element
    //
    virtual T
    Get(
        int producerId,
        int valueId) = 0;

    // DeleteProducer decreases the number of producers by one.
    // When no producers are left and queue is empty, Pop will throw an exception
    //
    virtual void DeleteProducer(int producerId) = 0;

    virtual void FreeBatchData(void (*FreeCallback)(T)) = 0;

    std::queue<T> m_queue;
};

#endif  // THREAD_SAFE_QUEUE_HPP
