/*
 * Copyright 2021-2026 SingleStore, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
