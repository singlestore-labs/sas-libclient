#include "queue/thread_safe_simple_queue.hpp"

#include <cassert>
#include <iostream>
#include <thread>
#include <vector>

const int pushesInOneThread = 10000;
const int threadsCount = 5;
const int queueCapacity = 20;
int cnt[pushesInOneThread];
std::mutex cnt_mutex;

void pushToQueue(ThreadSafeQueue<int> *q)
{
    for (int i = 0; i < pushesInOneThread; i++)
    {
        q->Push(i);
    }
    q->DeleteProducer(0);
}

void popFromQueue(ThreadSafeQueue<int> *q)
{
    for (int i = 0; i < pushesInOneThread; i++)
    {
        int cur = q->Pop();
        {
            std::unique_lock<std::mutex> lock(cnt_mutex);
            cnt[cur]++;
        }
    }
}

int main()
{
    ThreadSafeSimpleQueue<int> q(queueCapacity, threadsCount);

    std::vector<std::thread> threads;
    for (int i = 0; i < threadsCount; i++)
    {
        threads.push_back(std::thread(popFromQueue, &q));
    }
    for (int i = 0; i < threadsCount; i++)
    {
        threads.push_back(std::thread(pushToQueue, &q));
    }

    for (std::thread &t : threads)
    {
        t.join();
    }

    for (int i = 0; i < pushesInOneThread; i++)
    {
        assert(cnt[i] == threadsCount && "Queue contains wrong elements");
    }
    std::cout << "Success!" << std::endl;
}
