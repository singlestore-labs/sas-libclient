#include <cassert>
#include <iostream>
#include <thread>
#include <vector>

#include "utils.hpp"
#include "queue/thread_safe_batch_queue.hpp"

const int pushesInOneThread = 100000;
const int nProducers = 5;
const int nConsumers = 20;

const int batchSize = 100;
int cnt_pop[nProducers];
int cnt_get[nProducers];

std::mutex cnt_mutex;

typedef struct IntContainer
{
    int val;
    int id;
    int partition_id;
} IntContainer;

void
pushToQueue(
    ThreadSafeBatchQueue<IntContainer *> *q,
    int selfId)
{
    for (int i = 0; i < pushesInOneThread + selfId; i++)
    {
        int value = (i + 1) + pushesInOneThread * selfId;
        IntContainer *data = new IntContainer
            {
                .val = value,
                .id = i,
                .partition_id = selfId
            };

        q->Push(data);
    }
    q->DeleteProducer(selfId);
}

void
popFromQueue(
    ThreadSafeBatchQueue<IntContainer *> *q,
    std::vector<IntContainer *> *received,
    int selfId)
{
    IntContainer *cur;
    while (true)
    {
        try
        {
            cur = q->Pop();
            (*received).push_back(cur);
            std::unique_lock<std::mutex> lock(cnt_mutex);
            ++cnt_pop[cur->partition_id];
            lock.unlock();
        }
        catch (const std::out_of_range &e)
        {
            break;
        }
    }
    printf("Popping finished for %d! Got %d items\n", selfId, (*received).size());
}

void
getFromQueue(
    ThreadSafeBatchQueue<IntContainer *> *q,
    std::vector<IntContainer *> *received,
    int selfId)
{
    IntContainer *val;
    std::vector<IntContainer *> received2;

    for (auto item : *received)
    {
        try
        {
            val = q->Get(item->partition_id, item->id);
            received2.push_back(val);
            {
                std::unique_lock<std::mutex> lock(cnt_mutex);
                cnt_get[item->partition_id]++;
            }
        }
        catch (const std::out_of_range &e)
        {
            return;
        }
    }
    for (int i = 0; i < (*received).size(); ++i)
    {
        if (!(((*received)[i]->val == received2[i]->val) &&
              ((*received)[i]->id == received2[i]->id) &&
              ((*received)[i]->partition_id == received2[i]->partition_id)))
        {
            printf(
                "%d %d\t%d %d\t%d %d\n",
                (*received)[i]->val,
                received2[i]->val,
                (*received)[i]->id,
                received2[i]->id,
                (*received)[i]->partition_id,
                received2[i]->partition_id);
        }
        assert(
            ((*received)[i]->val == received2[i]->val) &&
            ((*received)[i]->id == received2[i]->id) &&
            ((*received)[i]->partition_id == received2[i]->partition_id));
    }
    printf("Getting finished for %d! Got %d items\n", selfId, received2.size());
}

int main()
{
    ThreadSafeBatchQueue<IntContainer *> q(
        batchSize,
        std::vector<int>
        {
            0,
            1,
            2,
            3,
            4
        });

    std::vector<std::thread> threads;
    std::vector<std::vector<IntContainer *> *> received_by_thread;
    for (int i = 0; i < nConsumers; i++)
    {
        auto c = new std::vector<IntContainer *>;
        received_by_thread.push_back(c);
        threads.push_back(std::thread(popFromQueue, &q, received_by_thread[i], i));
    }
    for (int i = 0; i < nProducers; i++)
    {
        threads.push_back(std::thread(pushToQueue, &q, i));
    }

    for (std::thread &t : threads)
    {
        t.join();
    }

    for (int i = 0; i < nProducers; i++)
    {
        assert(cnt_pop[i] == pushesInOneThread + i && "Queue contains wrong elements");
    }
    printf("Pop OK\n");

    ThreadSafeBatchQueue<IntContainer *> q1(
        batchSize,
        std::vector<int>
        {
            0,
            1,
            2,
            3,
            4
        });

    std::vector<std::thread> new_threads;
    for (int i = 0; i < nConsumers; i++)
    {
        new_threads.push_back(std::thread(getFromQueue, &q1, received_by_thread[i], i));
    }
    for (int i = 0; i < nProducers; i++)
    {
        new_threads.push_back(std::thread(pushToQueue, &q1, i));
    }

    for (std::thread &t : new_threads)
    {
        t.join();
    }

    for (int i = 0; i < nProducers; i++)
    {
        assert(cnt_get[i] == pushesInOneThread + i && "Queue contains wrong elements");
    }
    printf("Get OK\n");

    std::cout << "[SUCCESS]" << std::endl;
}
