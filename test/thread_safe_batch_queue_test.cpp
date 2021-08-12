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

typedef struct intContainer
{
    int val;
    int id;
    int producer_id;
} intContainer;

void
pushToQueue(
    ThreadSafeBatchQueue<intContainer *> *q,
    int selfId)
{
    for (int i = 0; i < pushesInOneThread + selfId; i++)
    {
        int value = (i + 1) + pushesInOneThread * selfId;
        intContainer *data = new intContainer
            {
                .val = value,
                .id = i,
                .producer_id = selfId
            };

        q->Push(data);
    }
    q->DeleteProducer(selfId);
}

void
popFromQueue(
    ThreadSafeBatchQueue<intContainer *> *q,
    std::vector<intContainer *> *received,
    int selfId)
{
    intContainer *cur;
    while (true)
    {
        try
        {
            cur = q->Pop();
            (*received).push_back(cur);
            std::unique_lock<std::mutex> lock(cnt_mutex);
            ++cnt_pop[cur->producer_id];
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
    ThreadSafeBatchQueue<intContainer *> *q,
    std::vector<intContainer *> *received,
    int selfId)
{
    intContainer *val;
    std::vector<intContainer *> received2;

    for (auto item : *received)
    {
        try
        {
            val = q->Get(item->producer_id, item->id);
            received2.push_back(val);
            {
                std::unique_lock<std::mutex> lock(cnt_mutex);
                cnt_get[item->producer_id]++;
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
              ((*received)[i]->producer_id == received2[i]->producer_id)))
        {
            printf(
                "%d %d\t%d %d\t%d %d\n",
                (*received)[i]->val,
                received2[i]->val,
                (*received)[i]->id,
                received2[i]->id,
                (*received)[i]->producer_id,
                received2[i]->producer_id);
        }
        assert(
            ((*received)[i]->val == received2[i]->val) &&
            ((*received)[i]->id == received2[i]->id) &&
            ((*received)[i]->producer_id == received2[i]->producer_id));
    }
    printf("Getting finished for %d! Got %d items\n", selfId, received2.size());
}

int main()
{
    ThreadSafeBatchQueue<intContainer *> q(batchSize, nProducers);

    std::vector<std::thread> threads;
    std::vector<std::vector<intContainer *> *> received_by_thread;
    for (int i = 0; i < nConsumers; i++)
    {
        auto c = new std::vector<intContainer *>;
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

    ThreadSafeBatchQueue<intContainer *> q1(batchSize, nProducers);

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
