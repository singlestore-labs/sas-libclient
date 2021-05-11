
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "s2_client_extern.h"
#include "test/db_creds.h"

// #define numWorkers 1
// #define threadsPerWorker 2
// #define queueCapacity 2
#define chunkSize 1048576
int numWorkers = 1;
int threadsPerWorker = 2;
int queueCapacity = 2;

const char *resultTable = "tmp";
static unsigned _Atomic TOTAL = ATOMIC_VAR_INIT(0);

struct workerArgs
{
    int id;
    int db_port;
};

struct readerThreadArgs
{
    int id;
    ChunkQueue *queue;
};

void processChunk(Chunk* chunk, bool print)
{
    TOTAL += chunk->row_count;
    if (print)
    {
        printf("Got chunk: %p, m_ptr: %p, m_size: %d, row_count: %d\n",
            chunk,
            chunk->m_ptr,
            chunk->m_size,
            chunk->row_count);
    }
    // print the data using the known schema
    int64_t x, y;
    memcpy(&x, chunk->m_ptr, 8);
    memcpy(&y, chunk->m_ptr + 8, 8);

    double u, v;
    memcpy(&u, chunk->m_ptr + 16, 8);
    memcpy(&v, chunk->m_ptr + 24, 8);

    int64_t offset, len;
    memcpy(&offset, chunk->m_ptr + 32, 8);
    memcpy(&len, chunk->m_ptr + 40, 8);
    if (print)
        {
        printf("The numbers in chunk are: %d %d %f %f. ", x, y, u, v);
        printf("The string is: ");
        for (int i = 0; i < len; i++)
        {
            printf("%c", (chunk->m_ptr + offset + i)[0]);
        }
        printf("\n");
    }
}

void *reader_thread(void *input)
{
    struct readerThreadArgs *args = (struct readerThreadArgs *)input;

    int partitionId;
    int err;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    printf("Thread %d allocated chunk %p\n", args->id, chunk);
    int numReceived = 0;

    while (GetNextChunk(args->queue, &partitionId, chunk, &err))
    {
        if (!numReceived)
        {
            RowSchema *s = GetRowSchema(args->queue);

            printf("First row inRowSchema: numColumns %d, type %d, name %s\n",
                s->numColumns,
                s->ColumnInfo[0].type,
                s->ColumnInfo[0].name);
        }
        assert(err == 0);
        numReceived++;

        processChunk(chunk, true);
        ChunkFree(chunk);
    }
    free(chunk);
    printf("Thread %d read %d chunks\n", args->id, numReceived);
}

void worker_read( S2Client *client, int worker_id)
{
    ChunkQueue *q = ParallelReadGetQueue(client, resultTable, chunkSize, queueCapacity);
    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        printf("S2 Error in worker %s\n", S2Error(client));
        fflush(stdout);
    }
    pthread_t readers[threadsPerWorker];
    struct readerThreadArgs readerArgs[threadsPerWorker];
    int i;

    for (i = 0; i < threadsPerWorker; i++)
    {
        readerArgs[i].id = i + 1000 * worker_id;
        readerArgs[i].queue = q;
        pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
    }

    for (i = 0; i < threadsPerWorker; i++)
    {
        pthread_join(readers[i], NULL);
    }
    // free the queue
    ChunkQueueFree(q);
}

void *worker(void *input)
{
    // init the client
    struct workerArgs *args = (struct workerArgs *)input;
    int err;
    S2Client *client = S2ClientInit(
        db_creds.host, args->db_port, db_creds.db, db_creds.user, db_creds.password, numWorkers, args->id, &err);
    assert(err == 0 && "Failed to init the client");
    assert(client != NULL && "S2Client is NULL");

    printf("Worker %d connected to port %d\n", args->id, args->db_port);
    fflush(stdout);
   
    worker_read(client, args->id);

    // free the client
    S2ClientFree(client);

    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc == 4)
    {
        numWorkers = atoi(argv[1]);
        threadsPerWorker = atoi(argv[2]);
        queueCapacity = atoi(argv[3]);
    }
    else
    {
        if (argc != 1)
        {
            printf("Exiting.. Correct usage: parallel_read_test <numWorkers> <threadsPerWorker> <queueCapacity>\n");
            exit(1);
        }
    }

    int agg_ports[numWorkers];
    // use MA for every connection
    for (int i = 0; i < numWorkers; ++i)
    {
        agg_ports[i] = db_creds.ma_port;
    }

    const char * version = S2GetClientVersion();
    printf("libs2client version: %s\n", version);

    int err;
    // init the client
    S2Client *client = S2ClientInit(
        db_creds.host, db_creds.ma_port, db_creds.db, db_creds.user, db_creds.password, numWorkers, -1, &err);
    assert(err == 0 && "Failed to init the client");
    assert(client != NULL && "S2Client is NULL");

    // check the partition number
    int p = GetPartitionsNumber(client);
    printf("Number of partitions: %d\n", p);
    // init the parallel read
    ParallelReadInit(client, resultTable, "SELECT * FROM t", false);
    if (S2Errno(client))
    {
        printf("S2 Error in controller %s\n", S2Error(client));
        fflush(stdout);
        return 1;
    }

    // start "CAS worker" threads
    pthread_t workers[numWorkers];
    int i;
    struct workerArgs args[numWorkers];
    for (i = 0; i < numWorkers; i++)
    {
        args[i].id = i;
        args[i].db_port = agg_ports[i];
        pthread_create(&workers[i], NULL, worker, &args[i]);
    }

    // join worker threads
    for (i = 0; i < numWorkers; i++)
    {
        pthread_join(workers[i], NULL);
    }
    printf("Processed TOTAL %d rows\n", TOTAL);

    // clean up parallel reading
    ParallelReadFree(client, resultTable);

    // free the client
    S2ClientFree(client);
    return 0;
}
