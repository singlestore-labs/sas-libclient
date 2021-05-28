
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
#define chunkSize 10485
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

typedef struct ErrorHandler
{
    S2ErrorCallback callback;
    int errorCode;
    char *errorString;
} ErrorHandler;

ErrorHandler EH;

void
dummyHandleError(
    S2ErrorCallback *cb,
    int error,
    const char *errorString)
{
    ErrorHandler *h = (ErrorHandler *)cb;
    h->errorCode = error;
    printf("[DUMMMY ERROR CALLBACK] GetNextChunk failed: %d %s\n", error, errorString);
    fflush(stdout);
}

void
dummyProcessChunk(
    Chunk *chunk,
    bool print)
{
    TOTAL += chunk->row_count;
    if (print)
    {
        printf(
            "Got chunk: %p, m_ptr: %p, m_size: %d, row_count: %d, id: %d, partition_id: %d\n",
            chunk,
            chunk->m_ptr,
            chunk->m_size,
            chunk->row_count,
            chunk->id,
            chunk->partition_id);
    }
}

void prepare_mult(S2Client *client)
{
    int err;
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS t_mult", &err);
    ExecuteDDLQuery(client, "CREATE TABLE t_mult AS SELECT * FROM t", &err);

    for (int i = 0; i < 10; ++i)
    {
        ExecuteDDLQuery(client, "INSERT INTO t_mult (SELECT * FROM t_mult)", &err);
    }
}

void *reader_thread(void *input)
{
    struct readerThreadArgs *args = (struct readerThreadArgs *)input;

    int partitionId;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    printf("Thread %d allocated chunk %p\n", args->id, chunk);
    fflush(stdout);
    int numReceived = 0;

    RowSchema *s = GetRowSchema(args->queue);
    assert(s && "GetRowSchema failed");

    for (int i = 0; i < s->numColumns; ++i)
    {
        printf("%d-th row: type %d, name %s; ", i, s->ColumnInfo[i].type, s->ColumnInfo[i].name);
    }
    printf("\n");
    fflush(stdout);

    while (GetNextChunk(args->queue, &partitionId, chunk, &EH.callback))
    {
        numReceived++;

        dummyProcessChunk(chunk, true);
        ChunkFree(chunk);
    }
    free(chunk);
    printf("Thread %d read %d chunks\n", args->id, numReceived);
    fflush(stdout);
}

void *worker(void *input)
{
    // init the client
    struct workerArgs *args = (struct workerArgs *)input;
    int err;
    S2Client *client = S2ClientInit(
        db_creds.host,
        args->db_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        numWorkers,
        args->id,
        &err);
    assert(err == 0 && "Failed to init the client");
    assert(client != NULL && "S2Client is NULL");

    printf("Worker %d connected to port %d\n", args->id, args->db_port);
    fflush(stdout);

    ChunkQueue *q = ParallelReadGetQueue(client, resultTable, chunkSize, queueCapacity, true);
    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        printf("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
        fflush(stdout);
    }
    pthread_t readers[threadsPerWorker];
    struct readerThreadArgs readerArgs[threadsPerWorker];
    int i;

    for (i = 0; i < threadsPerWorker; i++)
    {
        readerArgs[i].id = i + 1000 * args->id;
        readerArgs[i].queue = q;
        pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
    }

    for (i = 0; i < threadsPerWorker; i++)
    {
        pthread_join(readers[i], NULL);
    }
    // free the queue
    ChunkQueueFree(q);

    if (S2Errno(client))
    {
        printf("S2 Error in controller %s\n", S2Error(client));
        fflush(stdout);
    }

    // free the client
    S2ClientFree(client);

    return NULL;
}

void parallel_test(S2Client *client)
{
    int agg_ports[numWorkers];
    // use MA for every connection
    for (int i = 0; i < numWorkers; ++i)
    {
        agg_ports[i] = db_creds.ma_port;
    }

    // init the parallel read in multi-pass mode
    ParallelReadInit(client, resultTable, "SELECT * FROM t_mult", true);
    if (S2Errno(client))
    {
        printf("S2 Error in controller: %d %s\n", S2Errno(client), S2Error(client));
        fflush(stdout);
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
    fflush(stdout);

    // clean up parallel reading
    ParallelReadFree(client, resultTable);
}

int
main(
    int argc,
    char *argv[])
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
            printf("Exiting... Correct usage: parallel_read_test <numWorkers> <threadsPerWorker> <queueCapacity>\n");
            exit(1);
        }
    }
    EH.callback.setError = dummyHandleError;

    const char *version = S2GetClientVersion();
    printf("libs2client version: %s\n", version);

    int err;
    // init the client
    S2Client *client = S2ClientInit(
        db_creds.host,
        db_creds.ma_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        numWorkers,
        -1,
        &err);
    assert(err == 0 && "Failed to init the client");
    assert(client != NULL && "S2Client is NULL");

    // check the partition number
    int p = GetPartitionsNumber(client);
    printf("Number of partitions: %d\n", p);

    prepare_mult(client);

    parallel_test(client);

    // free the client
    S2ClientFree(client);
    return 0;
}
