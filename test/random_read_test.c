#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>

#include "s2_client_extern.h"
#include "test/db_creds.h"

#define chunkSize 10485

int numWorkers = 3;
int threadsPerWorker = 5;
int batchSize = 2;

const char *resultTable = "tmp";
static unsigned _Atomic TOTAL = ATOMIC_VAR_INIT(0);
static unsigned _Atomic TOTAL_SINGLE_ROWS = ATOMIC_VAR_INIT(0);

struct workerArgs
{
    int id;
    int db_port;
};

typedef struct ReceivedChunk
{
    int chunk_id;
    int partition_id;
    int row_count;
} ReceivedChunk;

typedef struct ReaderThreadArgs
{
    int id;
    int worker_id;
    ChunkQueue *queue;
    bool is_first_pass;
    int n_chunks_read;
    ReceivedChunk *chunks_read;
} ReaderThreadArgs;

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
    printf("[DUMMMY ERROR CALLBACK] Got error: %d %s\n", error, errorString);
    fflush(stdout);
}

void
dummyProcessChunk(
    Chunk *chunk,
    bool print,
    ReaderThreadArgs *args)
{
    if (args->is_first_pass)
    {
        (args->chunks_read)[args->n_chunks_read].chunk_id = chunk->id;
        (args->chunks_read)[args->n_chunks_read].partition_id = chunk->partition_id;
        (args->chunks_read)[args->n_chunks_read].row_count = chunk->row_count;

        (args->n_chunks_read)++;
    }

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

    for (int i = 0; i < 6; ++i)
    {
        ExecuteDDLQuery(client, "INSERT INTO t_mult (SELECT * FROM t_mult)", &err);
    }
}

void *reader_thread(void *input)
{
    ReaderThreadArgs *args = (struct ReaderThreadArgs *)input;

    int partitionId;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));

    int numReceived = 0;

    if (args->is_first_pass)
    {
        RowSchema *s = GetRowSchema(args->queue);
        assert(s && "GetRowSchema failed");

        while (GetNextChunk(args->queue, &partitionId, chunk, &EH.callback))
        {
            numReceived++;

            dummyProcessChunk(chunk, false, args);
            ChunkFree(chunk);
        }
        printf("Thread %d worker %d read %d chunks\n", args->id, args->worker_id, numReceived);

        free(chunk);
    }
    else
    {
        printf("Starting GetChunkMulti in thread %d worker %d\n", args->id, args->worker_id);

        while (numReceived < args->n_chunks_read && GetChunkMulti(
                                                        args->queue,
                                                        (args->chunks_read)[numReceived].partition_id,
                                                        (args->chunks_read)[numReceived].chunk_id,
                                                        chunk,
                                                        &EH.callback))
        {
            if (chunk->row_count != args->chunks_read[numReceived].row_count)
            {
                printf(
                    "[ERROR]: got %d rows, expected %d, thread %d\n ",
                    chunk->row_count,
                    args->chunks_read[numReceived].row_count,
                    args->id);
            }
            assert(chunk->row_count == args->chunks_read[numReceived].row_count);
            numReceived++;

            ChunkFree(chunk);
        }
        if (numReceived != args->n_chunks_read)
        {
            printf(
                "[ERROR]: got %d chunks, expected %d, thread %d\n ",
                numReceived,
                args->n_chunks_read,
                args->id);
        }

        assert(numReceived == args->n_chunks_read);

        printf(
            "Finished GetChunkMulti in thread %d worker %d, numReceived %d\n",
            args->id,
            args->worker_id,
            numReceived);
        if (numReceived)
        {
            for (int i = 0; i < args->n_chunks_read; ++i)
            {
                for (int row_num = 0; row_num < args->chunks_read[i].row_count; ++row_num)
                {
                    GetChunkRow(
                        args->queue,
                        args->chunks_read[i].partition_id,
                        args->chunks_read[i].chunk_id,
                        row_num,
                        args->id,
                        chunk,
                        &EH.callback);
                    printf(
                        "Got chunk in GetChunkRow: row_count %d, size %d, consumed %d\n",
                        chunk->row_count,
                        chunk->m_size,
                        chunk->consumed_size);
                    TOTAL_SINGLE_ROWS++;
                }
            }
        }
        free(chunk);
    }

    fflush(stdout);
}

void *worker(void *input)
{
    // init the client
    struct workerArgs *args = (struct workerArgs *)input;
    S2Client *client = S2ClientInit(
        db_creds.host,
        args->db_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        numWorkers,
        args->id,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    printf("Worker %d connected to port %d\n", args->id, args->db_port);
    fflush(stdout);

    ChunkQueue *q = ParallelReadGetQueue(client, resultTable, chunkSize, batchSize, threadsPerWorker, true);
    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        printf("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
        fflush(stdout);
    }
    pthread_t readers[threadsPerWorker];
    ReaderThreadArgs readerArgs[threadsPerWorker];

    for (int i = 0; i < threadsPerWorker; i++)
    {
        readerArgs[i].worker_id = args->id;
        readerArgs[i].id = i;
        readerArgs[i].queue = q;
        readerArgs[i].is_first_pass = true;
        readerArgs[i].n_chunks_read = 0;
        readerArgs[i].chunks_read = (ReceivedChunk *)malloc(100 * sizeof(ReceivedChunk));

        pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
    }

    for (int i = 0; i < threadsPerWorker; i++)
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

    printf("...Starting second pass in %d\n", args->id);

    // read the second time
    ChunkQueue *q_multi = ParallelReadGetQueue(client, resultTable, chunkSize, batchSize, threadsPerWorker, true);

    for (int i = 0; i < threadsPerWorker; i++)
    {
        readerArgs[i].queue = q_multi;
        readerArgs[i].is_first_pass = false;

        pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
    }

    for (int i = 0; i < threadsPerWorker; i++)
    {
        pthread_join(readers[i], NULL);
    }

    // free the queue
    ChunkQueueFree(q_multi);

    if (S2Errno(client))
    {
        printf("S2 Error in controller %s\n", S2Error(client));
        fflush(stdout);
    }

    // free the client
    S2ClientFree(client);

    return NULL;
}

void main_test(S2Client *client)
{
    int agg_ports[numWorkers];
    // use MA for every connection
    for (int i = 0; i < numWorkers; ++i)
    {
        agg_ports[i] = db_creds.ma_port;
    }

    // init the parallel read in multi-pass mode
    ParallelReadInit(client, resultTable, "SELECT * FROM t_mult", true, NULL, 0);
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
    assert(TOTAL == TOTAL_SINGLE_ROWS);
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
        batchSize = atoi(argv[3]);
    }
    else
    {
        if (argc != 1)
        {
            printf("Exiting... Correct usage: multi_pass_test <numWorkers> <threadsPerWorker> <batchSize>\n");
            exit(1);
        }
    }
    EH.callback.setError = dummyHandleError;

    const char *version = S2GetClientVersion();
    printf("libs2client version: %s\n", version);

    // init the client
    S2Client *client = S2ClientInit(
        db_creds.host,
        db_creds.ma_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        numWorkers,
        -1,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    // check the partition number
    int p = GetPartitionsNumber(client);
    printf("Number of partitions: %d\n", p);

    prepare_mult(client);

    main_test(client);

    // free the client
    S2ClientFree(client);
    return 0;
}
