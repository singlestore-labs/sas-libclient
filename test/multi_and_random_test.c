#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>

#include "s2_client_extern.h"
#include "test/db_creds.h"
#include "test/helpers.h"

#define chunkSize 10485
#define chunkBufferSize 1000

int numWorkers = 2;
int threadsPerWorker = 3;
int batchSize = 5;

const char *resultTable = "tmp";
static unsigned _Atomic TOTAL = ATOMIC_VAR_INIT(0);
static unsigned _Atomic TOTAL_SINGLE_ROWS = ATOMIC_VAR_INIT(0);

const char *testQuery = "SELECT * FROM t_mult";

void
prepare_mult(
    S2Client *client,
    int scaleFactor)
{
    int err;
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS t_mult", &err);
    ExecuteDDLQuery(client, "CREATE TABLE t_mult AS SELECT * FROM t", &err);

    for (int i = 0; i < scaleFactor; ++i)
    {
        ExecuteDDLQuery(client, "INSERT INTO t_mult (SELECT * FROM t_mult)", &err);
    }
}

void *reader_thread(void *input)
{
    ReaderThreadArgs *args = (ReaderThreadArgs *)input;

    int partitionId;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));

    int numReceived = 0;

    if (args->mode == FIRST_PASS)
    {
        RowSchema *s = GetRowSchema(args->queue);
        assert(s && "GetRowSchema failed");

        while (GetNextChunk(args->queue, &partitionId, chunk, &EH.callback))
        {
            numReceived++;

            TOTAL += dummyProcessChunk(chunk, false, args);
            ChunkFree(chunk);
        }
        printf("Finished first pass: worker %d thread %d read %d chunks\n", args->worker_id, args->id, numReceived);

        free(chunk);
    }
    if (args->mode == SECOND_PASS)
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
        free(chunk);
        return NULL;
    }
    if (args->mode == RANDOM_READ)
    {
        if (args->n_chunks_read)
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
                    // printf(
                    //     "Got chunk in GetChunkRow: row_count %d, size %d, consumed %d\n",
                    //     chunk->row_count,
                    //     chunk->m_size,
                    //     chunk->consumed_size);
                    TOTAL_SINGLE_ROWS++;
                    numReceived++;
                }
            }
        }
        printf(
            "Finished GetChunkRow in thread %d worker %d, rows read: %d\n",
            args->id,
            args->worker_id,
            numReceived);
        free(chunk);
        return NULL;
    }
    return NULL;
}

void *worker(void *input)
{
    // init the client
    WorkerArgs *w_args = (WorkerArgs *)input;
    S2Client *client = S2ClientInit(
        db_creds.host,
        w_args->db_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        numWorkers,
        w_args->id,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    printf("Worker %d connected to port %d\n", w_args->id, w_args->db_port);
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
        readerArgs[i].worker_id = w_args->id;
        readerArgs[i].id = i;
        readerArgs[i].queue = q;
        readerArgs[i].mode = FIRST_PASS;
        readerArgs[i].n_chunks_read = 0;
        readerArgs[i].chunks_read = (ReceivedChunk *)malloc(chunkBufferSize * sizeof(ReceivedChunk));

        pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
    }

    for (int i = 0; i < threadsPerWorker; i++)
    {
        pthread_join(readers[i], NULL);
    }

    // free the queue
    ChunkQueueFree(q);
    printf("...Finished first pass in worker %d\n", w_args->id);

    if (S2Errno(client))
    {
        printf("S2 Error in controller %s\n", S2Error(client));
        fflush(stdout);
    }

    printf("...Starting second pass in worker %d\n", w_args->id);

    // read the second time
    ChunkQueue *q_multi = ParallelReadGetQueue(client, resultTable, chunkSize, batchSize, threadsPerWorker, true);

    for (int i = 0; i < threadsPerWorker; i++)
    {
        readerArgs[i].queue = q_multi;
        readerArgs[i].mode = SECOND_PASS;

        pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
    }

    for (int i = 0; i < threadsPerWorker; i++)
    {
        pthread_join(readers[i], NULL);
    }

    if (w_args->needRandomRead)
    {
        for (int i = 0; i < threadsPerWorker; i++)
        {
            readerArgs[i].queue = q_multi;
            readerArgs[i].mode = RANDOM_READ;

            pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
        }

        for (int i = 0; i < threadsPerWorker; i++)
        {
            pthread_join(readers[i], NULL);
        }
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

void
main_test(
    S2Client *client,
    bool needRandomRead)
{
    TOTAL = 0;
    TOTAL_SINGLE_ROWS = 0;
    int agg_ports[numWorkers];
    // use MA for every connection
    for (int i = 0; i < numWorkers; ++i)
    {
        agg_ports[i] = db_creds.ma_port;
    }

    // init the parallel read in multi-pass mode
    ParallelReadInit(client, resultTable, testQuery, true, NULL, 0);
    if (S2Errno(client))
    {
        printf("S2 Error in controller: %d %s\n", S2Errno(client), S2Error(client));
        fflush(stdout);
    }

    // start "CAS worker" threads
    pthread_t workers[numWorkers];
    int i;
    WorkerArgs args[numWorkers];
    for (i = 0; i < numWorkers; i++)
    {
        args[i].id = i;
        args[i].db_port = agg_ports[i];
        args[i].needRandomRead = needRandomRead;
        pthread_create(&workers[i], NULL, worker, &args[i]);
    }

    // join worker threads
    for (i = 0; i < numWorkers; i++)
    {
        pthread_join(workers[i], NULL);
    }
    if (needRandomRead)
    {
        assert(TOTAL == TOTAL_SINGLE_ROWS);
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

    prepare_mult(client, 12);

    main_test(client, false);
    printf("SUCCESS in multi pass!\n");

    prepare_mult(client, 6);

    main_test(client, true);
    printf("SUCCESS in random read!\n");

    // free the client
    S2ClientFree(client);
    return 0;
}
