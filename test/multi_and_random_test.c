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

int numWorkers = 2;
int threadsPerWorker = 3;
int batchSize = 5;

bool printInfo = 0;

const char *resultTable = "tmp";
const char *keyCol = "rowId";

static unsigned _Atomic TOTAL = ATOMIC_VAR_INIT(0);
static unsigned _Atomic TOTAL_SINGLE_ROWS = ATOMIC_VAR_INIT(0);

const char *testQuery = "SELECT * FROM multi_pass_test";
const char *multiPassTable = "multi_pass_test";

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

        while (GetNextChunk(args->queue, args->id, &partitionId, chunk, &EH.callback))
        {
            numReceived++;

            TOTAL += RecordChunk(chunk, false, args);
            ChunkFree(chunk);
        }
        PRINT_INFO("Finished first pass: worker %d thread %d read %d chunks\n", args->worker_id, args->id, numReceived);
        free(chunk);
        return NULL;
    }
    if (args->mode == SECOND_PASS)
    {
        int offset = args->id % 2 ? 0 : 10;
        PRINT_INFO("Starting GetChunkMulti in thread %d worker %d\n", args->id, args->worker_id);

        while (numReceived < args->n_chunks_read - offset && GetChunkMulti(
                                                                 args->queue,
                                                                 (args->chunks_read)[numReceived].partition_id,
                                                                 (args->chunks_read)[numReceived].chunk_id,
                                                                 chunk,
                                                                 &EH.callback))
        {
            if (chunk->row_count != args->chunks_read[numReceived].row_count)
            {
                PRINT_ERROR(
                    "got %d rows, expected %d, thread %d\n ",
                    chunk->row_count,
                    args->chunks_read[numReceived].row_count,
                    args->id);
            }
            assert(chunk->row_count == args->chunks_read[numReceived].row_count);
            numReceived++;

            ChunkFree(chunk);
        }
        if (!offset && numReceived != args->n_chunks_read)
        {
            PRINT_ERROR(
                "got %d chunks, expected %d, thread %d\n ",
                numReceived,
                args->n_chunks_read,
                args->id);
        }
        offset ? assert(args->n_chunks_read < offset || numReceived == args->n_chunks_read - offset)
               : assert(numReceived == args->n_chunks_read);

        PRINT_INFO(
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
            uint64_t rowId;
            bool result;
            for (int i = 0; i < args->n_chunks_read; ++i)
            {
                for (uint64_t row_num = 0; row_num < args->chunks_read[i].row_count; ++row_num)
                {
                    rowId = args->read_type == ReadTypeResultTable ? args->chunks_read[i].upto_row_count + row_num
                                                                   : args->chunks_read[i].row_ids_read[row_num];
                    result = GetChunkRow(
                        args->queue,
                        args->chunks_read[i].partition_id,
                        rowId,
                        args->id,
                        chunk,
                        &EH.callback);
                    assert(result);
                    ChunkFree(chunk);
                    TOTAL_SINGLE_ROWS++;
                    numReceived++;
                }
                result = GetChunkMultipleRows(
                    args->queue,
                    args->chunks_read[i].partition_id,
                    args->chunks_read[i].row_ids_read,
                    args->chunks_read[i].row_count,
                    args->id,
                    chunk,
                    chunkSize,
                    &EH.callback);
                assert(result);
            }
        }
        PRINT_INFO(
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
        db_creds.ssl_ca,
        numWorkers,
        w_args->id,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    PRINT_INFO("Worker %d connected to port %d\n", w_args->id, w_args->db_port);

    ChunkQueue *q = ParallelReadGetQueue(
        client,
        resultTable,
        testQuery,
        multiPassTable,
        keyCol,
        w_args->read_type,
        NULL,
        0,
        chunkSize,
        batchSize,
        threadsPerWorker,
        true,
        &EH.callback);
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
        readerArgs[i].read_type = w_args->read_type;
        readerArgs[i].check_partition_order = false;
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
    PRINT_INFO("...Finished first pass in worker %d\n", w_args->id);

    if (S2Errno(client))
    {
        printf("S2 Error in worker %s\n", S2Error(client));
        fflush(stdout);
    }

    PRINT_INFO("...Starting second pass in worker %d\n", w_args->id);

    // read the second time
    ChunkQueue *q_multi = ParallelReadGetQueue(
        client,
        resultTable,
        testQuery,
        multiPassTable,
        keyCol,
        w_args->read_type,
        NULL,
        0,
        chunkSize,
        batchSize,
        threadsPerWorker,
        true,
        &EH.callback);

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

    if (w_args->need_random_read)
    {
        CalculatePartitionRows(readerArgs, threadsPerWorker);
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

    if (S2Errno(client)) PRINT_ERROR("S2 Error in controller %s\n", S2Error(client));

    // free the client
    S2ClientFree(client);

    for (int i = 0; i < threadsPerWorker; i++)
    {
        free(readerArgs[i].chunks_read);
    }

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
    ParallelReadType readType = ReadTypeUnknown;
    // ParallelReadType readType = ReadTypeColumnStoreTable;

    const char *partCols[2] = {"i1", "d1"};
    const char *partOrderCols[2] = {"i1", "rowId"};

    readType = ParallelReadInit(
        client,
        resultTable,
        testQuery,
        multiPassTable,
        keyCol,
        readType,
        true,
        partCols,
        2,
        partOrderCols,
        2);
    if (S2Errno(client)) PRINT_ERROR("S2 Error in controller: %d %s\n", S2Errno(client), S2Error(client));

    printf("Using read type: %d\n", readType);

    // start "CAS worker" threads
    pthread_t workers[numWorkers];
    int i;
    WorkerArgs args[numWorkers];
    for (i = 0; i < numWorkers; i++)
    {
        args[i].id = i;
        args[i].db_port = agg_ports[i];
        args[i].need_random_read = needRandomRead;
        args[i].read_type = readType;
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
    PRINT_INFO("Processed TOTAL %d rows\n", TOTAL);

    // clean up parallel reading
    ParallelReadFree(client, resultTable, readType, &EH.callback);
}

int
main(
    int argc,
    char *argv[])
{
    if (argc > 1)
    {
        printInfo = atoi(argv[1]);
    }
    if (argc == 5)
    {
        numWorkers = atoi(argv[2]);
        threadsPerWorker = atoi(argv[3]);
        batchSize = atoi(argv[4]);
    }

    EH.callback.setError = dummyHandleError;

    const char *version = S2GetClientVersion();
    PRINT_INFO("libs2client version: %s\n", version);

    // init the client
    S2Client *client = S2ClientInit(
        db_creds.host,
        db_creds.ma_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        db_creds.ssl_ca,
        numWorkers,
        -1,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    setup_multi_pass_table(client);

    main_test(client, false);
    printf("[SUCCESS] multi pass test passed!\n");

    main_test(client, true);
    printf("[SUCCESS] random read test passed!\n");

    // ExecuteDDLQuery(client, "DROP TABLE multi_pass_test", &EH.callback);

    // free the client
    S2ClientFree(client);
    return 0;
}
