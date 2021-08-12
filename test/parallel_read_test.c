#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "s2_client_extern.h"
#include "hdat_read_extern.h"

#include "test/db_creds.h"
#include "test/helpers.h"

#define chunkSize 1048576
int numWorkers = 2;
int threadsPerWorker = 5;
int queueCapacity = 3;

bool printInfo = 0;

const char *queryMain = "SELECT * FROM small_test";
const char *queryPartition = "SELECT * FROM partition_test";
const char *resultTable = "tmp";
static unsigned _Atomic TOTAL = ATOMIC_VAR_INIT(0);

// check that each partition key is present in none or only one of threads
void check_affinity(ReaderThreadArgs readerArgs[])
{
    int marker = 0;
    for (int key = 0; key < nSmallTestRows; ++key)
    {
        for (int thread_id = 0; thread_id < threadsPerWorker; thread_id++)
        {
            if (readerArgs[thread_id].partition_keys_counter[key])
            {
                ++marker;
            }
        }
        assert(marker == 0 || marker == 1);
        marker = 0;
    }
}

void *reader_thread(void *input)
{
    ReaderThreadArgs *args = (struct ReaderThreadArgs *)input;

    int partitionId;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    PRINT_INFO("Thread %d allocated chunk %p\n", args->id, chunk);

    int numReceived = 0;

    RowSchema *s = GetRowSchema(args->queue);
    assert(s && "GetRowSchema failed");
    IF_INFO(PrintRowSchema(s));

    while (GetNextChunk(args->queue, args->id, &partitionId, chunk, &EH.callback))
    {
        numReceived++;

        TOTAL += RecordChunk(chunk, printInfo, args, true);
        ChunkFree(chunk);
    }

    free(chunk);

    PRINT_INFO("Thread %d read %d chunks\n", args->id, numReceived);
}

void *worker(void *input)
{
    // init the client
    WorkerArgs *args = (WorkerArgs *)input;
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
    PRINT_INFO("Worker %d connected to port %d\n", args->id, args->db_port);

    ChunkQueue *q = ParallelReadGetQueue(client, resultTable, chunkSize, queueCapacity, threadsPerWorker, false);
    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
        return NULL;
    }
    pthread_t readers[threadsPerWorker];
    ReaderThreadArgs readerArgs[threadsPerWorker];

    for (int i = 0; i < threadsPerWorker; i++)
    {
        readerArgs[i].worker_id = args->id;
        readerArgs[i].id = i;
        readerArgs[i].queue = q;
        readerArgs[i].mode = FIRST_PASS;
        readerArgs[i].n_chunks_read = 0;
        readerArgs[i].chunks_read = (ReceivedChunk *)malloc(chunkBufferSize * sizeof(ReceivedChunk));
        memset(readerArgs[i].partition_keys_counter, 0, sizeof readerArgs[i].partition_keys_counter);

        pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
    }

    for (int i = 0; i < threadsPerWorker; i++)
    {
        pthread_join(readers[i], NULL);
    }
    if (args->checkAffinity)
    {
        check_affinity(readerArgs);
    }

    // free the queue
    PRINT_INFO("Calling ChunkQueueFree...\n");
    ChunkQueueFree(q);

    if (S2Errno(client)) PRINT_ERROR("S2 Error in controller %s\n", S2Error(client));

    // free the client
    S2ClientFree(client);

    return NULL;
}

void ddl_test(S2Client *client)
{
    int err = 0;
    ExecuteDDLQuery(client, "CREATE TABLE ddl_test(col_0 INT)", &err);
    if (err)
    {
        PRINT_ERROR("Error creating table: %s\n", S2Error(client));
        return;
    }

    ExecuteDDLQuery(client, "INSERT INTO ddl_test VALUES (1), (2), (3)", &err);
    if (err)
    {
        PRINT_ERROR("Error inserting data: %s\n", S2Error(client));
        return;
    }

    ExecuteDDLQuery(client, "DROP TABLE ddl_test", &err);
    if (err) PRINT_ERROR("Error dropping table: %s\n", S2Error(client));
}

void error_test(S2Client *client)
{
    // invalid query
    ParallelReadInit(client, resultTable, "SELECT * FROM small_test WHERE non_defined_func(i) = 1", false, NULL, 0);

    PRINT_INFO("[EXPECTED] Invalid query error: %d %s\n", S2Errno(client), S2Error(client));

    assert(S2Errno(client));
    ParallelReadFree(client, resultTable);
}

void
parallel_test(
    S2Client *client,
    const char *query,
    const char *const *const partitionByCols,
    const int partitionByColsLen,
    bool checkAffinity)
{
    int agg_ports[numWorkers];
    // use MA for every connection
    for (int i = 0; i < numWorkers; ++i)
    {
        agg_ports[i] = db_creds.ma_port;
    }
    // init the parallel read
    ParallelReadInit(client, resultTable, query, false, partitionByCols, partitionByColsLen);
    if (S2Errno(client)) PRINT_ERROR("S2 Error in controller: %d %s\n", S2Errno(client), S2Error(client));

    // start "CAS worker" threads
    pthread_t workers[numWorkers];
    WorkerArgs w_args[numWorkers];
    for (int i = 0; i < numWorkers; i++)
    {
        w_args[i].id = i;
        w_args[i].db_port = agg_ports[i];
        w_args[i].checkAffinity = checkAffinity;
        pthread_create(&workers[i], NULL, worker, &w_args[i]);
    }

    // join worker threads
    for (int i = 0; i < numWorkers; i++)
    {
        pthread_join(workers[i], NULL);
    }
    PRINT_INFO("Processed TOTAL %d rows\n", TOTAL);

    // clean up parallel reading
    ParallelReadFree(client, resultTable);
}

void non_parallel_test(S2Client *client)
{
    const char *query_bad = "SELECT table_name, something_wrong FROM information_schema.tables";

    ChunkQueue *q_bad = QueryGetQueue(
        client,
        query_bad,
        200,
        queueCapacity);

    assert(S2Errno(client));
    PRINT_INFO("[EXPECTED] S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));

    ChunkQueueFree(q_bad);

    const char *query = "SELECT table_name FROM information_schema.tables";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        200,
        queueCapacity);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client)) PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));

    int dummy_partition;
    int err = 0;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        assert(err == 0 && "GetNextChunk failed in non parallel mode");
        if (!numReceived)
        {
            RowSchema *s = GetRowSchema(q);
            IF_INFO(PrintRowSchema(s));
        }
        assert(!err);
        numReceived++;

        int current_offset = 0;
        for (int i = 0; i < chunk->row_count; ++i)
        {
            int64_t offset, len;
            memcpy(&offset, chunk->m_ptr + current_offset, 8);
            memcpy(&len, chunk->m_ptr + current_offset + 8, 8);
            current_offset += 16;
            if (printInfo)
            {
                printf("Got table #%d: ", i);
                for (int j = 0; j < len; j++)
                {
                    printf("%c", (chunk->m_ptr + offset + j)[0]);
                }
                printf("\n");
                fflush(stdout);
            }
        }

        ChunkFree(chunk);
    }
    free(chunk);
    ChunkQueueFree(q);
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
        queueCapacity = atoi(argv[4]);
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
    int nPartitions = GetPartitionsNumber(client);
    assert(nPartitions);
    PRINT_INFO("Number of partitions: %d\n", nPartitions);

    setup_small_test_table(client);

    // misc tests
    ddl_test(client);

    non_parallel_test(client);

    error_test(client);

    // parallel read modes tests
    parallel_test(client, queryMain, NULL, 0, false);

    const char *cols[3] = {"i1", "i2"};
    parallel_test(client, queryMain, cols, 2, false);

    mult_table(client, "small_test", "partition_test", 10);
    const char *cols1[3] = {"d1"};
    parallel_test(client, queryPartition, cols1, 1, true);

    // int err = 0;
    // ExecuteDDLQuery(client, "DROP TABLE partition_test", &err);
    // cleanup_small_test_table(client);

    // free the client
    S2ClientFree(client);

    printf("[SUCCESS] parallel read test passed!\n");

    return 0;
}
