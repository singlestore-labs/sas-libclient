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

#define chunkSize 15200
int numWorkers = 2;
int threadsPerWorker = 1;
int queueCapacity = 32;

bool printInfo = 0;

const char *queryMain = "SELECT * FROM `small'test`";
const char *queryWarn = "SELECT * FROM `small'test` WHERE rowId = 'i' OR 1";
const char *queryPartition = "SELECT * FROM partition_test";
const char *smallTable = "small'test";
const char *partitionTable = "partition_test";

const char *resultTable = "tmp";
const char *keyCol = "rowId";

static unsigned _Atomic TOTAL = ATOMIC_VAR_INIT(0);

// check that each partition key is present in none or only one of threads
void check_affinity(ReaderThreadArgs readerArgs[])
{
    int marker = 0;
    for (int key = 0; key < nSmallTestRows; ++key)
    {
        for (int thread_id = 0; thread_id < threadsPerWorker; thread_id++)
        {
            if (readerArgs[thread_id].partition_key_i1_counter[key])
            {
                PRINT_INFO("key %i read by thread %i\n", key, thread_id);
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
    AssertRowSchema(s);
    IF_INFO(PrintRowSchema(s));

    while (GetNextChunk(args->queue, args->id, &partitionId, chunk, &EH.callback))
    {
        numReceived++;

        TOTAL += RecordChunk(chunk, printInfo, args);
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
        db_creds.ssl_ca,
        numWorkers,
        args->id,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");
    PRINT_INFO("Worker %d connected to port %d\n", args->id, args->db_port);

    ChunkQueue *q = ParallelReadGetQueue(
        client,
        resultTable,
        args->query,
        partitionTable,
        keyCol,
        args->read_type,
        args->partition_order_by_cols,
        args->order_by_cols_number,
        chunkSize,
        queueCapacity,
        threadsPerWorker,
        false,
        &EH.callback);
    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        PRINT_ERROR("S2 Error in worker after ParallelReadGetQueue: %d %s\n", S2Errno(client), S2Error(client));
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
        memset(readerArgs[i].partition_key_i1_counter, 0, sizeof readerArgs[i].partition_key_i1_counter);
        readerArgs[i].check_partition_order = args->check_order;

        pthread_create(&readers[i], NULL, reader_thread, &readerArgs[i]);
    }

    for (int i = 0; i < threadsPerWorker; i++)
    {
        pthread_join(readers[i], NULL);
    }
    if (args->check_affinity)
    {
        check_affinity(readerArgs);
    }

    // free the queue
    PRINT_INFO("Calling ChunkQueueFree...\n");
    ChunkQueueFree(q);

    if (S2Errno(client))
        PRINT_ERROR("S2 Error in worker after ChunkQueueFree: %d %s\n", S2Errno(client), S2Error(client));

    // free the client
    S2ClientFree(client);

    return NULL;
}

void ddl_test(S2Client *client)
{
    ExecuteDDLQuery(client, "CREATE TABLE ddl_test(col_0 INT)", &EH.callback);

    int affected = ExecuteDDLQuery(client, "INSERT INTO ddl_test VALUES (1), (2), (3)", &EH.callback);
    assert(affected == 3);

    affected = ExecuteDDLQuery(client, "DELETE FROM ddl_test WHERE col_0 < 2", &EH.callback);
    assert(affected == 1);

    affected = ExecuteDDLQuery(client, "UPDATE ddl_test SET col_0 = col_0 * 2", &EH.callback);
    assert(affected == 2);

    ExecuteDDLQuery(client, "DROP TABLE ddl_test", &EH.callback);
}

void error_test(S2Client *client)
{
    // invalid query
    ParallelReadType readType = ReadTypeUnknown;
    readType = ParallelReadInit(
        client,
        resultTable,
        "SELECT * FROM `small'test` WHERE non_defined_func(i1) = 1",
        NULL,
        keyCol,
        readType,
        false,
        NULL,
        0,
        NULL,
        0);
    if (readType == ReadTypeOriginalTable) return;

    PRINT_INFO("[EXPECTED] Invalid query error: %d %s\n", S2Errno(client), S2Error(client));
    assert(S2Errno(client));
    EH.errorExpected = true;
    ParallelReadFree(client, resultTable, readType, &EH.callback);
    EH.errorExpected = false;
}

void
parallel_test(
    S2Client *client,
    const char *query,
    const char *table,
    const char *const *const partitionByCols,
    const int partitionByColsLen,
    const char *const *const partitionOrderByCols,
    const int partitionOrderByColsNumber,
    bool checkAffinity,
    bool checkOrder)
{
    int agg_ports[numWorkers];
    // use MA for every connection
    for (int i = 0; i < numWorkers; ++i)
    {
        agg_ports[i] = db_creds.ma_port;
    }
    // init the parallel read
    ParallelReadType readType = ReadTypeUnknown;
    // ParallelReadType readType = ReadTypeOriginalTable;
    readType = ParallelReadInit(
        client,
        resultTable,
        query,
        table,
        keyCol,
        readType,
        false,
        partitionByCols,
        partitionByColsLen,
        partitionOrderByCols,
        partitionOrderByColsNumber);
    if (S2Errno(client)) PRINT_ERROR("S2 Error in controller: %d %s\n", S2Errno(client), S2Error(client));
    printf("Using Read Type %d\n", readType);

    // start "CAS worker" threads
    pthread_t workers[numWorkers];
    WorkerArgs w_args[numWorkers];
    for (int i = 0; i < numWorkers; i++)
    {
        w_args[i].id = i;
        w_args[i].db_port = agg_ports[i];
        w_args[i].check_affinity = checkAffinity;
        w_args[i].check_order = checkOrder;
        w_args[i].query = query;
        w_args[i].read_type = readType;
        w_args[i].partition_order_by_cols = partitionOrderByCols;
        w_args[i].order_by_cols_number = partitionOrderByColsNumber;

        pthread_create(&workers[i], NULL, worker, &w_args[i]);
    }

    // join worker threads
    for (int i = 0; i < numWorkers; i++)
    {
        pthread_join(workers[i], NULL);
    }
    PRINT_INFO("Processed TOTAL %d rows\n", TOTAL);

    // clean up parallel reading
    ParallelReadFree(client, resultTable, readType, &EH.callback);
}

void non_parallel_test()
{
    S2Client *client = S2ClientInit(
        db_creds.host,
        db_creds.ma_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        db_creds.ssl_ca,
        1,
        -1,
        &EH.callback);

    int err1 = 0;
    ExecuteDDLQuery(
        client,
        "CREATE TABLE IF NOT EXISTS `t_special_symbols` (\
        `a?` DOUBLE , \
        `b.` DOUBLE , \
        `c$` DOUBLE , \
        SHARD KEY() , \
        KEY USING CLUSTERED COLUMNSTORE())",
        &EH.callback);

    const char *query_symbols = "SELECT `a?`, `b.`, `c$` FROM t_special_symbols";
    ChunkQueue *q_long = QueryGetQueue(
        client,
        (char *)query_symbols,
        200,
        queueCapacity,
        &EH.callback);

    if (S2Errno(client)) PRINT_ERROR("QueryGetQueue failed: %d %s\n", S2Errno(client), S2Error(client));
    assert(q_long && "QueryGetQueue failed");

    ChunkQueueFree(q_long);

    const char *query_bad = "SELECT table_name, something_wrong FROM information_schema.tables";

    EH.errorExpected = true;
    ChunkQueue *q_bad = QueryGetQueue(
        client,
        query_bad,
        200,
        queueCapacity,
        &EH.callback);
    assert(S2Errno(client));

    EH.errorExpected = false;

    ChunkQueueFree(q_bad);

    const char *query = "SELECT table_name :> TEXT FROM information_schema.tables";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        10000,
        1,
        &EH.callback);

    if (S2Errno(client)) PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
    assert(q != NULL && "ChunkQueue is NULL");

    int dummy_partition;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    RowSchema *s = GetRowSchema(q);
    AssertRowSchema(s);
    IF_INFO(PrintRowSchema(s));
    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        numReceived++;

        int current_offset = 0;
        for (uint64_t i = 0; i < chunk->row_count; ++i)
        {
            int64_t offset, len;
            memcpy(&offset, chunk->m_ptr + current_offset, 8);
            memcpy(&len, chunk->m_ptr + current_offset + 8, 8);
            current_offset += 16;
            if (printInfo)
            {
                printf("Got table #%d of len %d: ", i, len);
                for (int j = 0; j < len; j++)
                {
                    printf("%c", (chunk->m_ptr + chunk->m_size - offset + j)[0]);
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
        db_creds.ssl_ca,
        numWorkers,
        -1,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    // check the partition number
    int nPartitions = GetPartitionsNumber(client);
    assert(nPartitions);
    PRINT_INFO("Number of partitions: %d\n", nPartitions);

    setup_small_test_table(client);

    // [TEST] ddl statements
    ddl_test(client);

    // [TEST] regular query processing
    non_parallel_test();

    // [TEST] check correct error handling
    error_test(client);

    // parallel read modes tests
    const char *partCols[3] = {"i1", "rowId"};
    const char *partOrderCols[2] = {"i1", "d1"};
    mult_table(client, smallTable, "partition_test", 10);

    // [TEST] no partitioning
    parallel_test(client, queryWarn, smallTable, NULL, 0, NULL, 0, false, false);

    // [TEST] only partitioning, no order
    parallel_test(client, queryMain, smallTable, partCols, 1, NULL, 0, true, false);

    // [TEST] partitioning and order
    parallel_test(client, queryMain, smallTable, partCols, 2, partOrderCols, 2, false, false);

    // [TEST] partitioning and order with affinity check
    parallel_test(client, queryPartition, partitionTable, partCols, 1, partOrderCols, 2, true, true);

    ExecuteDDLQuery(
        client,
        "CREATE TABLE IF NOT EXISTS show_columns_test(`col_0_]` INT, `col_1_]` TEXT, `col[` DATE)",
        &EH.callback);
    parallel_test(client, "SELECT * FROM show_columns_test", "show_columns_test", NULL, 0, NULL, 0, false, false);

    ExecuteDDLQuery(client, "DROP TABLE partition_test", &EH.callback);
    ExecuteDDLQuery(client, "DROP TABLE show_columns_test", &EH.callback);
    ExecuteDDLQuery(client, "DROP TABLE t_special_symbols", &EH.callback);

    cleanup_small_test_table(client);
    S2ClientFree(client);

    printf("[SUCCESS] parallel read test passed!\n");

    return 0;
}
