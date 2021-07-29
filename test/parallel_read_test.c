
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

const char *queryMain = "SELECT * FROM t";
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
    printf("[DUMMMY ERROR CALLBACK] Got error: %d %s\n", error, errorString);
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
            "Got chunk: %p, m_ptr: %p, partition_id: %d, m_size: %d, row_count: %d, consumed_size: %d\n",
            chunk,
            chunk->m_ptr,
            chunk->partition_id,
            chunk->m_size,
            chunk->row_count,
            chunk->consumed_size);
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
        printf("The numbers in chunk are: %ld %ld %f %f. ", x, y, u, v);
        printf("The string is: ");
        for (int i = 0; i < len; i++)
        {
            printf("%c", (chunk->m_ptr + offset + i)[0]);
        }
        printf("\n");
        fflush(stdout);
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
        printf("%d-th column: type %d, name %s; ", i, s->ColumnInfo[i].type, s->ColumnInfo[i].name);
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

    ChunkQueue *q = ParallelReadGetQueue(client, resultTable, chunkSize, queueCapacity, 0, false);
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
    printf("Calling ChunkQueueFree...\n");
    fflush(stdout);
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

void ddl_test(S2Client *client)
{
    int err = 0;
    ExecuteDDLQuery(client, "CREATE TABLE small_test(col_0 INT)", &err);
    if (err)
    {
        printf("Error creating table: %s\n", S2Error(client));
        fflush(stdout);
    }

    ExecuteDDLQuery(client, "INSERT INTO small_test VALUES (1), (2), (3)", &err);
    if (err)
    {
        printf("Error inserting data: %s\n", S2Error(client));
        fflush(stdout);
    }

    ExecuteDDLQuery(client, "DROP TABLE small_test", &err);
    if (err)
    {
        printf("Error dropping table: %s\n", S2Error(client));
        fflush(stdout);
    }
}

void null_test(S2Client *client)
{
    int err = 0;
    ExecuteDDLQuery(client, "CREATE TABLE null_test(i INT, d DOUBLE, t TEXT)", &err);
    if (err)
    {
        printf("Error creating table: %s\n", S2Error(client));
        fflush(stdout);
    }

    ExecuteDDLQuery(client, "INSERT INTO null_test VALUES (NULL, NULL, NULL)", &err);
    if (err)
    {
        printf("Error inserting data: %s\n", S2Error(client));
        fflush(stdout);
    }

    const char *query = "SELECT * FROM null_test";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        200,
        queueCapacity);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        printf("S2 Error in null_test: %d %s\n", S2Errno(client), S2Error(client));
        fflush(stdout);
    }

    int dummy_partition;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    while (GetNextChunk(q, &dummy_partition, chunk, &EH.callback))
    {
        assert(err == 0 && "GetNextChunk failed in null_test in non parallel mode");
        if (!numReceived)
        {
            RowSchema *s = GetRowSchema(q);
            for (int i = 0; i < s->numColumns; ++i)
            {
                printf("%d-th row: type %d, name %s\n", i, s->ColumnInfo[0].type, s->ColumnInfo[0].name);
                fflush(stdout);
            }
        }
        assert(err == 0);
        numReceived++;

        int current_offset = 0;
        for (int i = 0; i < chunk->row_count; ++i)
        {
            int64_t int_val, offset, len;
            double double_val;

            memcpy(&int_val, chunk->m_ptr + current_offset, 8);
            memcpy(&double_val, chunk->m_ptr + current_offset + 8, 8);
            memcpy(&offset, chunk->m_ptr + current_offset + 16, 8);
            memcpy(&len, chunk->m_ptr + current_offset + 24, 8);
            current_offset += 32;
            assert(len == 0);
            assert(int_val == int64Null);
            assert(double_val == doubleNull);
        }

        ChunkFree(chunk);
    }
    free(chunk);

    // free the queue
    ChunkQueueFree(q);

    ExecuteDDLQuery(client, "DROP TABLE null_test", &err);
    if (err)
    {
        printf("Error dropping table: %s\n", S2Error(client));
        fflush(stdout);
    }
}

void error_test(S2Client *client)
{
    // invalid query
    ParallelReadInit(client, resultTable, "SELECT * FROM t WHERE non_defined_func(i) = 1", false, NULL, 0);

    printf("[EXPECTED] Invalid query error: %d %s\n", S2Errno(client), S2Error(client));
    fflush(stdout);

    assert(S2Errno(client));
    ParallelReadFree(client, resultTable);
}

void
parallel_test(
    S2Client *client,
    const char *query,
    const char *const *const partitionByCols,
    const int n)
{
    int agg_ports[numWorkers];
    // use MA for every connection
    for (int i = 0; i < numWorkers; ++i)
    {
        agg_ports[i] = db_creds.ma_port;
    }
    // init the parallel read
    ParallelReadInit(client, resultTable, query, false, partitionByCols, n);
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

void non_parallel_test(S2Client *client)
{
    const char *query_bad = "SELECT table_name, something_wrong FROM information_schema.tables";

    ChunkQueue *q_bad = QueryGetQueue(
        client,
        query_bad,
        200,
        queueCapacity);

    assert(S2Errno(client));
    printf("[EXPECTED] S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
    fflush(stdout);
    ChunkQueueFree(q_bad);

    const char *query = "SELECT table_name FROM information_schema.tables";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        200,
        queueCapacity);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        printf("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
        fflush(stdout);
    }

    int dummy_partition;
    int err = 0;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    while (GetNextChunk(q, &dummy_partition, chunk, &EH.callback))
    {
        assert(err == 0 && "GetNextChunk failed in non parallel mode");
        if (!numReceived)
        {
            RowSchema *s = GetRowSchema(q);
            for (int i = 0; i < s->numColumns; ++i)
            {
                printf("%d-th row: type %d, name %s\n", i, s->ColumnInfo[0].type, s->ColumnInfo[0].name);
                fflush(stdout);
            }
        }
        assert(err == 0);
        numReceived++;

        int current_offset = 0;
        for (int i = 0; i < chunk->row_count; ++i)
        {
            int64_t offset, len;
            memcpy(&offset, chunk->m_ptr + current_offset, 8);
            memcpy(&len, chunk->m_ptr + current_offset + 8, 8);
            current_offset += 16;
            // printf("Got table #%d: ", i);
            // for (int j = 0; j < len; j++)
            // {
            //     printf("%c", (chunk->m_ptr + offset + j)[0]);
            // }
            // printf("\n");
            // fflush(stdout);
        }

        ChunkFree(chunk);
    }
    free(chunk);

    // free the queue
    ChunkQueueFree(q);
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

    ddl_test(client);

    non_parallel_test(client);

    null_test(client);

    error_test(client);

    parallel_test(client, queryMain, NULL, 0);

    const char *cols[3] = {"i", "i2"};

    parallel_test(client, queryMain, cols, 2);

    const char *cols1[3] = {"t"};

    parallel_test(client, queryMain, cols1, 1);

    // free the client
    S2ClientFree(client);
    return 0;
}
