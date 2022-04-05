#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "s2_client_extern.h"
#include "hdat_read_extern.h"
#include "hdat_write_extern.h"

#include "test/db_creds.h"
#include "test/helpers.h"

#define chunkSize 104850
#define nTableRows 4

int numWorkers = 1;
int threadsPerWorker = 2;
int queueCapacity = 2;

bool printInfo = 0;

const char *resultTable = "tmp";
const char *queryMain = "SELECT dt, d, t FROM t_date_time ORDER BY dt";

const struct ParsedDateTime TIME_TEST_DATA[nTableRows] =
    {
        {-61854364800000000,
         -715907,
         0},
        {-1234567,
         -50,
         -1000},
        {0,
         0,
         0},
        {10,
         10,
         10},
};

void write_data(S2Client *client)
{
    Chunk *chunk = allocChunk(chunkSize);
    RowSchema *schema = GetTableRowSchema(client, "t_date_time", &EH.callback);

    IF_INFO(PrintRowSchema(schema));

    SuperChunkWriter *w = CreateWriter(chunk, schema, &EH.callback);
    for (int i = 0; i < nTableRows; ++i)
    {
        WriteInt64(w, TIME_TEST_DATA[i].datetime);
        WriteInt32(w, TIME_TEST_DATA[i].date);
        WriteInt64(w, TIME_TEST_DATA[i].time);
        WriteRowEnd(w);
    }
    LoadDataWrite(client, chunk, schema, "t_date_time", &EH.callback);

    WriterFree(w);
    RowSchemaFree(schema);
    ChunkFree(chunk);
    free(chunk);
}

void read_data(S2Client *client)
{
    ChunkQueue *q = QueryGetQueue(client, queryMain, chunkSize, queueCapacity, NULL);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
        return;
    }

    int dummy_id;
    int err = 0;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    SuperChunkReader *r = CreateReader(chunk, NULL, &EH.callback);
    RowSchema *s;

    while (GetNextChunk(q, 0, &dummy_id, chunk, &EH.callback))
    {
        assert(err == 0 && "GetNextChunk failed in non parallel mode");
        if (!numReceived)
        {
            s = GetRowSchema(q);
            IF_INFO(PrintRowSchema(s));
        }
        numReceived++;
        IF_INFO(PrintChunk(r, chunk, s));

        struct ParsedDateTime chunkData;
        int current_offset = 0;
        assert(chunk->row_count == nTableRows && "Chunk contains wrong number of rows");
        for (uint64_t i = 0; i < chunk->row_count; ++i)
        {
            current_offset = parseDateTimeChunkRow(chunk, current_offset, &chunkData);
            if (chunkData.datetime != TIME_TEST_DATA[i].datetime)
            {
                PRINT_ERROR("Expected %d, got %d ", TIME_TEST_DATA[i].datetime, chunkData.datetime);
                assert(0);
            }
            assert(chunkData.date == TIME_TEST_DATA[i].date);
            assert(chunkData.time == TIME_TEST_DATA[i].time);
        }

        ChunkFree(chunk);
    }
    free(chunk);

    // free the queue
    ChunkQueueFree(q);
    printf("[SUCCESS] data types test passed\n");
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
    EH.callback.setError = dummyHandleError;

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
    int err = 0;
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS t_date_time", &err);
    assert(!err && "Failed to DROP TABLE");

    ExecuteDDLQuery(client, "CREATE TABLE t_date_time(dt datetime(6), d date, t time(6))", &err);
    assert(!err && "Failed to CREATE TABLE");

    write_data(client);
    read_data(client);

    ExecuteDDLQuery(client, "DROP TABLE t_date_time", &err);
    assert(!err && "Failed to DROP TABLE");

    // free the client
    S2ClientFree(client);
    printf("[SUCCESS] time test passed!\n");

    return 0;
}
