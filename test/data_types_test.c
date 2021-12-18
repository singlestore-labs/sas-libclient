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

int numWorkers = 1;
int threadsPerWorker = 2;
int queueCapacity = 2;

bool printInfo = 0;

int nTableRows = 1000;

const char *resultTable = "tmp";
const char *queryMain = "SELECT * FROM superchunk_table";

const struct ParsedTestChunk DATA_TYPES_TEST_DATA =
    {
        -1460002,
        12507,
        1.23456789012345,
        {"textVAL_рус",
         14},
        {"LOOONGVAL",
         9},
        {"varcharVAL",
         10},
        {"varbinaryVAL",
         12},
        "русVAL",
        "fbVAL",
        1935835200000000,
        31666394987654,
        22405,
        40271000000,
};

void null_test(S2Client *client)
{
    int err = 0;

    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS null_test", &err);
    if (err) PRINT_ERROR("Error dropping table: %s\n", S2Error(client));
    assert(!err);

    ExecuteDDLQuery(
        client,
        "CREATE TABLE null_test (\
        ibigint BIGINT(20),\
        iint INT,\
        ddouble DOUBLE,\
        vtext TEXT,\
        vlongtext LONGTEXT,\
        vvarchar_10 VARCHAR(10),\
        vvarbinary_20 VARBINARY(20),\
        fchar_16 CHAR(16),\
        fbbinary_8 BINARY(9),\
        idatetime DATETIME,\
        idatetime_6 DATETIME(6),\
        idate DATE,\
        itime TIME\
        )",
        &err);
    if (err) printf("Error creating table: %s\n", S2Error(client));
    assert(!err);

    ExecuteDDLQuery(
        client,
        "INSERT INTO null_test VALUES (\
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        &err);
    if (err) PRINT_ERROR("Error inserting data: %s\n", S2Error(client));
    assert(!err);

    const char *query = "SELECT * FROM null_test";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        chunkSize,
        queueCapacity);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client)) PRINT_ERROR("S2 Error in null_test: %d %s\n", S2Errno(client), S2Error(client));
    assert(!S2Errno(client));

    int dummy_partition = 0;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));

    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        assert(!err && "GetNextChunk failed in null_test in non parallel mode");

        int current_offset = 0;
        struct ParsedTestChunk chunkData;
        for (int i = 0; i < chunk->row_count; ++i)
        {
            current_offset = parseTestChunkRow(chunk, current_offset, &chunkData);
            assert(chunkData.int_64 == int64Null);
            assert(chunkData.int_32 == int32Null);
            assert(isnan(chunkData.double_val));

            assert(chunkData.variable_text.len == 0);
            assert(chunkData.variable_char.len == 0);
            assert(chunkData.variable_binary.len == 0);
            assert(strlen(chunkData.fixed_char) == 0);
            assert(strlen(chunkData.fixed_binary) == 0);

            assert(chunkData.date_time == int64Null);
            assert(chunkData.date == int32Null);
            assert(chunkData.time == int64Null);
        }
        ChunkFree(chunk);
    }
    free(chunk);

    // free the queue
    ChunkQueueFree(q);

    ExecuteDDLQuery(client, "DROP TABLE null_test", &err);
    if (err) printf("Error dropping table: %s\n", S2Error(client));
    assert(!err);
    printf("[SUCCESS] NULL test passed\n");
}

void read_test(S2Client *client)
{
    ChunkQueue *q = QueryGetQueue(client, queryMain, chunkSize, queueCapacity);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
        return;
    }

    int dummy_partition;
    int err = 0;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    SuperChunkReader *r = CreateReader(chunk, NULL, &EH.callback);
    RowSchema *s;

    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        assert(err == 0 && "GetNextChunk failed in non parallel mode");
        if (!numReceived)
        {
            s = GetRowSchema(q);
            IF_INFO(PrintRowSchema(s));
        }
        numReceived++;
        IF_INFO(PrintChunk(r, chunk, s));

        struct ParsedTestChunk chunkData;
        int current_offset = 0;
        for (int i = 0; i < chunk->row_count; ++i)
        {
            current_offset = parseTestChunkRow(chunk, current_offset, &chunkData);
            assert(chunkData.int_64 == DATA_TYPES_TEST_DATA.int_64);
            assert(chunkData.int_32 == DATA_TYPES_TEST_DATA.int_32);
            assert(chunkData.double_val == DATA_TYPES_TEST_DATA.double_val);

            assert(chunkData.variable_text.len == DATA_TYPES_TEST_DATA.variable_text.len);
            assert(chunkData.variable_char.len == DATA_TYPES_TEST_DATA.variable_char.len);
            assert(chunkData.variable_binary.len == DATA_TYPES_TEST_DATA.variable_binary.len);

            assert(!strncmp(
                chunkData.variable_text.data,
                DATA_TYPES_TEST_DATA.variable_text.data,
                DATA_TYPES_TEST_DATA.variable_text.len));
            assert(!strncmp(
                chunkData.variable_char.data,
                DATA_TYPES_TEST_DATA.variable_char.data,
                DATA_TYPES_TEST_DATA.variable_char.len));
            assert(!strncmp(
                chunkData.variable_binary.data,
                DATA_TYPES_TEST_DATA.variable_binary.data,
                DATA_TYPES_TEST_DATA.variable_binary.len));

            assert(!strcmp(chunkData.fixed_char, DATA_TYPES_TEST_DATA.fixed_char));
            assert(!strcmp(chunkData.fixed_binary, DATA_TYPES_TEST_DATA.fixed_binary));

            // these values have been found using online epoch converter lol
            assert(chunkData.date_time == DATA_TYPES_TEST_DATA.date_time);
            assert(chunkData.date_time_6 == DATA_TYPES_TEST_DATA.date_time_6);
            assert(chunkData.date == DATA_TYPES_TEST_DATA.date);
            assert(chunkData.time == DATA_TYPES_TEST_DATA.time);
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
        numWorkers,
        -1,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    null_test(client);

    setup_superchunk_table(client, 10);
    read_test(client);
    cleanup_superchunk_table(client);
    // free the client
    S2ClientFree(client);
    return 0;
}
