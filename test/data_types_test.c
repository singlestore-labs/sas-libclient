#include <assert.h>
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

#define chunkSize 10485
int numWorkers = 1;
int threadsPerWorker = 2;
int queueCapacity = 2;

int nTableRows = 16;

const char *mainTable = "data_type_table";
const char *resultTable = "tmp";
const char *queryMain = "SELECT * FROM data_type_table";

typedef struct variable
{
    char data[120];
    int len;
} variable;

struct ParsedChunk
{
    int64_t int_64;
    int32_t int_32;
    double double_val;
    variable variable_text;
    variable variable_char;
    variable variable_binary;
    char fixed_char[49];
    char fixed_binary[9];
};

const struct ParsedChunk INPUT =
    {
        -1460002,
        12507,
        1.2,
        {"textVAL_рус",
         14},
        {"varcharVAL",
         10},
        {"varbinaryVAL",
         12},
        "русVAL",
        "fbVAL"
};
const char *testData = "(-1460002, 12507, 1.2, 'textVAL_рус', 'varcharVAL', 'varbinaryVAL', 'русVAL', 'fbVAL')";

int
parseTestChunkRow(
    Chunk *chunk,
    int current_offset,
    struct ParsedChunk *out)
{
    int64_t int_64_val, offset, len;
    int32_t int_32_val;
    double double_val;

    memcpy(&out->int_64, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&out->int_32, chunk->m_ptr + current_offset, 4);
    current_offset += 8;
    memcpy(&out->double_val, chunk->m_ptr + current_offset, 8);
    current_offset += 8;

    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_text.data, chunk->m_ptr + offset, len);
    out->variable_text.len = len;

    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_char.data, chunk->m_ptr + offset, len);
    out->variable_char.len = len;

    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_binary.data, chunk->m_ptr + offset, len);
    out->variable_binary.len = len;

    memcpy(out->fixed_char, chunk->m_ptr + current_offset, 48);
    current_offset += 48;

    memcpy(out->fixed_binary, chunk->m_ptr + current_offset, 8);
    current_offset += 8;

    return current_offset;
}

void null_test(S2Client *client)
{
    int err = 0;

    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS null_test", &err);
    if (err) printf("Error creating table: %s\n", S2Error(client));

    ExecuteDDLQuery(
        client,
        "CREATE TABLE null_test (\
        ibigint BIGINT(20),\
        iint INT,\
        ddouble DOUBLE,\
        vtext TEXT,\
        vvarchar_10 VARCHAR(10),\
        vvarbinary_20 VARBINARY(20),\
        fchar_16 CHAR(16),\
        fbbinary_8 BINARY(8)\
        )",
        &err);
    if (err) printf("Error creating table: %s\n", S2Error(client));

    ExecuteDDLQuery(client, "INSERT INTO null_test VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", &err);
    if (err) printf("Error inserting data: %s\n", S2Error(client));

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

    while (GetNextChunk(q, &dummy_partition, chunk, &EH.callback))
    {
        assert(!err && "GetNextChunk failed in null_test in non parallel mode");

        int current_offset = 0;
        struct ParsedChunk chunkData;
        for (int i = 0; i < chunk->row_count; ++i)
        {
            current_offset = parseTestChunkRow(chunk, current_offset, &chunkData);
            assert(chunkData.int_64 == int64Null);
            assert(chunkData.int_32 == int32Null);
            assert(chunkData.double_val == doubleNull);
            assert(chunkData.variable_text.len == 0);
            assert(chunkData.variable_char.len == 0);
            assert(chunkData.variable_binary.len == 0);
            assert(strlen(chunkData.fixed_char) == 0);
            assert(strlen(chunkData.fixed_binary) == 0);
        }
        ChunkFree(chunk);
    }
    free(chunk);

    // free the queue
    ChunkQueueFree(q);

    ExecuteDDLQuery(client, "DROP TABLE null_test", &err);
    if (err) printf("Error dropping table: %s\n", S2Error(client));
    printf("[SUCCESS] NULL test passed\n");
}

void setup(S2Client *client)
{
    int err = 0;
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS data_type_table", &err);
    if (err) printf("Error dropping table: %s\n", S2Error(client));

    ExecuteDDLQuery(
        client,
        "CREATE TABLE data_type_table (\
        ibigint BIGINT(20),\
        iint INT,\
        ddouble DOUBLE,\
        vtext TEXT,\
        vvarchar_10 VARCHAR(10),\
        vvarbinary_20 VARBINARY(20),\
        fchar_16 CHAR(16),\
        fbbinary_8 BINARY(8)\
        )",
        &err);

    if (err) printf("Error creating table: %s\n", S2Error(client));
    for (int i = 0; i < nTableRows; ++i)
    {
        char query[200] = "INSERT INTO data_type_table VALUES ";
        strcat(query, testData);
        ExecuteDDLQuery(client, query, &err);
        if (err) printf("Error inserting data: %s\n", S2Error(client));
    }
}

void cleanup(S2Client *client)
{
    int err;
    ExecuteDDLQuery(client, "DROP TABLE data_type_table", &err);
    if (err) printf("Error dropping table: %s\n", S2Error(client));
}

void read_test(S2Client *client)
{
    ChunkQueue *q = QueryGetQueue(client, queryMain, chunkSize, queueCapacity);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client))
    {
        printf("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
        return;
    }

    int dummy_partition;
    int err = 0;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    SuperChunkReader *r = CreateReader(chunk, NULL, &EH.callback);
    RowSchema *s;

    while (GetNextChunk(q, &dummy_partition, chunk, &EH.callback))
    {
        assert(err == 0 && "GetNextChunk failed in non parallel mode");
        if (!numReceived)
        {
            s = GetRowSchema(q);
            // PrintRowSchema(s);
        }
        numReceived++;
        // PrintChunk(r, chunk, s);
        struct ParsedChunk chunkData;
        int current_offset = 0;
        for (int i = 0; i < chunk->row_count; ++i)
        {
            current_offset = parseTestChunkRow(chunk, current_offset, &chunkData);
            assert(chunkData.int_64 == INPUT.int_64);
            assert(chunkData.int_32 == INPUT.int_32);
            assert(chunkData.double_val == INPUT.double_val);

            assert(chunkData.variable_text.len == INPUT.variable_text.len);
            assert(chunkData.variable_char.len == INPUT.variable_char.len);
            assert(chunkData.variable_binary.len == INPUT.variable_binary.len);

            assert(!strncmp(chunkData.variable_text.data, INPUT.variable_text.data, INPUT.variable_text.len));
            assert(!strncmp(chunkData.variable_char.data, INPUT.variable_char.data, INPUT.variable_char.len));
            assert(!strncmp(chunkData.variable_binary.data, INPUT.variable_binary.data, INPUT.variable_binary.len));

            assert(!strcmp(chunkData.fixed_char, INPUT.fixed_char));
            assert(!strcmp(chunkData.fixed_binary, INPUT.fixed_binary));
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

    setup(client);
    read_test(client);
    cleanup(client);
    // free the client
    S2ClientFree(client);
    return 0;
}
