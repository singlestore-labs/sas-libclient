#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>

#include "s2_client_extern.h"
#include "chunk_extern.h"
#include "hdat_write_extern.h"

#include "test/db_creds.h"
#include "test/helpers.h"

int chunkSize = 1024000;
int printInfo = 1;
int nRowsToWrite = 30;
int queueCapacity = 10;

double val_64 = -1.123456789012345e-300;  // 15-digits after comma

const struct ParsedTestChunk TEST_DATA =
    {
        0,
        0,
        0,
        {"Cube",
         4},
        {"t\nt",
         3},
        {"lon\ttxt",
         7},
        {"\x40\x60",
         2},
        "юникод",
        "fixed",
        1935835200000000,
        31666394987654,
        2,
        40271000000,
};

int is_const_buffer(char *buff, int size, char exp_char)
{
    while(size--) if(*buff++ != exp_char) return 0;
    return 1;
}

void read_and_check(S2Client *client)
{
    const char *query = "SELECT * FROM superchunk_table";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        chunkSize,
        queueCapacity,
        &EH.callback);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(client)) PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
    assert(!S2Errno(client));

    int dummy_partition;
    int err = 0;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));

    // This assumes all the rows are consumed by one chunk
    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        struct ParsedTestChunk chunkData;
        int current_offset = 0;
        assert(chunk->row_count == nRowsToWrite + 1);
        for (int i = 0; i < chunk->row_count; ++i)
        {
            current_offset = parseTestChunkRow(chunk, current_offset, &chunkData);
            if (i >= nRowsToWrite)
            {
                assert(chunkData.int_64 == int64Null);
                assert(chunkData.int_32 == int32Null);
                assert(isnan(chunkData.double_val));

                assert(!chunkData.variable_text.len);
                assert(!chunkData.variable_char.len);
                assert(!chunkData.variable_binary.len);

                assert(is_const_buffer(chunkData.fixed_char, 16 * get_db_char_size(), '\0'));
                assert(is_const_buffer(chunkData.fixed_binary, 9, '\0'));

                assert(chunkData.date_time == int64Null);
                assert(chunkData.date_time_6 == int64Null);
                assert(chunkData.date == int32Null);
                assert(chunkData.time == int64Null);
                break;
            }

            assert(chunkData.int_64 == i * i);
            assert(chunkData.int_32 == i);

            if (i % 3 == 0)
            {
                assert(isnan(chunkData.double_val));
            }
            if (i % 3 == 1)
            {
                assert(chunkData.double_val == val_64);
            }
            if (i % 3 == 2)
            {
                assert(chunkData.double_val == i);
            }

            assert(chunkData.variable_text.len > TEST_DATA.variable_text.len);  // Here we added cube of i
            assert(chunkData.variable_char.len == TEST_DATA.variable_char.len);
            assert(chunkData.variable_binary.len == TEST_DATA.variable_binary.len);

            assert(!strncmp(chunkData.variable_text.data, TEST_DATA.variable_text.data, TEST_DATA.variable_text.len));
            assert(!strncmp(chunkData.variable_char.data, TEST_DATA.variable_char.data, TEST_DATA.variable_char.len));
            assert(!strncmp(
                chunkData.variable_binary.data,
                TEST_DATA.variable_binary.data,
                TEST_DATA.variable_binary.len));

            assert(!strncmp(chunkData.fixed_char, TEST_DATA.fixed_char, strlen(TEST_DATA.fixed_char)));
            assert(!strcmp(chunkData.fixed_binary, TEST_DATA.fixed_binary));

            assert(chunkData.date_time == TEST_DATA.date_time);
            assert(chunkData.date_time_6 == TEST_DATA.date_time_6);
            assert(chunkData.date == TEST_DATA.date);
            assert(chunkData.time == TEST_DATA.time);
        }

        ChunkFree(chunk);
    }
    free(chunk);
    ChunkQueueFree(q);
    printf("[SUCCESS] validity checked. Test passed!\n");
}

void write_test(S2Client *client)
{
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    char *chunk_data = (char *)malloc(chunkSize * sizeof(char));
    chunk->m_ptr = chunk_data;
    chunk->m_size = chunkSize;
    chunk->consumed_size = 0;
    chunk->row_count = 0;

    RowSchema *schema = GetTableRowSchema(client, superchunkTable, &EH.callback);

    IF_INFO(PrintRowSchema(schema));

    SuperChunkWriter *w = CreateWriter(chunk, schema, &EH.callback);
    for (int i = 0; i < nRowsToWrite; ++i)
    {
        WriteInt64(w, i * i);
        WriteInt32(w, i);
        if (i % 3 == 0)
        {
            WriteDouble(w, doubleNull);
        }
        if (i % 3 == 1)
        {
            WriteDouble(w, val_64);
        }
        if (i % 3 == 2)
        {
            WriteDouble(w, (double)i);
        }

        char buffer[33];
        snprintf(buffer, 33, "Cube %d", i * i * i);
        WriteVariable(w, buffer, strlen(buffer));

        // these values are copied from TEST_DATA
        WriteVariable(w, "t\nt", 3);
        WriteVariable(w, "lon\ttxt", 7);
        WriteVariable(w, "\x40\x60", 2);

        WriteFixed(w, "юникод", 12, 16 * get_db_char_size(), false);
        WriteFixed(w, "fixed", 5, 9, true);

        WriteInt64(w, TEST_DATA.date_time);
        WriteInt64(w, TEST_DATA.date_time_6);
        WriteInt32(w, TEST_DATA.date);
        WriteInt64(w, TEST_DATA.time);

        WriteRowEnd(w);
    }
    {
        WriteInt64(w, int64Null);
        WriteInt32(w, int32Null);
        WriteDouble(w, doubleNull);

        WriteVariable(w, "", 0);
        WriteVariable(w, "", 0);
        WriteVariable(w, "", 0);
        WriteVariable(w, "", 0);

        WriteFixed(w, "", 0, 16 * get_db_char_size(), false);
        WriteFixed(w, "", 0, 9, true);

        WriteInt64(w, int64Null);
        WriteInt64(w, int64Null);
        WriteInt32(w, int32Null);
        WriteInt64(w, int64Null);
        WriteRowEnd(w);
    }

    LoadDataWrite(client, chunk, schema, superchunkTable, &EH.callback);
    WriterFree(w);
    RowSchemaFree(schema);

    free(chunk);
    free(chunk_data);
    printf("[SUCCESS] data written!\n");
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
        1,
        -1,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    setup_superchunk_table(client, 0);
    write_test(client);
    read_and_check(client);
    cleanup_superchunk_table(client);

    // free the client
    S2ClientFree(client);
    return 0;
}
