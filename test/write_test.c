#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <stdatomic.h>
#include <unistd.h>

#include "s2_client_extern.h"
#include "chunk_extern.h"
#include "hdat_write_extern.h"
#include "avro_write_extern.h"

#include "avro.h"

#include "test/db_creds.h"
#include "test/helpers.h"

int chunkSize = 1 << 30;
int printInfo = 1;
uint64_t nRowsToWrite = 30;
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
        40271001234,
};

int
is_const_buffer(
    char *buff,
    int size,
    char exp_char)
{
    while (size--)
        if (*buff++ != exp_char) return 0;
    return 1;
}

void read_and_check(S2Client *client)
{
    const char *query = "SELECT * FROM all_data_types_table WHERE rowId > 0 ORDER BY rowId";

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
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int db_char_size = get_db_char_size();

    // This assumes all the rows are consumed by one chunk
    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        struct ParsedTestChunk chunkData;
        int current_offset = 0;
        assert(chunk->row_count == nRowsToWrite - 1);  // 2 rows are excluded by the query
        for (uint64_t i = 1; i <= chunk->row_count; ++i)
        {
            current_offset = parseAllDataTypesChunkRow(chunk, current_offset, &chunkData, db_char_size);

            assert(chunkData.int_64 == (int64_t)(i * i));
            assert(chunkData.int_32 == (int32_t)i);

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
    printf("[SUCCESS] validity checked. Write test passed!\n");
}

void write_chunk_test(S2Client *client)
{
    Chunk *chunk = allocChunk(chunkSize);

    RowSchema *schema = GetTableRowSchema(client, allDataTypesTable, &EH.callback);

    IF_INFO(PrintRowSchema(schema));
    int db_char_size = get_db_char_size();

    SuperChunkWriter *w = CreateWriter(chunk, schema, &EH.callback);
    for (uint64_t i = 0; i < nRowsToWrite; ++i)
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

        WriteFixed(w, "юникод", 12, 16 * db_char_size, false);
        WriteFixed(w, "fixed", 5, 9, true);

        WriteInt64(w, TEST_DATA.date_time);
        WriteInt64(w, TEST_DATA.date_time_6);
        WriteInt32(w, TEST_DATA.date);
        WriteInt64(w, TEST_DATA.time);

        WriteRowEnd(w);
    }
    {
        WriteInt64(w, 0);
        WriteInt32(w, int32Null);
        WriteDouble(w, doubleNull);

        WriteVariable(w, "", 0);
        WriteVariable(w, "", 0);
        WriteVariable(w, "", 0);
        WriteVariable(w, "", 0);

        WriteFixed(w, "", 0, 16 * db_char_size, false);
        WriteFixed(w, "", 0, 9, true);

        WriteInt64(w, int64Null);
        WriteInt64(w, int64Null);
        WriteInt32(w, int32Null);
        WriteInt64(w, int64Null);
        WriteRowEnd(w);
    }

    LoadDataWrite(client, chunk, schema, allDataTypesTable, &EH.callback);
    WriterFree(w);
    RowSchemaFree(schema);
    ChunkFree(chunk);
    free(chunk);
    printf("[SUCCESS] data written!\n");
}

void write_avro_test(S2Client *client)
{
    int db_char_size = get_db_char_size();
    RowSchema *schema = GetTableRowSchema(client, allDataTypesTable, &EH.callback);
    IF_INFO(PrintRowSchema(schema));

    int buf_size = 1 << 30;
    char *buf = (char *)malloc(buf_size);

    avro_writer_t w = avro_writer_memory(buf, buf_size);

    for (uint64_t i = 0; i < nRowsToWrite; ++i)
    {
        WriteInt64Avro(w, i * i);
        WriteInt32Avro(w, i);
        if (i % 3 == 0)
        {
            WriteDoubleAvro(w, doubleNull);
        }
        if (i % 3 == 1)
        {
            WriteDoubleAvro(w, val_64);
        }
        if (i % 3 == 2)
        {
            WriteDoubleAvro(w, (double)i);
        }

        char buffer[33];
        snprintf(buffer, 33, "Cube %d", i * i * i);
        WriteBytesAvro(w, buffer, strlen(buffer));

        // these values are copied from TEST_DATA
        WriteBytesAvro(w, "t\nt", 3);
        WriteBytesAvro(w, "lon\ttxt", 7);
        WriteBytesAvro(w, "\x40\x60", 2);

        WriteBytesAvro(w, "юникод", 12);
        WriteBytesAvro(w, "fixed", 5);

        WriteInt64Avro(w, TEST_DATA.date_time);
        WriteInt64Avro(w, TEST_DATA.date_time_6);
        WriteInt32Avro(w, TEST_DATA.date);
        WriteInt64Avro(w, TEST_DATA.time);
    }
    {
        WriteInt64Avro(w, 0);
        WriteInt32Avro(w, int32Null);
        WriteDoubleAvro(w, doubleNull);

        WriteBytesAvro(w, "", 0);
        WriteBytesAvro(w, "", 0);
        WriteBytesAvro(w, "", 0);
        WriteBytesAvro(w, "", 0);

        WriteBytesAvro(w, "", 0);
        WriteBytesAvro(w, "", 0);

        WriteInt64Avro(w, int64Null);
        WriteInt64Avro(w, int64Null);
        WriteInt32Avro(w, int32Null);
        WriteInt64Avro(w, int64Null);
    }
    avro_writer_flush(w);
    int written = avro_writer_tell(w);
    avro_writer_free(w);

    LoadDataAvro(client, buf, written, schema, allDataTypesTable, &EH.callback);
    RowSchemaFree(schema);

    printf("[SUCCESS] AVRO data written!\n");
}

void boundary_test(S2Client *client)
{
    int size = 8 + 8 + 8 + 3;
    Chunk *chunk = allocChunk(size);
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS boundary_table", &EH.callback);
    ExecuteDDLQuery(client, "CREATE TABLE IF NOT EXISTS boundary_table (id BIGINT, var BLOB)", &EH.callback);
    RowSchema *schema = GetTableRowSchema(client, "boundary_table", &EH.callback);
    SuperChunkWriter *w = CreateWriter(chunk, schema, &EH.callback);
    WriteInt64(w, 12345678);
    WriteVariable(w, "\x44\x66\x88", 3);
    WriteRowEnd(w);
    LoadDataWrite(client, chunk, schema, "boundary_table", &EH.callback);
    WriterFree(w);
    ChunkFree(chunk);
    free(chunk);

    const char *query = "SELECT * FROM boundary_table";

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
    chunk = (Chunk *)malloc(sizeof(Chunk));
    SuperChunkReader *r = CreateReader(chunk, schema, &EH.callback);

    // This assumes all the rows are consumed by one chunk
    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        int64_t int_val;
        bool is_null;
        ReadInt64(r, &int_val, &is_null);
        assert(!is_null);
        assert(int_val == 12345678);
        const char *binary_val;
        int64_t len;
        ReadVariable(r, &binary_val, &len, &is_null);
        assert(!is_null);
        assert(len == 3);
        assert(binary_val[0] == '\x44');
        assert(binary_val[1] == '\x66');
        assert(binary_val[2] == '\x88');
    }
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS boundary_table", &EH.callback);
    ReaderFree(r);
    RowSchemaFree(schema);
    ChunkFree(chunk);
    free(chunk);
    printf("[SUCCESS] Boundary size chunk test passed!\n");
}

void max_allowed_packet_test(S2Client *client)
{
    int blobLength = 12000;
    int size = 8 + 8 + 8 + blobLength;
    Chunk *chunk = allocChunk(size);
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS max_allowed_packet_table", &EH.callback);
    ExecuteDDLQuery(client, "CREATE TABLE IF NOT EXISTS max_allowed_packet_table (id BIGINT, var BLOB)", &EH.callback);
    RowSchema *schema = GetTableRowSchema(client, "max_allowed_packet_table", &EH.callback);
    SuperChunkWriter *w = CreateWriter(chunk, schema, &EH.callback);
    WriteInt64(w, 12345678);
    char buff[blobLength];
    memset(buff, 'a', blobLength / 2);
    memset(buff + blobLength / 2, 'b', blobLength / 2);
    WriteVariable(w, buff, blobLength);

    WriteRowEnd(w);
    LoadDataWrite(client, chunk, schema, "max_allowed_packet_table", &EH.callback);
    WriterFree(w);
    ChunkFree(chunk);
    free(chunk);

    ExecuteDDLQuery(client, "SET GLOBAL MAX_ALLOWED_PACKET = 1024", &EH.callback);

    S2Client *read_client = S2ClientInit(
        db_creds.host,
        db_creds.ma_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        db_creds.ssl_ca,
        1,
        -1,
        &EH.callback);
    assert(read_client != NULL && "S2Client is NULL");

    const char *query = "SELECT * FROM max_allowed_packet_table";

    ChunkQueue *q = QueryGetQueue(
        read_client,
        query,
        chunkSize,
        queueCapacity,
        &EH.callback);

    assert(q != NULL && "ChunkQueue is NULL");
    if (S2Errno(read_client)) PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
    assert(!S2Errno(read_client));

    int dummy_partition;
    chunk = (Chunk *)malloc(sizeof(Chunk));
    SuperChunkReader *r = CreateReader(chunk, schema, &EH.callback);

    EH.errorExpected = true;
    GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback);
    EH.errorExpected = false;

    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS max_allowed_packet_table", &EH.callback);
    ReaderFree(r);
    RowSchemaFree(schema);
    ExecuteDDLQuery(client, "SET GLOBAL MAX_ALLOWED_PACKET = 104857600", &EH.callback);
    printf("[SUCCESS] Max allowed packet test passed!\n");
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
        1,
        -1,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    struct timeval start, end;
    long seconds, micros;

    setup_all_data_types_table(client, 0);
    clock_t tic = clock();
    gettimeofday(&start, NULL);
    write_chunk_test(client);
    gettimeofday(&end, NULL);
    clock_t toc = clock();
    seconds = (end.tv_sec - start.tv_sec);
    micros = ((seconds * 1000000) + end.tv_usec) - (start.tv_usec);
    PRINT_INFO("write_chunk_test active: %f seconds\n", (double)(toc - tic) / CLOCKS_PER_SEC);
    PRINT_INFO("write_chunk_test elapsed: %d.%d seconds\n", seconds, micros);

    read_and_check(client);
    cleanup_all_data_types_table(client);

    setup_all_data_types_table(client, 0);

    tic = clock();
    gettimeofday(&start, NULL);
    write_avro_test(client);
    gettimeofday(&end, NULL);
    toc = clock();
    seconds = (end.tv_sec - start.tv_sec);
    micros = ((seconds * 1000000) + end.tv_usec) - (start.tv_usec);
    PRINT_INFO("write_avro_test active: %f seconds\n", (double)(toc - tic) / CLOCKS_PER_SEC);
    PRINT_INFO("write_avro_test elapsed: %d.%d seconds\n", seconds, micros);

    read_and_check(client);
    cleanup_all_data_types_table(client);

    boundary_test(client);
    max_allowed_packet_test(client);

    // free the client
    S2ClientFree(client);
    return 0;
}
