#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chunk_extern.h"
#include "s2_client_extern.h"
#include "hdat_read_extern.h"

#define IF_INFO(fn)                                                                                                    \
    if (printInfo) fn
#define PRINT_INFO(args...)                                                                                            \
    if (printInfo)                                                                                                     \
    {                                                                                                                  \
        printf(args);                                                                                                  \
        fflush(stdout);                                                                                                \
    }

#define PRINT_ERROR(args...)                                                                                           \
    {                                                                                                                  \
        fprintf(stderr, "[ERROR] ");                                                                                   \
        fprintf(stderr, args);                                                                                         \
    }

#define chunkBufferSize 1000
#define queryLen 1000
#define maxVariableLen 120
#define nSmallTestRows 14
#define collationCharSize 3

const char *superchunkTable = "superchunk_table";
const char *testData =
    "(-1460002, 12507, 1.23456789012345,\
    'textVAL_рус', 'LOOONGVAL', 'varcharVAL', 'varbinaryVAL', 'русVAL', 'fbVAL',\
    '2021-05-05 12:00:00', '1961-01-01 12:13:14.987654', '2021-05-05', '11:11:11')";

char smallTestData[nSmallTestRows][60] =
    {
        "(1, -22, 3.4, 5.6, 'abc', 'de')",
        "(2, -33, 4.5, 6.7, 'a', 'b')",
        "(3, -44, 5.6, 7.8, '3x', 'c')",
        "(4, -55, 6.7, 8.9, '4x', 'cc')",
        "(5, -66, -7.8, 0.01, 'xxxxx', 'ccc')",
        "(1, -77, -8.9, 0.001, 'xxxxx', 'cccc')",
        "(2, -88, 5.1, 0.001, 'xxxxx', 'ccccc')",
        "(2, -99, 5.11, 0.0001, 'xxxxx', 'cccccc')",
        "(2, 101, 5.111, 9.8, 'xxxxx', 'cccccccc')",
        "(2, 202, 5.1111, 10.8, 'xxxxx', 'cccccccc')",
        "(5, 303, 5.11111, 11.8, 'xxxxx', 'ccccccccc')",
        "(1, 404, 5.111111, 12.8, 'xxxxx', 'cccccccccc')",
        "(4, 505, 5.1111111, 12.8, 'xxxxx', 'ccccccccc')",
        "(NULL, NULL, NULL, NULL, NULL, NULL)"
};

const int smallTestFixedSize = 8 + 8 + 8 + 8 + 16 + 16;

typedef struct variable
{
    char data[maxVariableLen];
    int len;
} variable;

struct ParsedTestChunk
{
    int64_t int_64;
    int32_t int_32;
    double double_val;
    variable variable_text;
    variable variable_long_text;
    variable variable_char;
    variable variable_binary;
    char fixed_char[49];
    char fixed_binary[17];
    int64_t date_time;
    int64_t date_time_6;
    int32_t date;
    int64_t time;
};

typedef enum ReadingMode
{
    FIRST_PASS,
    SECOND_PASS,
    RANDOM_READ,
} ReadingMode;

typedef struct WorkerArgs
{
    int id;
    int db_port;
    bool needRandomRead;
    bool checkAffinity;
    bool checkOrder;
    const char *query;
} WorkerArgs;

typedef struct ReceivedChunk
{
    int chunk_id;
    int partition_id;
    int row_count;
} ReceivedChunk;

typedef struct ReaderThreadArgs
{
    int id;
    int worker_id;

    ReadingMode mode;

    ChunkQueue *queue;
    int n_chunks_read;
    ReceivedChunk *chunks_read;

    int partition_key_i1_counter[nSmallTestRows];
    bool check_partition_order;
} ReaderThreadArgs;

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
    assert(false);
}

void setup_small_test_table(S2Client *client)
{
    int err = 0;
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS small_test", &err);
    if (err) PRINT_ERROR("Error dropping table: %s\n", S2Error(client));

    ExecuteDDLQuery(
        client,
        "CREATE TABLE small_test (\
        i1 BIGINT,\
        i2 BIGINT,\
        d1 DOUBLE,\
        d2 DOUBLE,\
        t1 TEXT,\
        t2 TEXT\
        )",
        &err);

    if (err) PRINT_ERROR("Error creating table: %s\n", S2Error(client));
    for (int i = 0; i < nSmallTestRows; ++i)
    {
        char query[queryLen] = "INSERT INTO small_test VALUES ";
        strcat(query, smallTestData[i]);
        ExecuteDDLQuery(client, query, &err);
        if (err) PRINT_ERROR("Error inserting data: %s\n", S2Error(client));
        assert(!err);
    }
}

void cleanup_small_test_table(S2Client *client)
{
    int err = 0;
    ExecuteDDLQuery(client, "DROP TABLE small_test", &err);
    if (err) PRINT_ERROR("Error dropping table: %s\n", S2Error(client));
}

void
setup_superchunk_table(
    S2Client *client,
    int nTableRows)
{
    int err = 0;
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS superchunk_table", &err);
    if (err) PRINT_ERROR("Error dropping table: %s\n", S2Error(client));

    ExecuteDDLQuery(
        client,
        "CREATE TABLE superchunk_table (\
        ibigint BIGINT(20),\
        iint INT,\
        ddouble DOUBLE,\
        vtext TEXT,\
        vlongtext LONGTEXT,\
        vvarchar_10 VARCHAR(10),\
        vvarbinary_20 VARBINARY(20),\
        fchar_16 CHAR(16),\
        fbbinary_9 BINARY(9),\
        idatetime DATETIME,\
        idatetime_6 DATETIME(6),\
        idate DATE,\
        itime TIME\
        )",
        &err);

    if (err) printf("Error creating table: %s\n", S2Error(client));
    for (int i = 0; i < nTableRows; ++i)
    {
        char query[queryLen] = "INSERT INTO superchunk_table VALUES ";
        strcat(query, testData);
        ExecuteDDLQuery(client, query, &err);
        if (err) PRINT_ERROR("Error inserting data: %s\n", S2Error(client));
        assert(!err);
    }
}

void cleanup_superchunk_table(S2Client *client)
{
    int err = 0;
    ExecuteDDLQuery(client, "DROP TABLE superchunk_table", &err);
    if (err) PRINT_ERROR("Error dropping table: %s\n", S2Error(client));
}

void
mult_table(
    S2Client *client,
    const char *inTable,
    const char *outTable,
    int scaleFactor)
{
    int err;
    int len = strlen("DROP TABLE IF EXISTS ") + strlen(outTable);
    char *query = (char *)malloc(len + 1);
    query[0] = '\0';
    strcpy(query, "DROP TABLE IF EXISTS ");
    strcat(query, outTable);
    ExecuteDDLQuery(client, query, &err);
    if (err) PRINT_ERROR("Error dropping table: %s\n", S2Error(client));

    free(query);

    len = strlen("CREATE TABLE ") + strlen(outTable) + strlen(" AS SELECT * FROM ") + strlen(inTable);
    query = (char *)malloc(len + 1);
    query[0] = '\0';
    strcpy(query, "CREATE TABLE ");
    strcat(query, outTable);
    strcat(query, " AS SELECT * FROM ");
    strcat(query, inTable);
    ExecuteDDLQuery(client, query, &err);
    if (err) PRINT_ERROR("Error creating table: %s\n", S2Error(client));

    free(query);

    len = strlen("INSERT INTO ") + strlen(outTable) + strlen("(SELECT * FROM ") + strlen(outTable) + 1;
    query = (char *)malloc(len + 1);
    strcpy(query, "INSERT INTO ");
    strcat(query, outTable);
    strcat(query, "(SELECT * FROM ");
    strcat(query, outTable);
    strcat(query, ")");

    for (int i = 0; i < scaleFactor; ++i)
    {
        ExecuteDDLQuery(client, query, &err);
        if (err) PRINT_ERROR("Error inserting data: %s\n", S2Error(client));
    }
    free(query);
}

// read data from the chunk constructed by reading the table
// created in setup_superchunk_table
int
parseTestChunkRow(
    Chunk *chunk,
    int current_offset,
    struct ParsedTestChunk *out)
{
    int64_t int_64_val, offset, len;
    int32_t int_32_val;
    double double_val;

    // Int64
    memcpy(&out->int_64, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    // Int32
    memcpy(&out->int_32, chunk->m_ptr + current_offset, 4);
    current_offset += 8;
    // Double
    memcpy(&out->double_val, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    // Variable, TEXT
    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_text.data, chunk->m_ptr + offset, len);
    out->variable_text.len = len;
    // Variable, LONGTEXT
    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_long_text.data, chunk->m_ptr + offset, len);
    // Variable, VARCHAR
    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_char.data, chunk->m_ptr + offset, len);
    out->variable_char.len = len;
    // Variable, VARBINARY
    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_binary.data, chunk->m_ptr + offset, len);
    out->variable_binary.len = len;
    // Fixed, CHAR(16)
    memcpy(out->fixed_char, chunk->m_ptr + current_offset, 16 * collationCharSize);
    current_offset += 16 * collationCharSize;
    // Fixed, BINARY(9)
    memcpy(out->fixed_binary, chunk->m_ptr + current_offset, 16);
    current_offset += 16;
    // DateTime
    memcpy(&out->date_time, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    // DateTime(6)
    memcpy(&out->date_time_6, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    // Date
    memcpy(&out->date, chunk->m_ptr + current_offset, 4);
    current_offset += 8;
    // Time
    memcpy(&out->time, chunk->m_ptr + current_offset, 8);
    current_offset += 8;

    return current_offset;
}

int
RecordChunk(
    Chunk *chunk,
    bool print,
    ReaderThreadArgs *args)
{
    if (args->mode == FIRST_PASS)
    {
        (args->chunks_read)[args->n_chunks_read].chunk_id = chunk->id;
        (args->chunks_read)[args->n_chunks_read].partition_id = chunk->partition_id;
        (args->chunks_read)[args->n_chunks_read].row_count = chunk->row_count;

        (args->n_chunks_read)++;
    }

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
    if (args->check_partition_order)
    {
        int64_t val;
        for (int row_num = 0; row_num < chunk->row_count; ++row_num)
        {
            // copy the value corresponding to `i1` column to val
            memcpy(&val, chunk->m_ptr + row_num * smallTestFixedSize, 8);
            args->partition_key_i1_counter[val]++;
        }
        int64_t int_val, int_prev;
        double double_val, double_prev;
        memcpy(&int_prev, chunk->m_ptr, 8);
        memcpy(&double_prev, chunk->m_ptr + 16, 8);
        for (int row_num = 1; row_num < chunk->row_count; ++row_num)
        {
            memcpy(&int_val, chunk->m_ptr + row_num * smallTestFixedSize, 8);
            memcpy(&double_val, chunk->m_ptr + row_num * smallTestFixedSize + 16, 8);
            assert(int_prev <= int_val);  // TODO: uncomment here and above when PARTITION_ORDER_BY works
            if (int_prev == int_val)
            {
                assert((isnan(double_prev) &&
                        isnan(double_val)) ||
                       (double_prev <= double_val));
            }
            int_prev = int_val;
            double_prev = double_val;
        }
    }

    return chunk->row_count;
}

void
PrintChunk(
    SuperChunkReader *reader,
    Chunk *chunk,
    RowSchema *schema)
{
    ResetReader(reader, chunk, schema);
    int64_t int_64_val, date_time, time;
    int32_t int_32_val, date;
    int64_t len;
    bool is_null;
    double float_val;
    for (int row_num = 0; row_num < chunk->row_count; ++row_num)
    {
        const char *buf;
        printf("Reading row number %d:\n", row_num);
        for (int col = 0; col < schema->numColumns; ++col)
        {
            Column col_info = schema->ColumnInfo[col];
            printf("\tcolumn %s: ", col_info.name);
            switch (col_info.type)
            {
                case Int64:
                    ReadInt64(reader, &int_64_val, &is_null);
                    printf("%ld", int_64_val);
                    break;
                case Int32:
                    ReadInt32(reader, &int_32_val, &is_null);
                    printf("%d", int_32_val);
                    break;
                case Double:
                    ReadFloat(reader, &float_val, &is_null);
                    printf("%f", float_val);
                    break;
                case Variable:
                    ReadVariable(reader, &buf, &len, &is_null);
                    for (int i = 0; i < len; ++i)
                    {
                        printf("%c", buf[i]);
                    }
                    fflush(stdout);
                    break;
                case Fixed:
                    len = col_info.size;
                    ReadFixed(reader, &buf, len, &is_null);
                    for (int i = 0; i < len; ++i)
                    {
                        printf("%c", buf[i]);
                    }
                    fflush(stdout);
                    break;
                case DateTime:
                    ReadInt64(reader, &date_time, &is_null);
                    printf("%ld", date_time);
                    break;
                case Date:
                    ReadInt32(reader, &date, &is_null);
                    printf("%d", date);
                    break;
                case Time:
                    ReadInt64(reader, &time, &is_null);
                    printf("%ld", time);
                    break;
                default:
                    assert(false && "unsupported data type");
                    break;
            }
            printf("\n");
        }
        printf("\n");
    }
}

void PrintRowSchema(RowSchema *s)
{
    printf("RowSchema:\n");
    for (int i = 0; i < s->numColumns; ++i)
    {
        printf(
            "\tidx %d, name %s; type %d, size: %d\n",
            i,
            s->ColumnInfo[i].name,
            s->ColumnInfo[i].type,
            s->ColumnInfo[i].size);
    }
    printf("\n");
}

#endif  // TEST_HELPERS_H
