#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mysql.h"

#include "chunk_extern.h"
#include "db_creds.h"
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
#define rowBufferSize 10000
#define queryLen 20000
#define maxVariableLen 120
#define nSmallTestRows 14

const char *allDataTypesTable = "all_data_types_table";
const char *testData =
    "(-1460002, 12507, 1.23456789012345,\
    'textVAL_рус', 'LOOONGVAL', 'varcharVAL', 'varbinaryVAL', 'русVAL', 'fbVAL',\
    '2021-05-05 12:00:00', '1961-01-01 12:13:14.987654', '2021-05-05', '11:11:11.001234')";

char smallTestData[nSmallTestRows][60] =
    {
        "(1, 3.4, 5.6, 'abc', 'de')",
        "(2, 4.5, 6.7, 'a', 'b')",
        "(3, 5.6, 7.8, '3x', 'c')",
        "(4, 6.7, 8.9, '4x', 'cc')",
        "(5, -7.8, 0.01, 'xxxxx', 'ccc')",
        "(1, -8.9, 0.001, 'xxxxx', 'cccc')",
        "(2, 5.1, 0.001, 'xxxxx', 'ccccc')",
        "(2, 5.11, 0.0001, 'xxxxx', 'cccccc')",
        "(2, 5.111, 9.8, 'xxxxx', 'cccccccc')",
        "(2, 5.1111, 10.8, 'xxxxx', 'cccccccc')",
        "(5, 5.11111, 11.8, 'xxxxx', 'ccccccccc')",
        "(1, 5.111111, 12.8, 'xxxxx', 'cccccccccc')",
        "(4, 5.1111111, 12.8, 'xxxxx', 'ccccccccc')",
        "(NULL, NULL, NULL, NULL, NULL)"
};

const int smallTestFixedSize = 8 + 8 + 8 + 8 + 16 + 16;

typedef struct VariableLen
{
    char data[maxVariableLen];
    int len;
} VariableLen;

struct ParsedTestChunk
{
    int64_t int_64;
    int32_t int_32;
    double double_val;
    VariableLen variable_text;
    VariableLen variable_long_text;
    VariableLen variable_char;
    VariableLen variable_binary;
    char fixed_char[64];
    char fixed_binary[9];
    int64_t date_time;
    int64_t date_time_6;
    int32_t date;
    int64_t time;
};

struct ParsedDateTime
{
    int64_t datetime;
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
    bool need_random_read;
    bool check_affinity;
    bool check_order;
    const char *query;
    ParallelReadType read_type;
    const char *const *partition_order_by_cols;
    int order_by_cols_number;
} WorkerArgs;

typedef struct ReceivedChunk
{
    int chunk_id;
    int partition_id;
    uint64_t row_count;
    uint64_t upto_row_count;
    uint64_t *row_ids_read;

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
    ParallelReadType read_type;
} ReaderThreadArgs;

typedef struct ErrorHandler
{
    S2ErrorCallback callback;
    int errorCode;
    char *errorString;
    bool errorExpected;
} ErrorHandler;

ErrorHandler EH;

void
dummyHandleError(
    S2ErrorCallback *cb,
    int error,
    const char *errorString,
    int severity)
{
    ErrorHandler *h = (ErrorHandler *)cb;
    h->errorCode = error;
    if (h->errorExpected)
        printf("[EXPECTED][DUMMMY CALLBACK] Got error: %d %s\n", error, errorString);
    else
    {
        printf("[ERROR][DUMMMY CALLBACK] Got error: %d %s\n", error, errorString);
        assert(0);
    }
}

void setup_small_test_table(S2Client *client)
{
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS `small'test`", &EH.callback);

    ExecuteDDLQuery(
        client,
        "CREATE TABLE `small'test` (\
        i1 BIGINT,\
        rowId BIGINT AUTO_INCREMENT,\
        d1 DOUBLE,\
        d2 DOUBLE,\
        t1 TEXT,\
        t2 TEXT,\
        SHARD KEY(i1, d1),\
        SORT KEY (rowId)\
        )",
        &EH.callback);

    for (int i = 0; i < nSmallTestRows; ++i)
    {
        char query[queryLen] = "INSERT INTO `small'test` (i1, d1, d2, t1, t2) VALUES ";
        strcat(query, smallTestData[i]);
        ExecuteDDLQuery(client, query, &EH.callback);
    }
}

void setup_multi_pass_table(S2Client *client)
{
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS multi_pass_test", &EH.callback);

    ExecuteDDLQuery(
        client,
        "CREATE TABLE multi_pass_test (\
        i1 BIGINT,\
        rowId BIGINT AUTO_INCREMENT,\
        d1 DOUBLE,\
        d2 DOUBLE,\
        t1 TEXT,\
        t2 TEXT,\
        SHARD KEY(i1),\
        SORT KEY (i1, rowId),\
        KEY(rowId)\
        )",
        &EH.callback);

    char query[queryLen] = "INSERT INTO multi_pass_test (i1, d1, d2, t1, t2) VALUES ";
    strcat(query, smallTestData[0]);

    for (int i = 1; i < nSmallTestRows; ++i)
    {
        strcat(query, ",");
        strcat(query, smallTestData[i]);
    }
    for (int j = 0; j < 64; ++j)
    {
        ExecuteDDLQuery(client, query, &EH.callback);
    }
}

void
setup_all_data_types_table(
    S2Client *client,
    int nTableRows)
{
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS all_data_types_table", &EH.callback);

    ExecuteDDLQuery(
        client,
        "CREATE TABLE all_data_types_table (\
        rowId BIGINT AUTO_INCREMENT,\
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
        itime TIME(6),\
        SORT KEY (rowId)\
        )",
        &EH.callback);

    for (int i = 0; i < nTableRows; ++i)
    {
        char query[queryLen] = "INSERT INTO all_data_types_table VALUES ";
        strcat(query, testData);
        ExecuteDDLQuery(client, query, &EH.callback);
    }
}

void cleanup_small_test_table(S2Client *client)
{
    ExecuteDDLQuery(client, "DROP TABLE `small'test`", &EH.callback);
}

void cleanup_all_data_types_table(S2Client *client)
{
    ExecuteDDLQuery(client, "DROP TABLE all_data_types_table", &EH.callback);
}

void
mult_table(
    S2Client *client,
    const char *inTable,
    const char *outTable,
    int scaleFactor)
{
    MYSQL *mysqlDummy = mysql_init(NULL);
    char *inTableQuoted = (char *)malloc(strlen(inTable) + 2);
    inTableQuoted[0] = '`';
    strcpy(inTableQuoted + 1, inTable);
    inTableQuoted[strlen(inTable) + 1] = '`';

    size_t len = strlen("DROP TABLE IF EXISTS ") + strlen(outTable);
    char *query = (char *)malloc(len + 1);
    query[0] = '\0';
    strcpy(query, "DROP TABLE IF EXISTS ");
    strcat(query, outTable);
    ExecuteDDLQuery(client, query, &EH.callback);

    free(query);

    len = strlen("CREATE TABLE ") + strlen(outTable) + strlen(" AS SELECT * FROM ") + strlen(inTableQuoted);
    query = (char *)malloc(len + 1);
    query[0] = '\0';
    strcpy(query, "CREATE TABLE ");
    strcat(query, outTable);
    strcat(query, " AS SELECT * FROM ");
    strcat(query, inTableQuoted);
    ExecuteDDLQuery(client, query, &EH.callback);

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
        ExecuteDDLQuery(client, query, &EH.callback);
    }
    free(query);
}

int get_db_char_size()
{
    MYSQL *mysql = mysql_init(NULL);
    mysql_options(mysql, MYSQL_OPT_SSL_CA, db_creds.ssl_ca);
    MYSQL_RES *res;
    MYSQL_ROW row;
    char *collation;
    int char_size = 1;
    const char *query = "SELECT @@collation_database";

    if (!mysql_real_connect(
            mysql,
            db_creds.host,
            db_creds.user,
            db_creds.password,
            db_creds.db,
            db_creds.ma_port,
            NULL,
            0))
        return 0;
    if (mysql_query(mysql, query)) return 0;
    res = mysql_store_result(mysql);
    if ((row = mysql_fetch_row(res)))
        collation = row[0];
    else
    {
        mysql_free_result(res);
        return 0;
    }
    if (!strncmp(collation, "utf8mb4", 7))
        char_size = 4;
    else if (!strncmp(collation, "utf8", 4))
        char_size = 3;
    mysql_free_result(res);
    mysql_close(mysql);
    return char_size;
}

Chunk *allocChunk(int chunkSize)
{
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    char *chunk_data = (char *)malloc(chunkSize * sizeof(char));
    chunk->m_ptr = chunk_data;
    chunk->m_size = chunkSize;
    chunk->consumed_size = 0;
    chunk->variable_offset = 0;
    chunk->row_count = 0;
    return chunk;
}

int
parseDateTimeChunkRow(
    Chunk *chunk,
    int current_offset,
    struct ParsedDateTime *out)
{
    // DateTime(6)
    memcpy(&out->datetime, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    // Date
    memcpy(&out->date, chunk->m_ptr + current_offset, 4);
    current_offset += 8;
    // Time(6)
    memcpy(&out->time, chunk->m_ptr + current_offset, 8);
    current_offset += 8;

    return current_offset;
}

// read data from the chunk constructed by reading the table
// created in setup_all_data_types_table
int
parseAllDataTypesChunkRow(
    Chunk *chunk,
    int current_offset,
    struct ParsedTestChunk *out,
    int db_char_size)
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
    memcpy(out->variable_text.data, chunk->m_ptr + chunk->m_size - offset, len);
    out->variable_text.len = len;
    // Variable, LONGTEXT
    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_long_text.data, chunk->m_ptr + chunk->m_size - offset, len);
    // Variable, VARCHAR
    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_char.data, chunk->m_ptr + chunk->m_size - offset, len);
    out->variable_char.len = len;
    // Variable, VARBINARY
    memcpy(&offset, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(&len, chunk->m_ptr + current_offset, 8);
    current_offset += 8;
    memcpy(out->variable_binary.data, chunk->m_ptr + chunk->m_size - offset, len);
    out->variable_binary.len = len;
    // Fixed, CHAR(16)
    memcpy(out->fixed_char, chunk->m_ptr + current_offset, 16 * db_char_size);
    current_offset += 16 * db_char_size;
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
        (args->chunks_read)[args->n_chunks_read].row_ids_read = (uint64_t *)malloc(chunk->row_count * sizeof(uint64_t));
        (args->n_chunks_read)++;
    }
    int64_t val;
    for (uint64_t row_num = 0; row_num < chunk->row_count; ++row_num)
    {
        // copy the value corresponding to `rowId` column to val
        memcpy(&val, chunk->m_ptr + row_num * smallTestFixedSize + 8, 8);
        (args->chunks_read)[args->n_chunks_read - 1].row_ids_read[row_num] = val;
    }

    if (print)
    {
        printf(
            "Got chunk: %p, m_ptr: %p, partition_id: %d, m_size: %d, row_count: %d, consumed_size: %d, vaiable_offest: %d\n",
            chunk,
            chunk->m_ptr,
            chunk->partition_id,
            chunk->m_size,
            chunk->row_count,
            chunk->consumed_size,
            chunk->variable_offset);
    }
    if (args->check_partition_order)
    {
        for (uint64_t row_num = 0; row_num < chunk->row_count; ++row_num)
        {
            // copy the value corresponding to `i1` column to val
            memcpy(&val, chunk->m_ptr + row_num * smallTestFixedSize, 8);
            args->partition_key_i1_counter[val]++;
        }
        int64_t int_val, int_prev;
        double double_val, double_prev;
        memcpy(&int_prev, chunk->m_ptr, 8);
        memcpy(&double_prev, chunk->m_ptr + 16, 8);
        for (uint64_t row_num = 1; row_num < chunk->row_count; ++row_num)
        {
            memcpy(&int_val, chunk->m_ptr + row_num * smallTestFixedSize, 8);
            memcpy(&double_val, chunk->m_ptr + row_num * smallTestFixedSize + 16, 8);
            assert(int_prev <= int_val);
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

// Calculate chunk's starting row within a partition
void
CalculatePartitionRows(
    ReaderThreadArgs *args,
    int threadsPerWorker)
{
    int chunk_count = 0;
    int highest_partition = 0;

    for (int i = 0; i < threadsPerWorker; i++)
    {
        chunk_count += args[i].n_chunks_read;
        for (int j = 0; j < args[i].n_chunks_read; j++)
        {
            if (highest_partition < args[i].chunks_read[j].partition_id)
                highest_partition = args[i].chunks_read[j].partition_id;
        }
    }
    highest_partition++;

    int *partition = (int *)malloc(sizeof(int) * highest_partition);
    ReceivedChunk **chunks = (ReceivedChunk **)malloc(sizeof(ReceivedChunk *) * chunk_count);
    memset(partition, 0, sizeof(int) * highest_partition);

    for (int i = 0; i < threadsPerWorker; i++)
    {
        for (int j = 0; j < args[i].n_chunks_read; j++) (partition[args[i].chunks_read[j].partition_id])++;
    }

    int running = 0;
    for (int i = 0; i < highest_partition; i++)
    {
        int part_chunks = partition[i];
        partition[i] = running;
        running += part_chunks;
    }

    for (int i = 0; i < threadsPerWorker; i++)
    {
        for (int j = 0; j < args[i].n_chunks_read; j++)
        {
            int slot = partition[args[i].chunks_read[j].partition_id] + args[i].chunks_read[j].chunk_id;
            chunks[slot] = &args[i].chunks_read[j];
        }
    }

    uint64_t rows;
    int last_partition = -1;
    for (int i = 0; i < chunk_count; i++)
    {
        if (last_partition != chunks[i]->partition_id)
        {
            rows = 0;
            last_partition = chunks[i]->partition_id;
        }
        chunks[i]->upto_row_count = rows;
        rows += chunks[i]->row_count;
    }

    free(partition);
    free(chunks);
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
    uint64_t len;
    bool is_null;
    double float_val;
    for (uint64_t row_num = 0; row_num < chunk->row_count; ++row_num)
    {
        const char *buf;
        printf("Reading row number %d:\n", row_num);
        for (uint32_t col = 0; col < schema->numColumns; ++col)
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
                    for (uint32_t i = 0; i < len; ++i)
                    {
                        printf("%c", buf[i]);
                    }
                    fflush(stdout);
                    break;
                case Fixed:
                    len = col_info.size;
                    ReadFixed(reader, &buf, len, &is_null);
                    for (uint32_t i = 0; i < len; ++i)
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
    for (uint32_t i = 0; i < s->numColumns; ++i)
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

void AssertRowSchema(RowSchema *s)
{
    assert(s && "GetRowSchema failed");
    assert(s->numColumns && "Invalid numColumns in RowSchema");
    assert(s->ColumnInfo && "Invalid ColumnInfo in RowSchema");
}

#endif  // TEST_HELPERS_H
