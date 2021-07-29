#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "chunk_extern.h"
#include "s2_client_extern.h"
#include "hdat_read_extern.h"

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
    fflush(stdout);
}

int
dummyProcessChunk(
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

    return chunk->row_count;
}

void
PrintChunk(
    SuperChunkReader *reader,
    Chunk *chunk,
    RowSchema *schema)
{
    ResetReader(reader, chunk, schema);
    int64_t int_64_val;
    int32_t int_32_val;
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
                case Fixed:
                    len = col_info.size;
                    ReadFixed(reader, &buf, len, &is_null);
                    for (int i = 0; i < len; ++i)
                    {
                        printf("%c", buf[i]);
                    }
                    fflush(stdout);
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
