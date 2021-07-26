#include <assert.h>
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

int chunkSize = 102400;

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
    printf("[DUMMMY ERROR CALLBACK] Write failed: %d %s\n", error, errorString);
    fflush(stdout);
}

void write_test(S2Client *client)
{
    const char *query = "CREATE TABLE IF NOT EXISTS `test write`(col_int BIGINT, col_double DOUBLE, col_text TEXT)";

    int err = 0;
    ExecuteDDLQuery(client, query, &err);
    assert(!err && "CREATE TABLE failed");

    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    char *chunk_data = (char *)malloc(chunkSize);
    chunk->m_ptr = chunk_data;
    chunk->m_size = chunkSize;

    RowSchema *schema = GetTableRowSchema(client, "test write", &EH.callback);

    for (int i = 0; i < schema->numColumns; ++i)
    {
        printf("%d-th column: type %d, name %s; ", i, schema->ColumnInfo[i].type, schema->ColumnInfo[i].name);
    }
    printf("\n");

    SuperChunkWriter *w = CreateWriter(chunk, schema, &EH.callback);
    for (int i = 0; i < 100; ++i)
    {
        char i_str[3] = "txt";
        WriteInteger(w, (int32_t)i);
        WriteFloat(w, (double)i);
        WriteVariable(w, i_str, 3);
        WriteRowEnd(w);
    }

    LoadDataWrite(client, chunk, schema, "test write", &EH.callback);
    WriterFree(w);

    free(chunk);
    free(chunk_data);
    printf("Write success!\n");
}

void chunk_test()
{
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    char *chunk_data = (char *)malloc(32);
    chunk->m_ptr = chunk_data;
    chunk->m_size = 32;
    chunk->consumed_size = 0;

    Column* cols = (Column *)malloc(3 * sizeof(Column));

    RowSchema schema = {.numColumns = 3, .ColumnInfo = cols};

    SuperChunkWriter *w = CreateWriter(chunk, &schema, &EH.callback);
    int written = 0;
    for (int j = 0; j < 3; ++j)
    {
        for (int i = 0; i < 3; ++i)
        {
            bool res = WriteFloat(w, (double)i);
            written += res;
            //printf("Written: %d, m_size: %d, consumed_size: %d\n", res, chunk->m_size, chunk->consumed_size);
        }
        WriteRowEnd(w);
    }
    assert(written == 4);
}

int
main(
    int argc,
    char *argv[])
{
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
        1,
        -1,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    write_test(client);
    chunk_test();

    // free the client
    S2ClientFree(client);
    return 0;
}
