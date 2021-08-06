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
#include "test/helpers.h"

int chunkSize = 102400;

void write_test(S2Client *client)
{
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    char *chunk_data = (char *)malloc(chunkSize);
    chunk->m_ptr = chunk_data;
    chunk->m_size = chunkSize;

    RowSchema *schema = GetTableRowSchema(client, superchunkTable, &EH.callback);

    PrintRowSchema(schema);

    SuperChunkWriter *w = CreateWriter(chunk, schema, &EH.callback);
    for (int i = 0; i < 30; ++i)
    {
        WriteInt64(w, i * i);
        WriteInt32(w, i);
        WriteDouble(w, (double)i);
        char buffer[33];
        snprintf(buffer, 33, "Cube %d", i * i * i);
        WriteVariable(w, buffer, strlen(buffer));

        WriteVariable(w, "txt", 3);
        WriteVariable(w, "\x40\x60", 2);

        WriteFixed(w, "юникод", 12, 48);
        WriteFixed(w, "fixed", 5, 9);

        WriteInt64(w, 1935835200000000);
        WriteInt64(w, 31666394987654);
        WriteInt32(w, 2);
        WriteInt64(w, 40271000000);

        WriteRowEnd(w);
    }

    LoadDataWrite(client, chunk, schema, superchunkTable, &EH.callback);
    WriterFree(w);

    free(chunk);
    free(chunk_data);
    printf("[SUCCESS] write test passed!\n");
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

    setup_table(client, 0);
    write_test(client);
    //cleanup_table(client);

    // free the client
    S2ClientFree(client);
    return 0;
}
