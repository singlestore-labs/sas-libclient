/*
 * Copyright 2021-2026 SingleStore, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "chunk_extern.h"
#include "s2_client_extern.h"
#include "hdat_read_extern.h"

#include "test/db_creds.h"
#include "test/helpers.h"

#define chunkSize 15200
int queueCapacity = 32;

bool printInfo = 0;

void explain_test(S2Client *client)
{
    const char *query = "EXPLAIN SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = 'information_schema'";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        10000,
        1,
        false,
        &EH.callback);

    if (S2Errno(client)) PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
    assert(q != NULL && "ChunkQueue is NULL");

    int dummy_partition;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    RowSchema *s = GetRowSchema(q);
    AssertRowSchema(s);
    IF_INFO(PrintRowSchema(s));
    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        numReceived++;

        int current_offset = 0;
        for (uint64_t i = 0; i < chunk->row_count; ++i)
        {
            int64_t offset, len;
            memcpy(&offset, chunk->m_ptr + current_offset, 8);
            memcpy(&len, chunk->m_ptr + current_offset + 8, 8);
            current_offset += 16;
            if (printInfo)
            {
                printf("Got row #%d of len %d: ", i, len);
                for (int j = 0; j < len; j++)
                {
                    printf("%c", (chunk->m_ptr + chunk->m_size - offset + j)[0]);
                }
                printf("\n");
                fflush(stdout);
            }
        }
        ChunkFree(chunk);
    }
    free(chunk);
    ChunkQueueFree(q);
    printf("explain_test OK\n");
}

void explain_and_row_schema(S2Client *client)
{
    ChunkQueue *queue = NULL;
    RowSchema *rowSchema = NULL;

    ExecuteDDLQuery(client, "create table if not exists t1 (id int, c1 int)", &EH.callback);
    ExecuteDDLQuery(client, "create table if not exists t2 (id int, c2 int)", &EH.callback);
    ExecuteDDLQuery(client, "drop view v1", &EH.callback);
    ExecuteDDLQuery(client, "create view v1 as select t1.id, c1, c2 from t1 join t2 on t1.id = t2.id", &EH.callback);

    queue = QueryGetQueue(client, "explain create result table r as select * from v1",
                          4096, 1, false, &EH.callback);
    printf("QueryGetQueue returned %p\n", queue);

    if (queue) {
        ChunkQueueFree(queue);
        queue = NULL;
    }

    rowSchema = GetTableRowSchema(client, "t1", &EH.callback);
    printf("GetTableRowSchema returned %p\n", rowSchema);

    if (rowSchema)
        RowSchemaFree(rowSchema);
}

void infoschema_test(S2Client *client)
{
    const char *query = "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = 'information_schema'";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        10000,
        1,
        true,
        &EH.callback);

    if (S2Errno(client)) PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
    assert(q != NULL && "ChunkQueue is NULL");

    int dummy_partition;
    Chunk *chunk = (Chunk *)malloc(sizeof(Chunk));
    int numReceived = 0;

    RowSchema *s = GetRowSchema(q);
    AssertRowSchema(s);
    PrintRowSchema(s);
    IF_INFO(PrintRowSchema(s));
    while (GetNextChunk(q, 0, &dummy_partition, chunk, &EH.callback))
    {
        numReceived++;

        int current_offset = 0;
        for (uint64_t i = 0; i < chunk->row_count; ++i)
        {
            int64_t offset, len;
            memcpy(&offset, chunk->m_ptr + current_offset, 8);
            memcpy(&len, chunk->m_ptr + current_offset + 8, 8);
            current_offset += 16;
            if (printInfo)
            {
                printf("Got table #%d of len %d: ", i, len);
                for (int j = 0; j < len; j++)
                {
                    printf("%c", (chunk->m_ptr + chunk->m_size - offset + j)[0]);
                }
                printf("\n");
                fflush(stdout);
            }
        }

        ChunkFree(chunk);
    }
    free(chunk);
    ChunkQueueFree(q);
    printf("infoschema_test OK\n");
}

void long_column_schema_test(S2Client *client)
{
    ExecuteDDLQuery(client, "DROP TABLE IF EXISTS sav128", &EH.callback);

    char query_create[2048] = {'\0'};
    const char* long_name = "££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££££abc";
    strcat(query_create, "CREATE TABLE sav128 (`");
    strcat(query_create, long_name);
    strcat(query_create, "` DOUBLE)");
    ExecuteDDLQuery(client, query_create, &EH.callback);

    const char *query = "SELECT * FROM sav128";

    ChunkQueue *q = QueryGetQueue(
        client,
        query,
        10000,
        1,
        true,
        &EH.callback);

    if (S2Errno(client)) PRINT_ERROR("S2 Error in worker: %d %s\n", S2Errno(client), S2Error(client));
    assert(q != NULL && "ChunkQueue is NULL");

    RowSchema *queue_schema = GetRowSchema(q);
    AssertRowSchema(queue_schema);
    IF_INFO(PrintRowSchema(queue_schema));
    assert(strlen(queue_schema->ColumnInfo[0].name) == 256);
    assert(queue_schema->ColumnInfo[0].type == Double);
    assert(queue_schema->ColumnInfo[0].size == 8);

    RowSchema *table_schema = GetTableRowSchema(client, "sav128", &EH.callback);
    AssertRowSchema(table_schema);
    IF_INFO(PrintRowSchema(table_schema));
    assert(!strcmp(table_schema->ColumnInfo[0].name, long_name));
    assert(table_schema->ColumnInfo[0].type == Double);
    assert(table_schema->ColumnInfo[0].size == 8);

    printf("long_column_name_test OK\n");
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

    const char *version = S2GetClientVersion();
    printf("libs2client version: %s\n", version);

    // init the client
    S2Client *client = S2ClientInit(
        db_creds.host,
        db_creds.ma_port,
        "testdb",
        db_creds.user,
        db_creds.password,
        db_creds.ssl_ca,
        1,
        -1,
        &EH.callback);
    assert(client != NULL && "S2Client is NULL");

    // infoschema_test(client);
    // long_column_schema_test(client);

    // explain_test(client);
    explain_and_row_schema(client);

    S2ClientFree(client);

    printf("[SUCCESS] information_schema test passed!\n");

    return 0;
}
