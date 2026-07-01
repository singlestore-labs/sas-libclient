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

#include <iostream>
#include <stdio.h>
#include <string.h>
#include <memory>

#include "s2_connection.hpp"
#include "s2_client.hpp"

#include "hdat/chunk_reader.hpp"
#include "utils.hpp"

#include "test/db_creds.h"

#include "mysql.h"

Column true_columns[] =
    {
        {.type = Int64},
        {.type = Int64},
        {.type = Double},
        {.type = Double},
        {.type = Variable},
        {.type = Variable}
};

RowSchema true_schema =
    {
        .ColumnInfo = true_columns,
        .numColumns = 6,
};

std::string IN_TABLE = "6_col_test";
std::string OUT_TABLE = "6_col_dupl";

std::unique_ptr<S2Connection> PrepareDB()
{
    std::string ddl_stmts[] =
        {
            "DROP TABLE IF EXISTS 6_col_test",
            "DROP TABLE IF EXISTS 6_col_dupl",

            "CREATE TABLE 6_col_test (i bigint(20), i2 bigint(20), d double, d2 double, t text, t2 text);",
            "CREATE TABLE 6_col_dupl (i bigint(20), i2 bigint(20), d double, d2 double, t text, t2 text);",
        };

    std::string insert_stmts[] =
        {
            "INSERT INTO 6_col_test VALUES (1, 2, 3.4, 5.6, 'abc', 'de');",
            "INSERT INTO 6_col_test VALUES (2, 3, 4.5, 6.7, 'a', 'b');",
            "INSERT INTO 6_col_test VALUES (3, 4, 5.6, 7.8, 'xxxxx', 'ccccc');",
        };

    auto conn = S2Connection::Connect(
        db_creds.host,
        db_creds.ma_port,
        db_creds.db,
        db_creds.user,
        db_creds.password,
        db_creds.ssl_ca);
    if (!conn)
    {
        printf("no connection!\n");
        return NULL;
    }
    printf("connected!\n");
    for (auto st : ddl_stmts)
    {
        conn->Prepare(st.c_str(), true, false);
    }

    for (int i = 0; i < 100; ++i)
    {
        for (auto st : insert_stmts)
        {
            conn->Prepare(st.c_str(), true, false);
        }
    }

    return conn;
}

void CleanDB(S2Connection* conn)
{
    std::string ddl_stmts[] =
        {
            "DROP TABLE IF EXISTS 6_col_test",
            "DROP TABLE IF EXISTS 6_col_dupl",
        };

    for (auto st : ddl_stmts)
    {
        conn->ExecuteDDL(st.c_str());
    }
}

void
testRun(
    int size,
    S2Connection* conn)
{
    auto aggs = conn->GetAggregators();
    std::cout << "Aggregators:\n";
    for (auto agg : aggs)
    {
        std::cout << agg.host << ":" << agg.port << "; " << agg.externalHost << ":" << agg.externalPort << std::endl;
    }

    conn->Prepare((std::string("SELECT * FROM ") + IN_TABLE).c_str(), true, true);
    RowSchema* schema = conn->GetRowSchema(false);
    Chunk* chunk = nullptr;

    try
    {
        // initialize chunk to fill
        char* ptr = (char*)malloc(size);
        chunk = NewChunk(ptr, size, 0, 0);

        auto writer = std::make_unique<SuperChunkWriter>();
        writer->Reset(chunk, schema);

        conn->NextChunk(writer, chunk, schema);
    }
    catch (std::invalid_argument ex)
    {
        std::cout << "Error: " << ex.what() << "\n";
        return;
    }

    std::cout << "Chunk size: " << chunk->m_size << std::endl;
    std::cout << "Chunk row_count: " << chunk->row_count << std::endl;

    auto reader = std::make_unique<SuperChunkReader>(chunk, schema);

    for (int i = 0; i < 10; ++i)
    {
        try
        {
            conn->WriteChunk(reader, chunk, schema, OUT_TABLE);
            std::cout << "Written!\n";
        }
        catch (S2ClientError& e)
        {
            std::cerr << e.m_errorCode << ' ' << e.m_errorMessage << '\n';
        }
    }
    utils::ChunkFree(chunk);
    free(chunk);
}

int
main(
    int argc,
    char* argv[])
{
    auto conn = PrepareDB();
    if (!conn)
    {
        return 1;
    }

    testRun(1 << 24, conn.get());

    CleanDB(conn.get());

    return 0;
}
