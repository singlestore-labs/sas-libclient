#include <iostream>
#include <stdio.h>
#include <string.h>
#include <memory>

#include "s2_connection.hpp"
#include "s2_client.hpp"
#include "hdat/chunk_reader.hpp"
#include "utils.hpp"

#include "libmysql/mysql.h"

Column columns[] =
    {
        {.type = BigInt},
        {.type = BigInt},
        {.type = Double},
        {.type = Double},
        {.type = Variable},
        {.type = Variable}
};

RowSchema schema =
    {
        .ColumnInfo = columns,
        .numColumns = 6,
};

RowSchema* new_schema;

std::unique_ptr<S2Connection> PrepareDB()
{
    std::string stmts[] =
        {
            "DROP TABLE IF EXISTS 6_col_test",
            "CREATE TABLE 6_col_test (i bigint(20), i2 bigint(20), d double, d2 double, t text, t2 text);",
            "INSERT INTO 6_col_test VALUES (1, 2, 3.4, 5.6, 'abc', 'de');",
            "INSERT INTO 6_col_test VALUES (2, 3, 4.5, 6.7, 'a', 'b');",
            "INSERT INTO 6_col_test VALUES (3, 4, 5.6, 7.8, 'xxxxx', 'ccccc');",
        };
    auto conn = S2Connection::Connect("127.0.0.1", 3306, "testdb", "", "p");
    if (!conn)
    {
        printf("no connection!\n");
        return NULL;
    }
    printf("connected!\n");
    for (auto st : stmts)
    {
        conn->Prepare(st.c_str());
    }

    return conn;
}

void
testRun(
    int size,
    S2Connection* conn,
    bool need_to_read)
{
    conn->Prepare("SELECT * FROM 6_col_test");
    new_schema = conn->GetRowSchema();
    Chunk* chunk = nullptr;

    try
    {
        // initialize chunk to fill
        char* ptr = (char*)malloc(size);
        chunk = (Chunk*)malloc(sizeof(Chunk));
        chunk->m_ptr = ptr;
        chunk->m_size = size;
        chunk->row_count = 0;

        auto writer = std::make_unique<SuperChunkWriter>();
        writer->Reset(chunk, new_schema);

        conn->NextChunk(writer, chunk, new_schema);
    }
    catch (std::invalid_argument err)
    {
        std::cout << "Error: " << err.what() << "\n";
        return;
    }

    std::cout << "Chunk size: " << chunk->m_size << std::endl;
    std::cout << "Chunk row_count: " << chunk->row_count << std::endl;

    if (need_to_read)
    {
        bool is_null;
        SuperChunkReader* reader = new SuperChunkReader(chunk);
        for (int row_num = 0; row_num < chunk->row_count; ++row_num)
        {
            std::cout << "Row " << row_num << ":\n";
            for (int col_num = 0; col_num < new_schema->numColumns; ++col_num)
            {
                switch (new_schema->ColumnInfo[col_num].type)
                {
                    case BigInt:
                        int64_t int_val;
                        reader->ReadInteger(&int_val, &is_null);
                        std::cout << int_val << " ";
                        break;
                    case Double:
                        double float_val;
                        reader->ReadFloat(&float_val, &is_null);
                        std::cout << float_val << " ";
                        break;
                    case Variable:
                        const char* buf;
                        uint64_t len;
                        reader->ReadVariable(&buf, &len, &is_null);
                        for (int i = 0; i < len; ++i)
                        {
                            std::cout << buf[i];
                        }
                        std::cout << " ";
                        break;
                    default:
                        throw "unsupported data type";
                }
            }
            std::cout << std::endl;
        }
    }
    super_chunk::utils::ChunkFree(chunk);
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
    for (auto chunk_size = 1; chunk_size < 1 << 8; ++chunk_size)
    {
        testRun(chunk_size, conn.get(), false);
    }
    testRun(1 << 12, conn.get(), true);
    return 0;
}
