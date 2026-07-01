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

#include <chrono>
#include <iostream>
#include <fstream>
#include <memory>
#include <vector>
#include <stdio.h>
#include <string.h>
#include <sstream>

#include <unistd.h>

#include "avro.h"
#include "avro/encoding.h"
#include "mysql.h"

#include "s2_connection.hpp"

#include "test/db_creds.h"

const char* FILE_PATH = "/home/pmishchenko-ua/Downloads/CSV/3_cols_1M_rows.csv";

Column true_columns[] =
    {
        {.type = Int64},
        {.type = Double},
        {.type = Variable}
};

RowSchema true_schema =
    {
        .ColumnInfo = true_columns,
        .numColumns = 3,
};

class My3ColData
{
  public:
    std::vector<int64_t> int64_col;
    std::vector<double> double_col;
    std::vector<std::string> variable_col;

    int64_t n_rows = 0;

    void
    Push(
        int64_t x,
        double y,
        std::string& s)
    {
        int64_col.push_back(x);
        double_col.push_back(y);
        variable_col.push_back(s);
        ++n_rows;
    }
};

std::string TABLE = "3_col_test";
int64_t CHUNK_SIZE = 1 << 30;

std::unique_ptr<S2Connection> PrepareDB()
{
    std::string ddl_stmts[] =
        {
            "DROP TABLE IF EXISTS 3_col_test",
            "DROP TABLE IF EXISTS 3_col_test_c",
            "CREATE TABLE 3_col_test (i bigint, d double, t text)",
            "CREATE TABLE 3_col_test_c LIKE 3_col_test",
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
        printf("No connection!\n");
        return NULL;
    }
    printf("Connected!\n");
    for (auto st : ddl_stmts)
    {
        conn->Prepare(st.c_str(), true, false);
    }
    printf("Table created!\n");
    return conn;
}

void
FillChunk(
    Chunk* chunk,
    RowSchema* schema,
    std::unique_ptr<My3ColData>& sourceData)
{
    SuperChunkWriter* w = new SuperChunkWriter(chunk, schema);

    for (int i = 0; i < sourceData->n_rows; ++i)
    {
        w->WriteInt64(&sourceData->int64_col[i]);
        w->WriteDouble(&sourceData->double_col[i], 8);
        w->WriteVariable(sourceData->variable_col[i].c_str(), sourceData->variable_col[i].length());
        w->WriteRowEnd();
    }
}

void
WriteAvroFileC(
    std::unique_ptr<My3ColData>& sourceData,
    S2Connection* conn)
{
    const char TEST_SCHEMA[] =
        "{\"type\":\"record\",\
        \"name\": \"test_3_col\",\
        \"fields\": [\
            {\"name\": \"i\", \"type\": \"long\"},\
            {\"name\": \"d\", \"type\": \"double\"},\
            {\"name\": \"t\", \"type\": \"string\"}]}";
    avro_schema_t test_schema;
    avro_schema_from_json_literal(TEST_SCHEMA, &test_schema);

    avro_file_writer_t fw;
    avro_file_writer_create("/home/pmishchenko-ua/Downloads/CSV/3_col_c.avro", test_schema, &fw);

    avro_value_iface_t* test_class = avro_generic_class_from_schema(test_schema);

    avro_value_t test_val;
    avro_generic_value_new(test_class, &test_val);

    avro_value_t i_value;
    avro_value_t d_value;
    avro_value_t t_value;
    for (int i = 0; i < sourceData->n_rows; ++i)
    {
        avro_value_get_by_name(&test_val, "i", &i_value, NULL);
        avro_value_set_long(&i_value, sourceData->int64_col[i]);

        avro_value_get_by_name(&test_val, "d", &d_value, NULL);
        avro_value_set_double(&d_value, sourceData->double_col[i]);

        avro_value_get_by_name(&test_val, "t", &t_value, NULL);
        avro_value_set_string(&t_value, sourceData->variable_col[i].c_str());

        avro_file_writer_append_value(fw, &test_val);
    }
    avro_file_writer_flush(fw);

    avro_value_decref(&test_val);
    avro_value_iface_decref(test_class);
}

void
LoadDataTestAvro(
    std::unique_ptr<My3ColData>& sourceData,
    S2Connection* conn)
{
    const char TEST_SCHEMA[] =
        "{\"type\":\"record\",\
        \"name\": \"test_3_col\",\
        \"fields\": [\
            {\"name\": \"i\", \"type\": \"long\"},\
            {\"name\": \"d\", \"type\": \"double\"},\
            {\"name\": \"t\", \"type\": \"string\"}]}";
    avro_schema_t test_schema;
    avro_schema_from_json_literal(TEST_SCHEMA, &test_schema);

    Column true_columns[] =
        {
            {.type = Int64,
             .name = const_cast<char*>("i")},
            {.type = Double,
             .name = const_cast<char*>("d")},
            {.type = Variable,
             .name = const_cast<char*>("t")}
        };
    RowSchema true_schema =
        {
            .ColumnInfo = true_columns,
            .numColumns = 3,
        };

    int buf_size = 150100100;

    char* buf = (char*)malloc(buf_size);

    avro_writer_t writer = avro_writer_memory(buf, buf_size);

    for (int i = 0; i < sourceData->n_rows; ++i)
    {
        avro_binary_encoding.write_long(writer, sourceData->int64_col[i]);
        avro_binary_encoding.write_double(writer, sourceData->double_col[i]);
        avro_binary_encoding.write_string(writer, sourceData->variable_col[i].c_str());
    }
    avro_writer_flush(writer);
    int64_t written = avro_writer_tell(writer);
    std::cout << "Written " << written << " bytes" << std::endl;

    // std::ofstream ofs;
    // ofs.open("/home/pmishchenko-ua/Downloads/CSV/3_col_memory.avro", std::ios::binary);
    // ofs.write(buf, written);
    // ofs.close();
    AvroBuffer source =
        {
            .m_ptr = buf,
            .m_size = written,
            .m_current_pos = (uint64_t)0
        };
    conn->WriteAvro(&source, &true_schema, "3_col_test_c", true);

    // avro_value_decref(&test_val);
    // avro_value_iface_decref(test_class);
    free(buf);
}

void TestRun(S2Connection* conn, const char* file_path)
{
    std::ifstream file(file_path);
    auto dataToWrite = std::make_unique<My3ColData>();

    int64_t x;
    double y;
    std::string s;
    char comma;

    std::cout << "Staring to read dataToWrite from file...";
    auto funcStart = std::chrono::system_clock::now();
    while (file >> x >> comma >> y >> comma >> s)
    {
        dataToWrite->Push(x, y, s);
    }
    std::chrono::duration<double> elapsed = std::chrono::system_clock::now() - funcStart;
    std::cout << "Finished in " << ((double)std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count())
              << " milliseconds" << std::endl;

    conn->Prepare("SELECT * FROM 3_col_test", false, false);
    RowSchema* schema = conn->GetRowSchema(true);

    // std::cout << "Staring to work on FillAvroBuffer..." << std::endl;
    // funcStart = std::chrono::system_clock::now();
    // std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    // FillAvroBuffer(out, dataToWrite);
    // elapsed = std::chrono::system_clock::now() - funcStart;
    // std::cout << "Finished in " << ((double)std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count())
    //           << " milliseconds" << std::endl;

    // std::cout << "Staring to work on WriteAvroFileOK..." << std::endl;
    // funcStart = std::chrono::system_clock::now();
    // WriteAvroFileOK(dataToWrite);
    // elapsed = std::chrono::system_clock::now() - funcStart;
    // std::cout << "Finished in " << ((double)std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count())
    //           << " milliseconds" << std::endl;

    // std::cout << "Staring to work on WriteAvroFile..." << std::endl;
    // funcStart = std::chrono::system_clock::now();
    // WriteAvroFile(dataToWrite);
    // elapsed = std::chrono::system_clock::now() - funcStart;
    // std::cout << "Finished in " << ((double)std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count())
    //           << " milliseconds" << std::endl;

    std::cout << "Staring to work on WriteAvroFileC..." << std::endl;
    funcStart = std::chrono::system_clock::now();
    WriteAvroFileC(dataToWrite, conn);
    elapsed = std::chrono::system_clock::now() - funcStart;
    std::cout << "Finished in " << ((double)std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count())
              << " milliseconds" << std::endl;

    std::cout << "Staring to work on LoadDataTestAvro..." << std::endl;
    funcStart = std::chrono::system_clock::now();
    LoadDataTestAvro(dataToWrite, conn);
    elapsed = std::chrono::system_clock::now() - funcStart;
    // conn->ExecuteDDL();
    std::cout << "Finished in " << ((double)std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count())
              << " milliseconds" << std::endl;

    // initialize the chunk to fill
    char* chunk_data = (char*)malloc(CHUNK_SIZE * sizeof(char));
    Chunk* chunk = NewChunk(chunk_data, CHUNK_SIZE, 0, 0);

    std::cout << "Staring to work on FillChunk...";
    funcStart = std::chrono::system_clock::now();
    FillChunk(chunk, schema, dataToWrite);
    elapsed = std::chrono::system_clock::now() - funcStart;
    std::cout << "Finished in " << ((double)std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count())
              << " milliseconds" << std::endl;
    auto reader = std::make_unique<SuperChunkReader>(chunk, schema);
    try
    {
        std::cout << "Staring to work on WriteChunk to DB..." << std::endl;
        funcStart = std::chrono::system_clock::now();
        conn->WriteChunk(reader, chunk, schema, TABLE);
        elapsed = std::chrono::system_clock::now() - funcStart;
        std::cout << "Finished WriteChunk to DB in "
                  << ((double)std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()) << " milliseconds"
                  << std::endl;
    }
    catch (S2ClientError& e)
    {
        std::cerr << e.m_errorCode << ' ' << e.m_errorMessage << '\n';
    }
    utils::ChunkFree(chunk);
    free(chunk);
}

int
main(
    int argc,
    char* argv[])
{
    const char* file_path = getenv("WRITE_TIMING_CSV");
    if (!file_path) {
        file_path = FILE_PATH;
    }
    if (access(file_path, R_OK) != 0) {
        std::cout << "Skipping write_timing_test: data file not found: " << file_path << std::endl;
        return 0;
    }

    auto conn = PrepareDB();
    if (!conn) return 1;

    TestRun(conn.get(), file_path);

    return 0;
}
