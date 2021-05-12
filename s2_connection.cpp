#include "memory"

#include "s2_connection.hpp"

#include "hdat/chunk_writer.hpp"

using MySQLResultPtr = std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)>;

std::unique_ptr<S2Connection>
S2Connection::Connect(
    const char* host,
    uint32_t port,
    const char* db,
    const char* user,
    const char* password)
{
    // allocate a S2Connection object
    std::unique_ptr<S2Connection> s2Connection(new S2Connection(host, port, db, user, password));

    // create a mysql client connection
    s2Connection->m_conn = mysql_init(nullptr);
    if (!s2Connection->m_conn)
    {
        throw S2ClientError(S2C_ERROR_UNKNOWN_FAILURE, "Failed to create a connection using MySQL C client");
    }

    if ((!user) || (user[0] == '\0'))
    {
        user = "root";
    }

    if (!mysql_real_connect(s2Connection->m_conn, host, user, password, db, port, nullptr, 0))
    {
        throw S2ClientError(mysql_errno(s2Connection->m_conn), mysql_error(s2Connection->m_conn));
    }

    s2Connection->m_stmt = mysql_stmt_init(s2Connection->m_conn);
    if (!s2Connection->m_stmt)
    {
        throw S2ClientError(mysql_errno(s2Connection->m_conn), mysql_error(s2Connection->m_conn));
    }

    return s2Connection;
}

int S2Connection::GetPartitionsNumber()
{
    if (mysql_query(m_conn, "SHOW PARTITIONS"))
    {
        throw S2ClientError(mysql_errno(m_conn), mysql_error(m_conn));
    }

    MySQLResultPtr res(mysql_store_result(m_conn), &mysql_free_result);
    if (res == nullptr)
    {
        throw S2ClientError(mysql_errno(m_conn), mysql_error(m_conn));
    }

    int numPartitions = mysql_num_rows(res.get());
    return numPartitions;
}

void S2Connection::ExecuteDDL(std::string query)
{
    if (mysql_query(m_conn, query.c_str()))
    {
        throw S2ClientError(mysql_errno(this->m_conn), mysql_error(this->m_conn));
    }
}

void S2Connection::Prepare(
    const char* query)
{
    if (mysql_stmt_prepare(m_stmt, query, strlen(query)))
    {
        throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
    }

    if (mysql_stmt_execute(m_stmt))
    {
        throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
    }

    // if we have the result set then prefetch the first row
    if (mysql_stmt_field_count(m_stmt))
    {
        Advance();
    }
}

void S2Connection::freeResult()
{
    // delete actual fields of the result
    for (int i = 0; i < last_columns_num; i++)
    {
        delete[] last_fetched_row[i];
        last_fetched_row[i] = nullptr;
    }
    delete[] last_fetched_row;
    last_fetched_row = nullptr;

    // delete lengths array
    delete[] last_fetched_lengths;
    last_fetched_lengths = nullptr;

    // reset columns number
    last_columns_num = 0;
}

bool S2Connection::Advance()
{
    // free the previous result set
    freeResult();

    // find the columns number
    last_columns_num = mysql_stmt_field_count(m_stmt);

    // find the length of each field
    MySQLResultPtr metadata(mysql_stmt_result_metadata(m_stmt), &mysql_free_result);
    if (metadata == nullptr)
    {
        freeResult();
        throw S2ClientError(S2C_ERROR_UNKNOWN_FAILURE, "Failed to get result set metadata");
    }

    MYSQL_FIELD* fields = mysql_fetch_fields(metadata.get());
    if (fields == nullptr)
    {
        freeResult();
        throw S2ClientError(S2C_ERROR_UNKNOWN_FAILURE, "Failed to get result set metadata fields");
    }

    last_fetched_lengths = new unsigned long[last_columns_num];
    last_fetched_row = new char*[last_columns_num];
    memset(last_fetched_row, 0, sizeof(char*) * last_columns_num);

    std::unique_ptr<MYSQL_BIND[]> bind = std::make_unique<MYSQL_BIND[]>(last_columns_num);
    memset(bind.get(), 0, sizeof(MYSQL_BIND) * last_columns_num);

    for (int i = 0; i < last_columns_num; i++)
    {
        // according to mysql C API docs we are able to pass zero length buffer in order to determine the actual
        // field lengths but it doesn't work, so we are passing 8 bytes buffer (it is the minimal buffer length
        // which is enough for all numeric data types)
        last_fetched_row[i] = new char[8];
        bind[i].buffer = last_fetched_row[i];
        bind[i].buffer_length = 8;
        bind[i].length = &last_fetched_lengths[i];
        bind[i].buffer_type = fields[i].type;
    }

    if (mysql_stmt_bind_result(m_stmt, bind.get()))
    {
        freeResult();
        throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
    }

    int status = mysql_stmt_fetch(m_stmt);
    if (status == MYSQL_NO_DATA)
    {
        freeResult();
        return false;
    }
    else if (status != MYSQL_DATA_TRUNCATED && status != 0)
    {
        freeResult();
        throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
    }

    // fetch actual fields
    for (int i = 0; i < last_columns_num; i++)
    {
        delete[] last_fetched_row[i];
        last_fetched_row[i] = new char[last_fetched_lengths[i]];
        bind[i].buffer = last_fetched_row[i];
        bind[i].buffer_length = last_fetched_lengths[i];
        bind[i].length = nullptr;
        if (mysql_stmt_fetch_column(m_stmt, &bind[i], i, 0))
        {
            freeResult();
            throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
        }
    }

    return true;
}

RowSchema* S2Connection::GetRowSchema(int* err)
{
    MySQLResultPtr res(mysql_stmt_result_metadata(m_stmt), &mysql_free_result);
    if (res == nullptr)
    {
        *err = 1;
        return nullptr;
    }

    unsigned int num_fields = mysql_num_fields(res.get());
    MYSQL_FIELD* fields = mysql_fetch_fields(res.get());

    auto freeColumns = [num_fields](Column column_info[])
    {
        for (int i = 0; i < num_fields; i++)
        {
            // name is created using strdup
            // so it should be deleted using free
            free(column_info[i].name);
        }

        delete[] column_info;
    };

    std::unique_ptr<Column[], decltype(freeColumns)> column_info(new Column[num_fields], freeColumns);
    memset(column_info.get(), 0, sizeof(Column) * num_fields);
    std::unique_ptr<RowSchema> row_schema(new RowSchema{column_info.get(), num_fields});
    super_chunk::utils::FieldsToRowSchema(num_fields, fields, row_schema.get());

    // release column_info as
    // it should be freed by the RowSchemaFree
    column_info.release();
    return row_schema.release();
}

bool S2Connection::HasNextRow()
{
    return last_fetched_row;
}

void
S2Connection::NextChunk(
    std::unique_ptr<SuperChunkWriter>& writer,
    Chunk* chunk,
    RowSchema* rowSchema)
{
    // TODO: handle the result
    writer->Reset(chunk, rowSchema);

    uint64_t row_count = 0;
    while (HasNextRow())
    {
        bool was_row_written = writer->WriteRow(this->last_fetched_row, this->last_fetched_lengths);
        if (was_row_written)
        {
            row_count++;
            Advance();
        }
        else  // if WriteRow returned false, the last_fetched_row has not been written to the current chunk
        {
            break;
        }
    }
    chunk->row_count = row_count;
}
