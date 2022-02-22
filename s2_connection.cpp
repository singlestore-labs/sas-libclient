#include "memory"
#include <sstream>

#include "hdat/chunk_writer.hpp"
#include "s2_connection.hpp"
#include "table_writer.hpp"

#define MySQLResultPtr std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)>

std::unique_ptr<S2Connection> S2Connection::Connect(const Credentials& creds)
{
    return Connect(
        creds.host.c_str(),
        creds.port,
        creds.db.c_str(),
        creds.user.c_str(),
        creds.password.c_str(),
        creds.ssl_ca);
}

std::unique_ptr<S2Connection>
S2Connection::Connect(
    const char* host,
    uint32_t port,
    const char* db,
    const char* user,
    const char* password,
    const char* ssl_ca)
{
    // allocate a S2Connection object
    std::unique_ptr<S2Connection> s2Connection(new S2Connection(host, port, db, user, password, ssl_ca));

    // create a mysql client connection
    s2Connection->m_conn = mysql_init(nullptr);
    if (!s2Connection->m_conn)
    {
        throw S2ClientError(S2C_ERROR_UNKNOWN_FAILURE, "Failed to init a connection using MySQL C client");
    }

    if (!user || user[0] == '\0')
    {
        user = "root";
    }

    if (ssl_ca && ssl_ca[0] != '\0')
    {
        mysql_options(s2Connection->m_conn, MYSQL_OPT_SSL_CA, ssl_ca);
        s2Connection->m_conn->options.use_ssl = 1;
    }

    if (!mysql_real_connect(s2Connection->m_conn, host, user, password, db, port, nullptr, 0))
    {
        throw S2ClientError(mysql_errno(s2Connection->m_conn), mysql_error(s2Connection->m_conn));
    }

    if (!(s2Connection->m_stmt = mysql_stmt_init(s2Connection->m_conn)))
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
    int totalPartitions = mysql_num_rows(res.get());  // this includes both Master and Slave partitions
    int numPartitions = 0;
    MYSQL_ROW row;
    for (int i = 0; i < totalPartitions; ++i)
    {
        row = mysql_fetch_row(res.get());
        if (!row[3])
        {
            throw S2ClientError(
                S2C_ERROR_UNKNOWN_FAILURE, "Failed to get SHOW PARTITIONS result: database returned invalid data");
        }
        if (!strcasecmp(row[3], "master")) ++numPartitions;
    }
    return numPartitions;
}

std::vector<AggregatorNode> S2Connection::GetAggregators()
{
    std::vector<AggregatorNode> aggregatorsList;
    std::string query = sql::MakeGetAggregatorsQuery();
    if (mysql_query(m_conn, query.c_str()))
    {
        throw S2ClientError(mysql_errno(m_conn), mysql_error(m_conn));
    }
    MYSQL_RES* res = mysql_store_result(m_conn);
    if (!res)
    {
        throw S2ClientError(
            S2C_ERROR_UNKNOWN_FAILURE,
            "Failed to get the list of aggregators from information_schema.mv_nodes");
    }
    MYSQL_ROW row;
    AggregatorNode node;
    while ((row = mysql_fetch_row(res)))
    {
        aggregatorsList.push_back(AggregatorNode
                                  {
                                      .host = row[1] ? std::string(row[1]) : "",
                                      .port = row[2] ? atoi(row[2]) : 0,
                                      .externalHost = row[3] ? std::string(row[3]) : "",
                                      .externalPort = row[4] ? atoi(row[4]) : 0
                                  });
    }
    mysql_free_result(res);

    return aggregatorsList;
}

int64_t S2Connection::ExecuteDDL(std::string query)
{
    if (mysql_query(m_conn, query.c_str()))
    {
        throw S2ClientError(mysql_errno(m_conn), mysql_error(m_conn));
    }
    return mysql_affected_rows(m_conn);
}

void
S2Connection::Prepare(
    const char* query,
    bool execute)
{
    if (!(m_stmt = mysql_stmt_init(m_conn)))
    {
        throw S2ClientError(mysql_errno(m_conn), mysql_error(m_conn));
    }
    if (mysql_stmt_prepare(m_stmt, query, strlen(query)))
    {
        throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
    }
    if (execute)
    {
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
}

void S2Connection::CleanupStatement(bool needFreeResult)
{
    mysql_stmt_close(m_stmt);
    m_stmt = nullptr;
    if (needFreeResult) FreeResult();
}

void S2Connection::FreeResult()
{
    // delete actual fields of the result
    for (int i = 0; i < m_last_columns_num; i++)
    {
        delete[] m_last_fetched_row[i];
        m_last_fetched_row[i] = nullptr;
    }
    delete[] m_last_fetched_row;
    m_last_fetched_row = nullptr;

    // delete lengths array
    delete[] m_last_fetched_lengths;
    m_last_fetched_lengths = nullptr;

    // reset columns number
    m_last_columns_num = 0;
}

bool S2Connection::Advance()
{
    // free the previous result set
    FreeResult();

    // find the columns number
    m_last_columns_num = mysql_stmt_field_count(m_stmt);

    // find the length of each field
    MySQLResultPtr metadata(mysql_stmt_result_metadata(m_stmt), &mysql_free_result);
    if (metadata == nullptr)
    {
        FreeResult();
        throw S2ClientError(S2C_ERROR_UNKNOWN_FAILURE, "Failed to get result set metadata");
    }

    MYSQL_FIELD* fields = mysql_fetch_fields(metadata.get());
    if (fields == nullptr)
    {
        FreeResult();
        throw S2ClientError(S2C_ERROR_UNKNOWN_FAILURE, "Failed to get result set metadata fields");
    }

    m_last_fetched_lengths = new unsigned long[m_last_columns_num];
    m_last_fetched_row = new char*[m_last_columns_num];
    memset(m_last_fetched_row, 0, sizeof(char*) * m_last_columns_num);

    std::unique_ptr<MYSQL_BIND[]> bind = std::make_unique<MYSQL_BIND[]>(m_last_columns_num);
    memset(bind.get(), 0, sizeof(MYSQL_BIND) * m_last_columns_num);

    my_bool is_null_arr[m_last_columns_num];
    my_bool need_column_re_fetch[m_last_columns_num];
    for (int i = 0; i < m_last_columns_num; i++)
    {
        int buffer_length = 8;
        switch (fields[i].type)
        {
            // for numeric and date/time types, actual data is fetched when mysql_stmt_fetch()
            // is called. For string types, we need to fetch the length first, then call
            // mysql_stmt_fetch_column to fetch the actual data
            case MYSQL_TYPE_LONG:
            {
                buffer_length = 4;
                need_column_re_fetch[i] = false;
                break;
            }
            case MYSQL_TYPE_LONGLONG:
            case MYSQL_TYPE_DOUBLE:
            {
                buffer_length = 8;
                need_column_re_fetch[i] = false;
                break;
            }
            // fixed size string
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_LONG_BLOB:
            case MYSQL_TYPE_MEDIUM_BLOB:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_TINY_BLOB:
            {
                buffer_length = 8;  // could be 0 potentially
                need_column_re_fetch[i] = true;
                break;
            }
            // date and time
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_NEWDATE:
            case MYSQL_TYPE_DATETIME2:
            {
                buffer_length = sizeof(MYSQL_TIME);
                need_column_re_fetch[i] = false;
                break;
            }
            default:
                // should never get here, but for unsupported types it can happen
                buffer_length = 8;
                need_column_re_fetch[i] = true;
                break;
        }

        m_last_fetched_row[i] = new char[buffer_length];
        bind[i].buffer = m_last_fetched_row[i];
        bind[i].buffer_length = buffer_length;
        bind[i].length = &m_last_fetched_lengths[i];
        bind[i].buffer_type = fields[i].type;
        bind[i].is_null = &is_null_arr[i];
    }

    if (mysql_stmt_bind_result(m_stmt, bind.get()))
    {
        FreeResult();
        throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
    }

    int status = mysql_stmt_fetch(m_stmt);
    if (status == MYSQL_NO_DATA)
    {
        CleanupStatement(true /* needFreeResult */);
        return false;
    }
    else if (status != MYSQL_DATA_TRUNCATED && status != 0)
    {
        FreeResult();
        throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
    }

    // fetch actual fields
    for (int i = 0; i < m_last_columns_num; i++)
    {
        if (*bind[i].is_null)
        {
            delete[] m_last_fetched_row[i];
            m_last_fetched_row[i] = nullptr;
            m_last_fetched_lengths[i] = 0;
            continue;
        }
        else if (!need_column_re_fetch[i])
        {
            continue;
        }
        else
        {
            delete[] m_last_fetched_row[i];
            unsigned long len = m_last_fetched_lengths[i];
            char* tmp = new char[len];
            m_last_fetched_row[i] = tmp;
        }
        bind[i].buffer = m_last_fetched_row[i];
        bind[i].buffer_length = m_last_fetched_lengths[i];
        bind[i].length = nullptr;
        if (mysql_stmt_fetch_column(m_stmt, &bind[i], i, 0))
        {
            FreeResult();
            throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
        }
    }

    return true;
}

RowSchema* S2Connection::GetRowSchema()
{
    MySQLResultPtr res(mysql_stmt_result_metadata(m_stmt), &mysql_free_result);
    if (!res)
    {
        throw S2ClientError(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
    }

    unsigned int num_fields = mysql_num_fields(res.get());
    MYSQL_FIELD* fields = mysql_fetch_fields(res.get());

    auto freeColumns = [num_fields](Column column_info[])
    {
        for (uint i = 0; i < num_fields; i++)
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
    utils::FieldsToRowSchema(num_fields, fields, row_schema.get());

    // release column_info as
    // it should be freed by the RowSchemaFree
    column_info.release();
    return row_schema.release();
}

RowSchema* S2Connection::ExplainRowSchema(const char* selectQuery)
{
    MYSQL_RES* res;
    MYSQL_ROW row;
    RowSchema* schema = new RowSchema;
    std::string query = sql::MakeExplainCreateResultTableQuery(selectQuery);
    if (mysql_query(m_conn, query.c_str()))
    {
        throw S2ClientError(mysql_errno(m_conn), mysql_error(m_conn));
    }
    res = mysql_store_result(m_conn);
    if (!res)
    {
        throw S2ClientError(
            S2C_ERROR_UNKNOWN_FAILURE,
            "Failed to get the result of EXPLAIN EXTENDED CREATE RESULT TABLE AS...");
    }
    row = mysql_fetch_row(res);
    unsigned long* lengths = mysql_fetch_lengths(res);
    utils::ExplainToRowSchema(std::string(row[0], lengths[0]), schema);

    mysql_free_result(res);
    return schema;
}

bool S2Connection::HasNextRow()
{
    return m_last_fetched_row;
}

void
S2Connection::NextChunk(
    std::unique_ptr<SuperChunkWriter>& writer,
    Chunk* chunk,
    RowSchema* rowSchema)
{
    writer->Reset(chunk, rowSchema);

    uint64_t row_count = 0;
    while (HasNextRow())
    {
        bool was_row_written = writer->WriteRow(m_last_fetched_row, m_last_fetched_lengths);
        if (was_row_written)
        {
            row_count++;
            Advance();
        }
        else  // if WriteRow returned false, the m_last_fetched_row has not been written to the current chunk
        {
            break;
        }
    }
    chunk->row_count = row_count;
}

Chunk*
S2Connection::GetSingleRow(
    SuperChunkWriter* writer,
    RowSchema* schema,
    const std::string resultTable,
    const uint32_t partitionId,
    const int64_t partitionRowId)
{
    std::string query = sql::MakePointInTimeQuery(resultTable.c_str(), partitionId, partitionRowId);
    Prepare(query.c_str(), true);

    uint64_t chunk_size = rowSize(schema, m_last_fetched_lengths);

    char* ptr = (char*)malloc(chunk_size);

    Chunk* chunk = new Chunk();
    chunk->m_ptr = ptr;
    chunk->m_size = chunk_size;
    chunk->row_count = 0;
    chunk->partition_id = partitionId;
    chunk->consumed_size = 0;

    writer->Reset(chunk, schema);
    writer->WriteRow(m_last_fetched_row, m_last_fetched_lengths);

    CleanupStatement(true /* needFreeResult */);

    return chunk;
}

void
S2Connection::WriteChunk(
    std::unique_ptr<SuperChunkReader>& reader,
    Chunk* chunk,
    RowSchema* schema,
    const std::string table)
{
    int is_infile_enabled;
    if (mysql_get_option(m_conn, MYSQL_OPT_LOCAL_INFILE, &is_infile_enabled))
    {
        FreeResult();
        throw S2ClientError(mysql_errno(m_conn), mysql_error(m_conn));
    }
    if (!is_infile_enabled)
    {
        FreeResult();
        throw S2ClientError(S2C_ERROR_BAD_CONNECTION, "LOAD DATA INFILE LOCAL is disabled on the server side");
    }

    reader->Reset(chunk, schema);

    std::stringstream tsv_output;
    TableWriter tw(&tsv_output);

    tw.ChunkToTSV(reader, chunk->row_count);

    mysql_set_local_infile_handler(
        m_conn,
        tw.ss_local_infile_init,
        tw.ss_local_infile_read,
        tw.ss_local_infile_end,
        tw.ss_local_infile_error,
        &tsv_output);

    std::string query = sql::MakeLoadDataQuery(table);

    if (mysql_query(m_conn, query.c_str()))
    {
        throw S2ClientError(mysql_errno(m_conn), mysql_error(m_conn));
    }
}
