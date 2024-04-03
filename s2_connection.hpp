#ifndef S2_CONNECTION_HPP
#define S2_CONNECTION_HPP

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include <iostream>

#include "mysql.h"

#include "chunk_extern.h"
#include "tsv_table_writer.hpp"
#include "avro_table_writer.hpp"
#include "utils.hpp"

#include "hdat/chunk_writer.hpp"
#include "hdat/chunk_reader.hpp"

// S2Connection represents an actual connection to S2
// It is a wrapper around mysql client
class S2Connection
{
  public:
    const char* m_host;
    const uint32_t m_port;
    const char* m_db;
    const char* m_user;
    const char* m_password;
    const char* m_ssl_ca;

    static std::unique_ptr<S2Connection> Connect(const Credentials& creds);
    static std::unique_ptr<S2Connection>
    ConnectWithRetryMA(
        const Credentials& creds,
        const Credentials& masterCreds,
        S2ClientError& err);

    // Connect creates an instance of S2Connection and connects to the S2 using mysql C client lib
    static std::unique_ptr<S2Connection>
    Connect(
        const char* host,
        const uint32_t port,
        const char* db,
        const char* user,
        const char* password,
        const char* ssl_ca);

    ~S2Connection()
    {
        FreeLastFetched();
        // if the flag m_need_stmt_close is set to true (default behavior)
        // we close the statement before closing the connection.
        // Otherwise we close the statement after connection, this
        // only frees the memory on the client side
        if (m_stmt && m_need_stmt_close)
        {
            CleanupStatement(false /* needFreeResult */);
        }
        if (m_conn)
        {
            mysql_close(m_conn);
            m_conn = nullptr;
        }
        if (m_stmt)
        {
            CleanupStatement(false /* needFreeResult */);
        }
    }

    // Prepare prepares a query using mysql binary protocol
    // If execute is set to true, then executes it
    // and fetches the first row. Returns true if the row has been fetched successfully
    bool
    Prepare(
        const char* query,
        bool execute,
        bool prefetch);

    // CleanupStatement is called to reset m_stmt to NULL
    // and free the memory used to fetch results, if needed
    void CleanupStatement(bool needFreeResult);

    // ExecuteDDL runs a ddl query through text protocol
    int64_t ExecuteDDL(const std::string query);

    // Execute runs a query through text protocol
    bool Execute(const std::string query, bool prefetch);

    // Advance retrieves the next row from the result set and saves a result in the
    // m_last_fetched_row, m_last_fetched_lengths, m_last_columns_num variables
    bool Advance();

    // GetRowSchema retrieves the schema of query result
    // If an error occurred, S2ClientError is thrown
    RowSchema* GetRowSchema(bool useOriginalName);

    // ExplainRowSchema gets the row schema for the result table
    // created using selectQuery
    RowSchema* ExplainRowSchema(const char* selectQuery);

    // GetParallelReadType checks what ParallelReadType
    // we need to use to read the results of selectQuery
    ParallelReadType
    GetParallelReadType(
        const char* selectQuery,
        const char* sourceTable,
        const char* keyColumnName,
        bool materialized,
        const char* const* const partitionByCols,
        int partitionByColsNumber,
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        ParallelReadType readType);

    TableKeys GetTableKeys(const char* sourceTable);
    bool CheckExplainOperations(const char* selectQuery);

    // HasNextRow returns true if the result set has a data
    bool HasNextRow();

    // NextChunk returns the next chunk of the data
    void
    NextChunk(
        std::unique_ptr<SuperChunkWriter>& writer,
        Chunk* chunk,
        RowSchema* schema);

    Chunk*
    GetSingleRow(
        SuperChunkWriter* writer,
        RowSchema* schema,
        const std::string& resultTable,
        const std::string& selectQuery,
        const std::string& keyColumnName,
        const uint32_t partitionId,
        const int64_t partitionRowId,
        ParallelReadType readType,
        std::string& serverVersion);

    void
    GetMultipleRows(
        SuperChunkWriter* writer,
        Chunk* chunk /*out*/,
        RowSchema* schema,
        const std::string& resultTable,
        const std::string& selectQuery,
        const std::string& keyColumnName,
        const uint32_t partitionId,
        const int64_t* rowIds,
        const int rowIdsNum,
        ParallelReadType readType);

    // GetPartitionsNumber returns the number of partitions in the table
    int GetPartitionsNumber();

    // GetServerVersion returns @@memsql_version server variable
    std::string GetServerVersion();

    // GetAggregators returns the list of all aggregators in the cluster
    std::vector<AggregatorNode> GetAggregators();

    // DiscardStmtClose sets the flag that indicates that we skip
    // closing the prepared statement which is being processed.
    // This results in slight memory leaks, but allows us to
    // destroy ChunkQueue object fast
    void DiscardStmtClose()
    {
        m_need_stmt_close = false;
    }
    // WriteChunk executes LOAD DATA LOCAL INFILE
    void
    WriteChunk(
        std::unique_ptr<SuperChunkReader>& reader,
        Chunk* chunk,
        RowSchema* schema,
        const std::string table);

    void
    WriteAvro(
        AvroBuffer* sourceData,
        const RowSchema* schema,
        const std::string& table,
        bool treatZeroLenAsNull);

  private:
    MYSQL* m_conn = nullptr;
    MYSQL_STMT* m_stmt = nullptr;
    MYSQL_RES* m_res = nullptr;
    bool m_need_stmt_close = true;
    bool m_use_binary_protocol = true;

    MYSQL_ROW m_last_fetched_row = nullptr;
    unsigned long* m_last_fetched_lengths = nullptr;
    int m_last_columns_num = 0;

    S2Connection(
        const char* host,
        const uint32_t port,
        const char* db,
        const char* user,
        const char* password,
        const char* ssl_ca)
        :
        m_host(host),
        m_port(port),
        m_db(db),
        m_user(user),
        m_password(password),
        m_ssl_ca(ssl_ca)
            {};

    // AdvanceBinary retrieves the next row from the result set of a query executed
    // using binary (prepared statement) protocol, and saves the result in the
    // m_last_fetched_row, m_last_fetched_lengths, m_last_columns_num variables
    bool AdvanceBinary();

    // AdvanceText retrieves the next row from the result set of a query executed
    // using text protocol, and saves the result in the
    // m_last_fetched_row, m_last_fetched_lengths, m_last_columns_num variables
    bool AdvanceText();

    void FreeLastFetched();
};

#endif  // S2_CONNECTION_HPP
