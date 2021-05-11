#ifndef S2_CONNECTION_HPP
#define S2_CONNECTION_HPP

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include <iostream>

#include "libmysql/mysql.h"

#include "chunk_extern.h"
#include "utils.hpp"

#include "hdat/chunk_writer.hpp"

using super_chunk::structs::PartitionChunk;

// S2Connection represents an actual connection to S2
// It is a wrapper around mysql client
class S2Connection
{
  public:
    const char* m_host;
    uint32_t m_port;
    const char* m_db;
    const char* m_user;
    const char* m_password;

    // Connect creates an instance of S2Connection and connects to the S2 using mysql C client lib
    static std::unique_ptr<S2Connection>
    Connect(
        const char* host,
        uint32_t port,
        const char* db,
        const char* user,
        const char* password);

    ~S2Connection()
    {
        freeResult();
        if (m_stmt)
        {
            mysql_stmt_close(m_stmt);
            m_stmt = nullptr;
        }
        if (m_conn)
        {
            mysql_close(m_conn);
            m_conn = nullptr;
        }
    }

    // Prepare prepares a query using mysql binary protocol and then executes it
    // It tries to prefetch the first row
    void Prepare(const char* query);

    // ExecuteDDL runs a ddl query through text protocol
    void ExecuteDDL(std::string query);

    // Advance retrieves the next row from the result set and saves a result in the
    // last_fetched_row, last_fetched_lengths, last_columns_num variables
    bool Advance();

    // GetRowSchema retrieves the schema of query result
    RowSchema* GetRowSchema();

    // HasNextRow returns true if the result set has a data
    bool HasNextRow();

    // NextChunk returns the next chunk of the data
    void
    NextChunk(
        std::unique_ptr<SuperChunkWriter>& writer,
        Chunk* chunk,
        RowSchema* schema);

    // GetPartitionsNumber returns the number of partitions in the table
    int GetPartitionsNumber();

  private:
    MYSQL* m_conn = nullptr;
    MYSQL_STMT* m_stmt = nullptr;
    MYSQL_ROW last_fetched_row = nullptr;
    unsigned long* last_fetched_lengths = nullptr;
    int last_columns_num = 0;

    S2Connection(
        const char* host,
        uint32_t port,
        const char* db,
        const char* user,
        const char* password)
        :
        m_host(host),
        m_port(port),
        m_db(db),
        m_user(user),
        m_password(password)
            {};

    void freeResult();
};

#endif  // S2_CONNECTION_HPP
