#include <iomanip>

#include "s2_client.hpp"
#include "utils.hpp"

std::unique_ptr<S2Client>
S2Client::Connect(
    uint32_t workerId,
    uint32_t numWorkers,
    const char* host,
    uint32_t port,
    const char* db,
    const char* user,
    const char* password,
    const char* ssl_ca)
{
    // allocate a S2Client object
    std::unique_ptr<S2Client> s2Client(new S2Client(workerId, numWorkers));

    // create a connection
    s2Client->m_conn = S2Connection::Connect(host, port, db, user, password, ssl_ca);

    // get the number of partitions
    s2Client->m_numPartitions = s2Client->m_conn->GetPartitionsNumber();
    s2Client->m_serverVersion = s2Client->m_conn->GetServerVersion();

    s2Client->m_chunk_reader = std::make_unique<SuperChunkReader>();

    return s2Client;
}

void
S2Client::SetError(
    S2ClientError s2_err,
    S2ErrorCallback* cb)
{
    if (cb)
    {
        cb->setError(cb, s2_err.m_errorCode, s2_err.m_errorMessage.c_str(), S2C_SEVERITY_ERROR);
    }
    std::unique_lock<std::mutex> lock(m_error_mutex);
    m_error = std::move(s2_err);
}

void
S2Client::LoadDataWrite(
    Chunk* chunk,
    RowSchema* schema,
    const char* table)
{
    this->m_conn->WriteChunk(this->m_chunk_reader, chunk, schema, table);
}

void
S2Client::LoadDataAvro(
    char* sourceData,
    int64_t sourceDataLen,
    RowSchema* schema,
    const char* table)
{
    AvroBuffer source =
        {
            .m_ptr = sourceData,
            .m_size = sourceDataLen,
            .m_current_pos = 0
        };
    this->m_conn->WriteAvro(&source, schema, table);
}

extern "C"
{
    S2Client*
    S2ClientInit(
        const char* host,
        uint32_t port,
        const char* db,
        const char* user,
        const char* password,
        const char* ssl_ca,
        int numWorkers,
        int workerId,
        S2ErrorCallback* cb)
    {
        try
        {
            return S2Client::Connect(workerId, numWorkers, host, port, db, user, password, ssl_ca).release();
        }
        catch (S2ClientError& s2_err)
        {
            cb->setError(cb, s2_err.m_errorCode, std::move(s2_err.m_errorMessage).c_str(), S2C_SEVERITY_ERROR);
            return nullptr;
        }
    }

    void S2ClientFree(S2Client* s2Client)
    {
        delete s2Client;
    }

    int64_t
    ExecuteDDLQuery(
        S2Client* client,
        const char* query,
        S2ErrorCallback* cb)
    {
        // clear the previous error if any
        client->SetError(S2ClientError(0, ""), nullptr);
        int64_t affected_rows = 0;
        try
        {
            affected_rows = client->m_conn->ExecuteDDL(query);
        }
        catch (S2ClientError& s2_err)
        {
            client->SetError(s2_err, cb);
            affected_rows = -1;
        }
        return affected_rows;
    }

    int GetPartitionsNumber(S2Client* client)
    {
        return client->m_numPartitions;
    }

    void
    LoadDataWrite(
        S2Client* client,
        Chunk* chunk,
        RowSchema* schema,
        const char* table,
        S2ErrorCallback* cb)
    {
        try
        {
            client->LoadDataWrite(chunk, schema, table);
        }
        catch (S2ClientError& s2_err)
        {
            cb->setError(cb, s2_err.m_errorCode, std::move(s2_err.m_errorMessage).c_str(), S2C_SEVERITY_ERROR);
        }
    }

    void
    LoadDataAvro(
        S2Client* client,
        char* sourceData,
        int64_t sourceDataLen,
        RowSchema* schema,
        const char* table,
        S2ErrorCallback* cb)
    {
        try
        {
            client->LoadDataAvro(sourceData, sourceDataLen, schema, table);
        }
        catch (S2ClientError& s2_err)
        {
            cb->setError(cb, s2_err.m_errorCode, std::move(s2_err.m_errorMessage).c_str(), S2C_SEVERITY_ERROR);
        }
    }

    RowSchema*
    GetTableRowSchema(
        S2Client* client,
        const char* table,
        S2ErrorCallback* cb)
    {
        std::string query = sql::MakeSelectQueryMeta(table);
        try
        {
            client->m_conn->Prepare(query.c_str(), false);
            RowSchema* res = client->m_conn->GetRowSchema();
            return res;
        }
        catch (S2ClientError& s2_err)
        {
            cb->setError(cb, s2_err.m_errorCode, std::move(s2_err.m_errorMessage).c_str(), S2C_SEVERITY_ERROR);
        }
        return nullptr;
    }

    const char* S2Error(S2Client* client)
    {
        std::unique_lock<std::mutex> lock(client->m_error_mutex);
        return client->m_error.m_errorMessage.c_str();
    }

    int S2Errno(S2Client* client)
    {
        std::unique_lock<std::mutex> lock(client->m_error_mutex);
        return client->m_error.m_errorCode;
    }
}
