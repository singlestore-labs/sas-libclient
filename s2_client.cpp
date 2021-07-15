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
    const char* password)
{
    // allocate a S2Client object
    std::unique_ptr<S2Client> s2Client(new S2Client(workerId, numWorkers));

    // create a connection
    s2Client->m_conn = S2Connection::Connect(host, port, db, user, password);

    // get the number of partitions
    s2Client->m_numPartitions = s2Client->m_conn->GetPartitionsNumber();

    s2Client->m_chunk_reader = std::make_unique<SuperChunkReader>();

    return s2Client;
}

void S2Client::SetError(S2ClientError err)
{
    std::unique_lock<std::mutex> lock(m_error_mutex);
    m_error = std::move(err);
}

void
S2Client::LoadDataWrite(
    Chunk* chunk,
    RowSchema* schema,
    const char* table)
{
    this->m_conn->WriteChunk(this->m_chunk_reader, chunk, schema, table);
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
        int numWorkers,
        int workerId,
        S2ErrorCallback* cb)
    {
        try
        {
            return S2Client::Connect(workerId, numWorkers, host, port, db, user, password).release();
        }
        catch (S2ClientError& s2_err)
        {
            cb->setError(cb, s2_err.m_errorCode, std::move(s2_err.m_errorMessage).c_str());
            return nullptr;
        }
    }

    void S2ClientFree(S2Client* s2Client)
    {
        delete s2Client;
    }

    void
    ExecuteDDLQuery(
        S2Client* client,
        const char* query,
        int* err)
    {
        try
        {
            client->m_conn->ExecuteDDL(query);
            *err = 0;
        }
        catch (S2ClientError& s2_err)
        {
            client->SetError(s2_err);
            *err = s2_err.m_errorCode;
        }
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
            cb->setError(cb, s2_err.m_errorCode, std::move(s2_err.m_errorMessage).c_str());
        }
    }

    RowSchema*
    GetTableRowSchema(
        S2Client* client,
        const char* table,
        S2ErrorCallback* cb)
    {
        std::string query = super_chunk::sql::MakeSelectQueryMeta(table);
        try
        {
            client->m_conn->Prepare(query.c_str());
            int err = 0;
            RowSchema* res = client->m_conn->GetRowSchema(&err);
            if (err)
            {
                cb->setError(cb, S2C_ERROR_UNKNOWN_FAILURE, "Error getting RowSchema in mysql_stmt_result_metadata");
                return nullptr;
            }
            return res;
        }
        catch (S2ClientError& s2_err)
        {
            cb->setError(cb, s2_err.m_errorCode, std::move(s2_err.m_errorMessage).c_str());
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
