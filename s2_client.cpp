#include "s2_client.hpp"

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

    return s2Client;
}

void S2Client::SetError(S2ClientError err)
{
    std::unique_lock<std::mutex> lock(m_error_mutex);
    m_error = std::move(err);
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
        int* errCode /*out*/)
    {
        *errCode = 0;
        try
        {
            return S2Client::Connect(workerId, numWorkers, host, port, db, user, password).release();
        }
        catch (S2ClientError &s2_err)
        {
            // TODO: find a place to save error message
            // in the case, if we failed to create S2Client
            *errCode = s2_err.m_errorCode;
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
        catch (S2ClientError &s2_err)
        {
            client->SetError(s2_err);
            *err = s2_err.m_errorCode;
        }
    }

    int GetPartitionsNumber(S2Client* client)
    {
        return client->m_numPartitions;
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
