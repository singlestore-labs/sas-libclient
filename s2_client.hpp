#ifndef S2_CLIENT_HPP
#define S2_CLIENT_HPP

#include "s2_connection.hpp"
#include <mutex>

// S2Client saves a global client configurations
// This is a number of partitions, number of workers, worker index, and connection to S2
class S2Client
{
  public:
    uint32_t m_numPartitions;
    uint32_t m_workerId;
    uint32_t m_numWorkers;
    std::unique_ptr<S2Connection> m_conn;

    S2ClientError m_error;
    std::mutex m_error_mutex;

    // Connect creates an instance of S2Client
    // It also connects to the S2 and retrieves the number of partitions
    static std::unique_ptr<S2Client>
    Connect(
        uint32_t workerId,
        uint32_t numWorkers,
        const char* host,
        uint32_t port,
        const char* db,
        const char* user,
        const char* password);

    void SetError(S2ClientError);

  private:
    S2Client(
        uint32_t workerId,
        uint32_t numWorkers)
        :
        m_numPartitions(0),
        m_workerId(workerId),
        m_numWorkers(numWorkers),
        m_conn(nullptr),
        m_error(0, "")
    {
    }
};

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
        int* err /*out*/);

    void S2ClientFree(S2Client* s2Client);

    int GetPartitionsNumber(S2Client* client);

    const char* S2Error(S2Client* client);

    int S2Errno(S2Client* client);
}

#endif  // S2_CLIENT_HPP
