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

#ifndef S2_CLIENT_HPP
#define S2_CLIENT_HPP

#include <mutex>

#include "s2_connection.hpp"

// S2Client saves a global client configurations
// This is a number of partitions, number of workers, worker index, and connection to S2
class S2Client
{
  public:
    uint32_t m_numPartitions;
    std::string m_serverVersion;
    uint32_t m_workerId;
    uint32_t m_numWorkers;
    std::unique_ptr<S2Connection> m_conn;
    std::unique_ptr<SuperChunkReader> m_chunk_reader;

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
        const char* password,
        const char* ssl_ca);

    void
    SetError(
        S2ClientError err,
        S2ErrorCallback* cb);

    void
    LoadDataWrite(
        Chunk* chunk,
        RowSchema* schema,
        const char* table);

    void
    LoadDataAvro(
        char* sourceData,
        int64_t sourceDataLen,
        RowSchema* schema,
        const char* table,
        bool treatZeroLenAsNull);

  private:
    S2Client(
        uint32_t workerId,
        uint32_t numWorkers)
        :
        m_numPartitions(0),
        m_workerId(workerId),
        m_numWorkers(numWorkers),
        m_conn(nullptr),
        m_chunk_reader(nullptr),
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
        const char* ssl_ca,
        int numWorkers,
        int workerId,
        S2ErrorCallback* cb);

    void S2ClientFree(S2Client* s2Client);

    int64_t
    ExecuteDDLQuery(
        S2Client* client,
        const char* query,
        S2ErrorCallback* cb);

    int GetPartitionsNumber(S2Client* client);

    void
    LoadDataWrite(
        S2Client* client,
        Chunk* chunk,
        RowSchema* schema,
        const char* table,
        S2ErrorCallback* cb);

    RowSchema*
    GetTableRowSchema(
        S2Client* client,
        const char* table,
        S2ErrorCallback* cb);

    const char* S2Error(S2Client* client);

    int S2Errno(S2Client* client);
}

#endif  // S2_CLIENT_HPP
