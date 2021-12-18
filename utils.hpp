#ifndef UTILS_HPP
#define UTILS_HPP

#include <atomic>
#include <ios>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "mysql.h"

#include "chunk_extern.h"
#include "s2_client_error.hpp"

typedef struct Credentials
{
    std::string host;
    uint32_t port;
    std::string db;
    std::string user;
    std::string password;
} Credentials;

typedef struct AggregatorNode
{
    std::string host;
    int port;
    std::string externalHost;
    int externalPort;
} AggregatorNode;

namespace utils
{
    std::vector<int>
    ConsumerPartitions(
        int nConsumers,
        int consumerId,
        std::vector<int> workerPartitions);

    std::vector<int>
    WorkerPartitions(
        int numWorkers,
        int workerId,
        int totalPartitions);

    void
    FieldsToRowSchema(
        int num_fields,
        MYSQL_FIELD* fields,
        RowSchema* rowSchema /*out*/);

    void FillCredentials(
        const std::vector<AggregatorNode> &aggregators,
        const int partition,
        Credentials *creds /*out*/);

    void
    ExplainToRowSchema(
        std::string explainStr,
        RowSchema* rowSchema /*out*/);

    extern "C"
    {
        void RowSchemaFree(RowSchema* schema);

        void
        MoveChunk(
            Chunk* out,
            Chunk* in);

        void ChunkFree(Chunk* chunk);

        const char* S2GetClientVersion();
    }
}  // namespace utils

namespace sql
{
    std::string
    MakeCreateResultTableQuery(
        const char* resultTableName,
        const char* selectQuery,
        bool materialized,
        const char* const* const partitionByCols,
        const int partitionByColsNumber,
        const char* const* const partitionOrderByCols,
        const int orderByColsNumber);

    std::string MakeDropQuery(const char* resultTableName);

    std::string
    MakeReadResultTableQuery(
        const char* resultTableName,
        uint32_t partition);

    std::string
    MakePointInTimeQuery(
        const char* table,
        int partition_id,
        int64_t row_id);

    std::string MakeLoadDataQuery(
        const std::string& tableName);

    std::string MakeSelectQueryMeta(
        const std::string& tableName);

    std::string MakeExplainCreateResultTableQuery(
        const char* selectQuery);

    std::string MakeGetAggregatorsQuery();
}  // namespace sql

#endif  // UTILS_HPP
