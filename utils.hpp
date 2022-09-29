#ifndef UTILS_HPP
#define UTILS_HPP

#include <atomic>
#include <ios>
#include <memory>
#include <set>
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
    const char* ssl_ca;
} Credentials;

typedef struct AggregatorNode
{
    std::string host;
    int port;
    std::string externalHost;
    int externalPort;
} AggregatorNode;

enum TableType
{
    AggResultTable,
    AggResultTableMaterialized,
    RegularTable,
};

typedef struct TableKeys
{
    std::set<std::string> shard_key;
    std::vector<std::string> sort_key;
} TableKeys;

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

    bool
    IsNullBuffer(
        const char* buff,
        int size);

    void
    FillCredentials(
        const std::vector<AggregatorNode>& aggregators,
        const int partition,
        Credentials* creds /*out*/);

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
    MakeCreateTableQuery(
        const char* resultTableName,
        const char* selectQuery,
        const char* keyColumnName,
        TableType tableType,
        const char* const* const partitionByCols,
        const int partitionByColsNumber,
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber);

    std::string MakeDropResultQuery(const char* resultTableName);

    std::string MakeDropTableQuery(const char* tableName);

    std::string
    MakeReadResultTableQuery(
        const char* resultTableName,
        uint32_t partition,
        const std::string& s2Version);

    std::string
    MakeReadColumnStoreTableQuery(
        const char* tableName,
        const char* keyColumnName,
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        uint32_t partition);

    std::string
    MakeReadOriginalTableQuery(
        const char* query,
        const std::vector<std::string>* columnstoreFullSortKey,
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        uint32_t partition);

    std::string
    MakePointInTimeQuery(
        const std::string& table,
        const std::string& query,
        const std::string& keyColumnName,
        int partition_id,
        int64_t row_id,
        bool is_result_table,
        const std::string& serverVersion);

    std::string
    MakeRowIdFilterQuery(
        const std::string& table,
        const std::string& selectQuery,
        const std::string& keyColumnName,
        int partition_id,
        const int64_t* rowIds,
        const int rowIdsNum);

    std::string
    MakeLoadDataQuery(
        const std::string& tableName,
        const RowSchema* schema);

    std::string MakeSelectQueryMeta(
        const std::string& tableName);

    std::string MakeExplainCreateResultTableQuery(
        const char* selectQuery);

    std::string MakeGetAggregatorsQuery();

    std::string MakeGetTableKeysQuery(const char* db, const char* tableName);
}  // namespace sql

#endif  // UTILS_HPP
