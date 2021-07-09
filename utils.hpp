#ifndef UTILS_HPP
#define UTILS_HPP

#include <atomic>
#include <ios>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "libmysql/mysql.h"

#include "chunk_extern.h"
#include "s2_client_error.hpp"

namespace super_chunk
{
    namespace utils
    {
        std::vector<int>
        AssignedPartitions(
            int numWorkers,
            int workerId,
            int totalPartitions);

        void
        FieldsToRowSchema(
            int num_fields,
            MYSQL_FIELD* fields,
            RowSchema* rowSchema /*out*/);

        void RowSchemaFree(RowSchema* schema);

        extern "C"
        {
            void
            CopyChunk(
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
            bool materialized);

        std::string MakeDropQuery(const char* resultTableName);

        std::string
        MakeReadResultTableQuery(
            const char* resultTableName,
            uint32_t partition);

        std::string MakeLoadDataQuery(
            const std::string &tableName);

        std::string MakeSelectQueryMeta(
            const std::string &tableName);
    }  // namespace sql
}  // namespace super_chunk

#endif  // UTILS_HPP
