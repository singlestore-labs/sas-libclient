#include <string.h>

#include <iostream>
#include <iomanip>
#include <sstream>

#include "client_config.h"
#include "utils.hpp"

namespace super_chunk
{
    namespace utils
    {
        std::vector<int>
        AssignedPartitions(
            int numWorkers,
            int workerId,
            int totalPartitions)
        {
            if (numWorkers <= 0)
            {
                throw S2ClientError(S2C_ERROR_INV_ARG, "received negative numWorkers");
            }
            std::vector<int> result;
            for (int i = workerId; i < totalPartitions; i += numWorkers)
            {
                result.push_back(i);
            }
            return result;
        }

        void
        FieldsToRowSchema(
            int num_fields,
            MYSQL_FIELD* fields,
            RowSchema* rowSchema /*out*/)
        {
            Column* column_info = rowSchema->ColumnInfo;

            for (int i = 0; i < num_fields; ++i)
            {
                column_info[i].name = strdup(fields[i].name);
                switch (fields[i].type)
                {
                    // integer types
                    case MYSQL_TYPE_TINY:
                    {
                        column_info[i].type = Int32;
                        break;
                    }
                    case MYSQL_TYPE_SHORT:
                    {
                        column_info[i].type = Int32;
                        break;
                    }
                    case MYSQL_TYPE_LONG:
                    {
                        column_info[i].type = Int32;
                        break;
                    }
                    case MYSQL_TYPE_INT24:
                    {
                        column_info[i].type = Int32;
                        break;
                    }
                    case MYSQL_TYPE_LONGLONG:
                    {
                        column_info[i].type = Int64;
                        break;
                    }
                    // floating point types
                    case MYSQL_TYPE_FLOAT:
                    {
                        column_info[i].type = Double;
                        break;
                    }
                    case MYSQL_TYPE_DOUBLE:
                    {
                        column_info[i].type = Double;
                        break;
                    }
                    // fixed size string
                    case MYSQL_TYPE_STRING:
                    {
                        column_info[i].type = Fixed;
                        column_info[i].size = fields[i].length;
                        break;
                    }
                    // variable length string
                    case MYSQL_TYPE_VAR_STRING:
                    {
                        column_info[i].type = Variable;
                        break;
                    }
                    case MYSQL_TYPE_BLOB:
                    {
                        column_info[i].type = Variable;
                        break;
                    }
                    default:
                        column_info[i].type = Unsupported;
                        break;
                }
            }
        }

        void RowSchemaFree(RowSchema* schema)
        {
            if (!schema)
            {
                return;
            }
            for (int i = 0; i < schema->numColumns; i++)
            {
                free(schema->ColumnInfo[i].name);
            }
            delete[] schema->ColumnInfo;
            delete schema;
            schema = nullptr;
        }

        extern "C"
        {
            void
            CopyChunk(
                Chunk* out,
                Chunk* in)
            {
                out->m_ptr = in->m_ptr;
                out->m_size = in->m_size;
                out->row_count = in->row_count;
                out->id = in->id;
                out->partition_id = in->partition_id;
                out->consumed_size = in->consumed_size;
            }

            void ChunkFree(Chunk* chunk)
            {
                if (chunk)
                {
                    free(chunk->m_ptr);
                }
            }

            const char* S2GetClientVersion()
            {
                return S2_CLIENT_VERSION;
            }
        }
    }  // namespace utils

    namespace sql
    {
        std::string QuotedName(const std::string& name)
        {
            std::stringstream out;
            out << std::quoted(name, '`', '`');
            return out.str();
        }

        std::string
        PartitionBy(
            const char* const* const partitionByCols,
            const int partitionByColsNumber)
        {
            if (partitionByColsNumber <= 0)
            {
                return "";
            }
            std::string result = " partition by (";
            result += QuotedName(partitionByCols[0]);

            for (int i = 1; i < partitionByColsNumber; ++i)
            {
                result += ", ";
                result += QuotedName(partitionByCols[i]);
            }
            result += ") ";
            return result;
        }

        std::string
        MakeCreateResultTableQuery(
            const char* resultTableName,
            const char* selectQuery,
            bool materialized,
            const char* const* const partitionByCols,
            const int partitionByColsNumber)
        {
            std::string resultQuery;

            if (materialized)
            {
                resultQuery += "CREATE MATERIALIZED RESULT TABLE ";
            }
            else
            {
                resultQuery += "CREATE RESULT TABLE ";
            }
#ifdef REGULAR_TABLE_MODE
            resultQuery = "CREATE TABLE ";
#endif

            resultQuery += QuotedName(resultTableName);
            resultQuery += PartitionBy(partitionByCols, partitionByColsNumber);
            resultQuery += " AS ";
            resultQuery += selectQuery;
            return resultQuery;
        }

        std::string MakeDropQuery(const char* resultTableName)
        {
            std::string resultQuery = "DROP RESULT TABLE ";

#ifdef REGULAR_TABLE_MODE
            resultQuery = "DROP TABLE ";
#endif

            resultQuery += QuotedName(resultTableName);
            return resultQuery;
        }

        std::string
        MakeReadResultTableQuery(
            const char* resultTableName,
            uint32_t partition)
        {
            std::string resultQuery = "SELECT * FROM ";

#ifndef REGULAR_TABLE_MODE
            resultQuery += "::";
#endif
            resultQuery += QuotedName(resultTableName);

            resultQuery += " WHERE partition_id() = ";
            resultQuery += std::to_string(partition);
            return resultQuery;
        }

        std::string
        MakePointInTimeQuery(
            const char* table,
            int partition_id,
            int row_id)
        {
            std::string resultQuery = "SELECT * FROM ::" + QuotedName(table);
            resultQuery += " WHERE partition_id() = " + std::to_string(partition_id);
            resultQuery += " AND partition_row_id() = " + std::to_string(row_id);
            return resultQuery;
        }

        std::string MakeLoadDataQuery(const std::string& tableName)
        {
            return "LOAD DATA LOCAL INFILE 'placeholder' INTO TABLE " + QuotedName(tableName);
        }

        std::string MakeSelectQueryMeta(const std::string& tableName)
        {
            return "SELECT * FROM " + QuotedName(tableName) + " WHERE 1 = 0";
        }
    }  // namespace sql
}  // namespace super_chunk
