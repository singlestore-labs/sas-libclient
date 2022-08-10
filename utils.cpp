#include <string.h>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <iomanip>
#include <sstream>

#include "client_config.h"
#include "utils.hpp"

const double doubleNull = *((double*)"\0\0\0\0\0\xfe\xff\xff");

namespace utils
{
    typedef struct FieldDefSplit
    {
        std::size_t name_start;
        std::size_t name_end;
        std::size_t type_start;
        std::size_t type_end;
        std::size_t field_end;
    } FieldDefSplit;

    std::vector<int>
    ConsumerPartitions(
        int nConsumers,
        int consumerId,
        std::vector<int> workerPartitions)
    {
        if (nConsumers <= 0)
        {
            throw S2ClientError(S2C_ERROR_INV_ARG, "received negative nConsumers");
        }
        std::vector<int> result;
        for (uint64_t i = consumerId; i < workerPartitions.size(); i += nConsumers)
        {
            result.push_back(workerPartitions[i]);
        }
        return result;
    }

    std::vector<int>
    WorkerPartitions(
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

    // IsNullBuffer returns true if buff consists of only zero bytes (BINARY NULL)
    // or spaces (CHAR NULL)
    bool
    IsNullBuffer(
        const char* buff,
        int size)
    {
        char firstChar = *buff;
        if (firstChar != ' ' && firstChar != '\0') return false;
        buff++;
        size--;

        while (size--)
        {
            if (*buff != firstChar) return false;
            buff++;
        }
        return true;
    }

    void
    FillCredentials(
        const std::vector<AggregatorNode>& aggregators,
        const int partition,
        Credentials* creds)
    {
        if (!aggregators.size()) return;
        int aggNumber = partition % aggregators.size();
        if (aggregators[aggNumber].externalHost.size())
        {
            creds->host = aggregators[aggNumber].externalHost;
            creds->port = aggregators[aggNumber].externalPort;
        }
        else
        {
            creds->host = aggregators[aggNumber].host;
            creds->port = aggregators[aggNumber].port;
        }
        // we need to tell mysql API that we are using tcp, not unix socket
        if (!(creds->host).compare("localhost"))
        {
            creds->host = "127.0.0.1";
        }
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
                case MYSQL_TYPE_LONG:
                {
                    column_info[i].type = Int32;
                    column_info[i].size = 4;
                    break;
                }
                case MYSQL_TYPE_LONGLONG:
                {
                    column_info[i].type = Int64;
                    column_info[i].size = 8;
                    break;
                }
                // floating point types
                case MYSQL_TYPE_DOUBLE:
                {
                    column_info[i].type = Double;
                    column_info[i].size = 8;
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
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_TINY_BLOB:
                {
                    column_info[i].type = Variable;
                    break;
                }
                // date and time
                case MYSQL_TYPE_DATETIME:
                {
                    column_info[i].type = DateTime;
                    column_info[i].size = 8;
                    break;
                }
                case MYSQL_TYPE_DATE:
                {
                    column_info[i].type = Date;
                    column_info[i].size = 4;
                    break;
                }
                case MYSQL_TYPE_TIME:
                {
                    column_info[i].type = Time;
                    column_info[i].size = 8;
                    break;
                }
                default:
                    column_info[i].type = Unsupported;
                    break;
            }
        }
    }

    void
    ShowColumnsToRowSchemaHelper(
        std::string explainStr,
        const std::vector<FieldDefSplit>& fieldDefs,
        RowSchema* schema /*out*/)
    {
        int size_in_char, char_size;
        std::string collation_part;
        schema->numColumns = fieldDefs.size();
        schema->ColumnInfo = new Column[schema->numColumns];
        for (uint64_t i = 0; i < fieldDefs.size(); ++i)
        {
            schema->ColumnInfo[i].name = strndup(
                explainStr.c_str() + fieldDefs[i].name_start,
                fieldDefs[i].name_end - fieldDefs[i].name_start + 1);
            auto col_type =
                explainStr.substr(fieldDefs[i].type_start, fieldDefs[i].type_end - fieldDefs[i].type_start + 1);
            std::transform(col_type.begin(), col_type.end(), col_type.begin(), ::tolower);
            // currently we don't support unsigned integers. If support will be needed,
            // one will need to check for 'unsigned' in
            // explainStr.substr(fieldDefs[i].type_end, fieldDefs[i].field_end - fieldDefs[i].type_end + 1)
            if (!col_type.compare(0, 6, "bigint", 0, 6))
            {
                schema->ColumnInfo[i].type = Int64;
                schema->ColumnInfo[i].size = 8;
                continue;
            }
            if (!col_type.compare(0, 3, "int", 0, 3))
            {
                schema->ColumnInfo[i].type = Int32;
                schema->ColumnInfo[i].size = 4;
                continue;
            }
            if (!col_type.compare(0, 6, "double", 0, 6))
            {
                schema->ColumnInfo[i].type = Double;
                schema->ColumnInfo[i].size = 8;
                continue;
            }
            if (!col_type.compare(0, 8, "datetime(6)", 0, 8))
            {
                schema->ColumnInfo[i].type = DateTime;
                schema->ColumnInfo[i].size = 8;
                continue;
            }
            if (!col_type.compare(0, 4, "date", 0, 4))
            {
                schema->ColumnInfo[i].type = Date;
                schema->ColumnInfo[i].size = 4;
                continue;
            }
            if (!col_type.compare(0, 4, "time", 0, 4))
            {
                schema->ColumnInfo[i].type = Time;
                schema->ColumnInfo[i].size = 8;

                continue;
            }
            if (!col_type.compare(0, 6, "binary", 0, 6))
            {
                schema->ColumnInfo[i].type = Fixed;
                schema->ColumnInfo[i].size = stoi(explainStr.substr(
                    fieldDefs[i].type_start + 7,
                    fieldDefs[i].type_end - fieldDefs[i].type_start - 8));
                continue;
            }
            if (!col_type.compare(0, 4, "char", 0, 4))
            {
                schema->ColumnInfo[i].type = Fixed;

                size_in_char = stoi(explainStr.substr(
                    fieldDefs[i].type_start + 5,
                    fieldDefs[i].type_end - fieldDefs[i].type_start - 6));
                collation_part =
                    explainStr.substr(fieldDefs[i].type_end, fieldDefs[i].field_end - fieldDefs[i].type_end + 1);
                std::transform(collation_part.begin(), collation_part.end(), collation_part.begin(), ::tolower);
                char_size = (collation_part.find("utf8mb4") != std::string::npos) ? 4 : 3;

                schema->ColumnInfo[i].size = size_in_char * char_size;
                continue;
            }
            if (!col_type.compare(0, 7, "varchar", 0, 7) ||
                !col_type.compare(0, 9, "varbinary", 0, 9) ||
                !col_type.compare(0, 8, "longtext", 0, 8) ||
                !col_type.compare(0, 8, "longblob", 0, 8) ||
                !col_type.compare(0, 10, "mediumblob", 0, 10) ||
                !col_type.compare(0, 4, "blob", 0, 4) ||
                !col_type.compare(0, 8, "tinyblob", 0, 8) ||
                !col_type.compare(0, 10, "mediumtext", 0, 10) ||
                !col_type.compare(0, 4, "text", 0, 4) ||
                !col_type.compare(0, 8, "tinytext", 0, 8))
            {
                schema->ColumnInfo[i].type = Variable;
                schema->ColumnInfo[i].size = 0;
                continue;
            }
            schema->ColumnInfo[i].type = Unsupported;
        }
    }
    /*
    explainStr has the form:
    ...show_columns:[`i1` bigint(20) NULL, `i2` bigint(20) NULL, `d1` double NULL, `d2` double NULL, `t1` text
    CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL, `t2` text CHARACTER SET utf8mb4 COLLATE
    utf8mb4_general_ci NULL]... We suppose that:
    - bacticks ` indicate the beginning and the end of column name, with no backticks in the name itself
    - column type is indicated after column name and a single space character
    - character set is indicated after the column type
    - for CHAR/VARCHAR(length) data type, if charset part contains substring "utf8mb4", char size is 4.
    Otherwise it is 3
    */
    void
    ExplainToRowSchema(
        std::string explainStr,
        RowSchema* rowSchema /*out*/)
    {
        std::size_t name_last_backtick;
        // TODO: PLAT-6120: make this function more robust
        auto show_columns_idx = explainStr.find(" show_columns:[");
        auto start_idx = explainStr.find('[', show_columns_idx);
        auto end_idx = explainStr.find("NULL] ", show_columns_idx);

        std::size_t field_start_idx, field_end_idx;
        field_end_idx = start_idx;
        std::vector<FieldDefSplit> field_defs;
        while (field_end_idx < end_idx)
        {
            FieldDefSplit current_field_idxs;
            current_field_idxs.name_start = field_start_idx = explainStr.find('`', field_end_idx) + 1;
            name_last_backtick = explainStr.find('`', current_field_idxs.name_start);
            // to account for backticks inside the name, we check that the next char is not
            // a backtick
            while (explainStr[name_last_backtick + 1] == '`')
            {
                name_last_backtick = explainStr.find('`', name_last_backtick + 2);
            }

            current_field_idxs.name_end = name_last_backtick - 1;

            current_field_idxs.type_start = current_field_idxs.name_end + 3;
            current_field_idxs.type_end = explainStr.find(' ', current_field_idxs.type_start);  // no space in type name

            field_end_idx = explainStr.find(',', current_field_idxs.type_end);  // no comma in charset part

            if (field_end_idx == std::string::npos)
            {
                field_end_idx = end_idx;
            }
            current_field_idxs.field_end = field_end_idx;
            field_defs.push_back(current_field_idxs);
        }
        ShowColumnsToRowSchemaHelper(explainStr, field_defs, rowSchema);
    }

    extern "C"
    {
        void RowSchemaFree(RowSchema* schema)
        {
            if (!schema)
            {
                return;
            }
            for (uint32_t i = 0; i < schema->numColumns; i++)
            {
                free(schema->ColumnInfo[i].name);
            }
            delete[] schema->ColumnInfo;
            delete schema;
            schema = nullptr;
        }

        void
        MoveChunk(
            Chunk* out,
            Chunk* in)
        {
            out->m_ptr = in->m_ptr;
            out->m_size = in->m_size;
            out->row_count = in->row_count;
            out->id = in->id;
            out->partition_id = in->partition_id;
            out->consumed_size = in->consumed_size;

            delete in;
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
    JoinColumnNames(
        const char* const* const cols,
        const int colsNum)
    {
        if (colsNum <= 0) return "";
        std::string result;
        result += QuotedName(cols[0]);
        for (int i = 1; i < colsNum; ++i)
        {
            result += ",";
            result += QuotedName(cols[i]);
        }
        return result;
    }

    std::string JoinColumnNames(const std::vector<std::string>* cols)
    {
        std::string result;
        assert (cols && cols->size() > 0);
        result += QuotedName((*cols)[0]);
        for (int i = 1; i < cols->size(); ++i)
        {
            result += ",";
            result += QuotedName((*cols)[i]);
        }
        return result;
    }

    std::string
    BuildSortKey(
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        const char* keyColumnName)
    {
        std::string result = JoinColumnNames(partitionOrderByCols, partitionOrderByColsNumber);
        bool needAddKey = true;
        for (int i = 0; i < partitionOrderByColsNumber; ++i)
        {
            if (!strcmp(keyColumnName, partitionOrderByCols[i]))
            {
                needAddKey = false;
                break;
            }
        }
        if (needAddKey)
        {
            if (result.size() > 0) result += ",";
            result += QuotedName(keyColumnName);
        }
        return result;
    }

    std::string
    PartitionBy(
        const char* const* const partitionByCols,
        const int partitionByColsNumber,
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        const char* keyColumnName,
        TableType tableType)
    {
        std::string result;
        if (tableType != TableType::RegularTable)
        {
            if (partitionByColsNumber <= 0 && partitionOrderByColsNumber <= 0)
            {
                return "";
            }
            // result table syntax
            if (partitionByColsNumber > 0)
            {
                result = " PARTITION BY (" + JoinColumnNames(partitionByCols, partitionByColsNumber) + ")";
            }
            if (partitionOrderByColsNumber > 0)
            {
                result += " WITH (PARTITION_ORDER_BY = (" +
                          JoinColumnNames(partitionOrderByCols, partitionOrderByColsNumber) + "))";
            }
            return result;
        }
        // now we are in tableType == RegularTable case, so columnstore syntax is applied
        if (partitionByColsNumber > 0)
        {
            result += "SHARD KEY(" + JoinColumnNames(partitionByCols, partitionByColsNumber) + ")";
        }
        if (result.size())
        {
            result += ",";
        }
        result += " SORT KEY(" + BuildSortKey(partitionOrderByCols, partitionOrderByColsNumber, keyColumnName) + ")";
        return "(" + result + ")";
    }

    std::string
    MakeCreateTableQuery(
        const char* resultTableName,
        const char* selectQuery,
        const char* keyColumnName,
        TableType tableType,
        const char* const* const partitionByCols,
        const int partitionByColsNumber,
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber)
    {
        std::string resultQuery = "CREATE ";

        switch (tableType)
        {
            case TableType::AggResultTableMaterialized:
                resultQuery += "MATERIALIZED RESULT TABLE ";
                break;
            case TableType::AggResultTable:
                resultQuery += "RESULT TABLE ";
                break;
            case TableType::RegularTable:
                resultQuery += "TABLE ";
                break;
            default:
                assert(false);
                return "";
        }

        resultQuery += QuotedName(resultTableName);
        resultQuery += PartitionBy(
            partitionByCols,
            partitionByColsNumber,
            partitionOrderByCols,
            partitionOrderByColsNumber,
            keyColumnName,
            tableType);
        resultQuery += " AS ";
        resultQuery += selectQuery;
        return resultQuery;
    }

    std::string MakeDropResultQuery(const char* resultTableName)
    {
        return "DROP RESULT TABLE " + QuotedName(resultTableName);
    }

    std::string MakeDropTableQuery(const char* resultTableName)
    {
        return "DROP TABLE IF EXISTS " + QuotedName(resultTableName);
    }

    std::string
    MakeReadResultTableQuery(
        const char* resultTableName,
        uint32_t partition)
    {
        std::string resultQuery = "SELECT * FROM ::";

        resultQuery += QuotedName(resultTableName);
        resultQuery += " WHERE partition_id() = " + std::to_string(partition);
        return resultQuery;
    }

    std::string
    MakeReadColumnStoreTableQuery(
        const char* tableName,
        const char* keyColumnName,
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        uint32_t partition)
    {
        std::string resultQuery = "SELECT * FROM " + QuotedName(tableName);
        resultQuery += " WHERE partition_id() = " + std::to_string(partition);
        resultQuery += " ORDER BY " + BuildSortKey(partitionOrderByCols, partitionOrderByColsNumber, keyColumnName);

        return resultQuery;
    }

    std::string
    MakeReadOriginalTableQuery(
        const char* query,
        const std::vector<std::string>* columnstoreFullSortKey,
        const char* const* const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        uint32_t partition)
    {
        std::string resultQuery = "SELECT * FROM (" + std::string(query) + ")";
        resultQuery += " WHERE partition_id() = " + std::to_string(partition);
        // we pass columnstoreFullSortKey from multi pass queue,
        // streaming queue sets the pointer to NULL
        if (columnstoreFullSortKey && columnstoreFullSortKey->size() > 0)
        {
            resultQuery += " ORDER BY " + JoinColumnNames(columnstoreFullSortKey);
        }
        else
        {
            if (partitionOrderByColsNumber > 0)
            {
                resultQuery += " ORDER BY " + JoinColumnNames(partitionOrderByCols, partitionOrderByColsNumber);
            }
        }
        return resultQuery;
    }

    std::string
    MakePointInTimeQuery(
        const std::string& table,
        const std::string& selectQuery,
        const std::string& keyColumnName,
        int partition_id,
        int64_t row_id,
        bool is_result_table)
    {
        // we set selectQuery to non-empty string when ReadTypeOriginalTable is used
        if (selectQuery != "")
        {
            std::string resultQuery = "SELECT * FROM (" + selectQuery + ")";
            resultQuery += " WHERE partition_id() = " + std::to_string(partition_id);
            resultQuery += " AND " + keyColumnName + "=" + std::to_string(row_id);
            return resultQuery;
        }
        if (is_result_table)
        {
            std::string resultQuery = "SELECT * FROM ::" + QuotedName(table);
            resultQuery += " WHERE partition_id() = " + std::to_string(partition_id);
            resultQuery += " AND partition_row_id() = " + std::to_string(row_id);
            return resultQuery;
        }
        else
        {
            std::string resultQuery = "SELECT * FROM " + QuotedName(table);
            resultQuery += " WHERE partition_id() = " + std::to_string(partition_id);
            resultQuery += " AND " + QuotedName(keyColumnName) + "=" + std::to_string(row_id);
            return resultQuery;
        }
    }

    std::string MakeLoadDataQuery(const std::string& tableName)
    {
        return "LOAD DATA LOCAL INFILE 'placeholder' INTO TABLE " + QuotedName(tableName) +
               "FIELDS OPTIONALLY ENCLOSED BY '\"'";
    }

    std::string MakeSelectQueryMeta(const std::string& tableName)
    {
        return "SELECT * FROM " + QuotedName(tableName) + " WHERE 1 = 0";
    }

    std::string MakeExplainCreateResultTableQuery(const char* selectQuery)
    {
        return "EXPLAIN EXTENDED CREATE RESULT TABLE tmp AS " + std::string(selectQuery);
    }

    // TYPE, IP_ADDR, PORT, EXTERNAL_HOST, EXTERNAL_PORT
    std::string MakeGetAggregatorsQuery()
    {
        return "SELECT "
               "TYPE, IP_ADDR, PORT, EXTERNAL_HOST, EXTERNAL_PORT "
               "FROM information_schema.mv_nodes "
               "WHERE TYPE != 'LEAF' "
               "ORDER BY IP_ADDR, PORT, EXTERNAL_HOST, EXTERNAL_PORT";
    }
}  // namespace sql
