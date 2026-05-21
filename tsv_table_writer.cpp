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

#include <cstring>
#include <iomanip>
#include <sstream>

#include "tsv_table_writer.hpp"
#include "hdat/common.hpp"

// ChunkToTSV writes
bool
TsvTableWriter::ChunkToTSV(
    std::unique_ptr<SuperChunkReader> &reader,
    int64_t row_count)
{
    bool is_null = 0;
    int64_t int_64_val;
    int32_t int_32_val;
    double float_val;
    const char *buf;
    uint64_t len;
    for (int64_t row_num = 0; row_num < row_count; ++row_num)
    {
        if (row_num > 0)
        {
            *m_tsv_rows << "\n";
        }
        for (uint32_t col_num = 0; col_num < reader->m_row_schema->numColumns; ++col_num)
        {
            if (col_num > 0)
            {
                *m_tsv_rows << "\t";
            }
            is_null = false;  // Reset is_null indicator
            switch (reader->m_row_schema->ColumnInfo[col_num].type)
            {
                case Int64:
                    if (!(reader->ReadInt64(&int_64_val, &is_null)))
                    {
                        return false;
                    }
                    if (!is_null)
                    {
                        *m_tsv_rows << int_64_val;
                    }
                    else
                    {
                        *m_tsv_rows << "\\N";
                    }
                    break;
                case Int32:
                    if (!(reader->ReadInt32(&int_32_val, &is_null)))
                    {
                        return false;
                    }
                    if (!is_null)
                    {
                        *m_tsv_rows << int_32_val;
                    }
                    else
                    {
                        *m_tsv_rows << "\\N";
                    }
                    break;
                case Double:
                    if (!(reader->ReadFloat(&float_val, &is_null)))
                    {
                        return false;
                    }
                    // -5.12345678907654321e-300 has length 25,
                    // exponent does not take more than 3 digits, so 30 bytes is enough
                    if (!is_null)
                    {
                        char buff[30];
                        len = sprintf(buff, "%.17e", float_val);
                        m_tsv_rows->write(buff, len);
                    }
                    else
                    {
                        *m_tsv_rows << "\\N";
                    }
                    break;
                case Variable:
                    if (!(reader->ReadVariable(&buf, &len, &is_null)))
                    {
                        return false;
                    }
                    if (!is_null)
                    {
                        *m_tsv_rows << std::quoted(std::string(buf, len), '"', '\\');
                    }
                    else
                    {
                        *m_tsv_rows << "\\N";
                    }
                    break;
                case Fixed:
                    if (!(reader->ReadFixed(&buf, reader->m_row_schema->ColumnInfo[col_num].size, &is_null)))
                    {
                        return false;
                    }
                    if (!is_null)
                    {
                        *m_tsv_rows << std::quoted(
                            std::string(buf, reader->m_row_schema->ColumnInfo[col_num].size),
                            '"',
                            '\\');
                    }
                    else
                    {
                        *m_tsv_rows << "\\N";
                    }
                    break;
                case DateTime:
                    if (!(reader->ReadInt64(&int_64_val, &is_null)))
                    {
                        return false;
                    }
                    if (!is_null)
                    {
                        *m_tsv_rows << fromDateTimeCAS(int_64_val);
                    }
                    else
                    {
                        *m_tsv_rows << "\\N";
                    }
                    break;
                case Time:
                    if (!(reader->ReadInt64(&int_64_val, &is_null)))
                    {
                        return false;
                    }
                    if (!is_null)
                    {
                        *m_tsv_rows << fromTimeCAS(int_64_val);
                    }
                    else
                    {
                        *m_tsv_rows << "\\N";
                    }
                    break;
                case Date:
                    if (!(reader->ReadInt32(&int_32_val, &is_null)))
                    {
                        return false;
                    }
                    if (!is_null)
                    {
                        *m_tsv_rows << fromDateCAS(int_32_val);
                    }
                    else
                    {
                        *m_tsv_rows << "\\N";
                    }
                    break;
                default:
                    return false;
            }
        }
    }
    return true;
}

// ss_local_infile_init copies the pointer *caller_data which is passed to
// mysql_set_local_infile_handler before the LOAD DATA statment is issued.
// This pointer will be then used in ss_local_infile_read
int
TsvTableWriter::ss_local_infile_init(
    void **ptr,
    const char *filename,
    void *caller_data)
{
    *ptr = caller_data;
    return 0;
}

// ss_local_infile_read uses *ptr passed in ss_local_infile_init.
// It reads buf_len characters from stringstream pointed by ptr
// and returns the actual number of bytes read
int
TsvTableWriter::ss_local_infile_read(
    void *ptr,
    char *buf,
    unsigned int buf_len)
{
    std::stringstream *ss = static_cast<std::stringstream *>(ptr);
    ss->read(buf, buf_len);

    if (ss->bad())
    {
        return -1;
    }
    return ss->gcount();
}

// ss_local_infile_end as long as we don't allocate additional
// memory in ss_local_infile_init
void TsvTableWriter::ss_local_infile_end(void *ptr)
{
    return;
}

// ss_local_infile_error is called to get a textual error message
// to return to the user in case any of ss_local_infile_* functions returns an error
int
TsvTableWriter::ss_local_infile_error(
    void *ptr,
    char *error_msg,
    unsigned int error_msg_len)
{
    if (ptr == nullptr)
    {
        strncpy(error_msg, "invalid *caller_data pointer", error_msg_len);
        return S2C_ERROR_INV_ARG;
    }
    std::stringstream *ss = static_cast<std::stringstream *>(ptr);

    if (ss->bad())
    {
        strncpy(error_msg, "Read/write error on i/o operation", error_msg_len);
        return S2C_IO_ERROR;
    }

    strncpy(error_msg, "Unknown error in LOAD DATA LOCAL INFILE", error_msg_len);
    return S2C_ERROR_UNKNOWN_FAILURE;
}
