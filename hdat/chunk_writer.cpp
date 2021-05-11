#include <sys/mman.h>
#include <string.h>
#include <string>
#include <memory>

#include "hdat/chunk_writer.hpp"
#include "hdat/common.hpp"

bool
SuperChunkWriter::WriteFixed(
    const void *val,
    uint64_t len)
{
    RecordColumn();

    // WriteFixed can just copy bytes directly since CheckFixedSpace will
    // have already ensured that we don't run out of space
    return m_current_chunk->WriteAligned8(val, len) >= 0;
}

bool
SuperChunkWriter::WriteInteger(
    const void *val,
    uint64_t len)
{
    RecordColumn();
    if (len == super_chunk::defaultAlignmentSize)
    {
        return Write8(val);
    }
    int64_t int8Bytes = 0;
    // TODO: check if it works for 4 and 2 byte integers
    memcpy(&int8Bytes, (void *)val, len);

    return Write8(&int8Bytes);
}

bool
SuperChunkWriter::WriteFloat(
    const void *val,
    uint64_t len)
{
    RecordColumn();
    if (len == super_chunk::defaultAlignmentSize)
    {
        return Write8(val);
    }
    double double8Bytes = 0;
    memcpy(&double8Bytes, (void *)val, len);
    // TODO: check if memcpy can be replaced by more robust method

    return Write8(&double8Bytes);
}

bool
SuperChunkWriter::WriteVariable(
    const void *val,
    uint64_t len)
{
    RecordColumn();

    // update m_variable_offset to store the new value
    m_variable_offset -= len;

    // write offset and length
    if (!Write8(&m_variable_offset))
    {
        return false;
    }
    if (!Write8(&len))
    {
        return false;
    }

    // write the value into the variable length region if it's nonempty
    if (len > 0)
    {
        if (m_current_chunk->WriteAt(m_variable_offset, val, len) < 0)
        {
            return false;
        }
    }

    return true;
}

bool SuperChunkWriter::CheckSpace(uint64_t requestedSize)
{
    assert(m_row_offset <= m_variable_offset);
    if (m_variable_offset - m_row_offset < requestedSize)
    {
        // not enough space
        return false;
    }

    return true;
}

bool SuperChunkWriter::WriteRowEnd()
{
    // if we haven't calculated the fixed size of each row, do so now
    if (m_row_fixed_size == 0)
    {
        assert(m_row_offset == 0);  // should be the first row
        m_row_fixed_size = m_current_chunk->Offset();
    }
    // update the row count
    m_row_count += 1;
    // update the current row offset for the next row
    m_row_offset = m_current_chunk->Offset();
    // reset the column counter for the next row
    m_row_column_count = 0;

    return true;
}

bool
SuperChunkWriter::WriteRow(
    MYSQL_ROW row,
    unsigned long *lengths)
{
    uint64_t total_size = super_chunk::rowSize(m_row_schema, lengths);

    if (!CheckSpace(total_size))
    {
        if (!m_row_count)
        {
            throw std::invalid_argument(
                "chunk size " + std::to_string(this->m_chunk_size) + " is too small. Required at least " +
                std::to_string(total_size));
        }
        return false;
    }
    for (int i = 0; i < this->m_row_schema->numColumns; ++i)
    {
        auto column_type = this->m_row_schema->ColumnInfo[i];

        auto column_length = lengths[i];

        auto column_value = row[i];

        switch (column_type.type)
        {
            case BigInt:
            {
                WriteInteger(column_value, column_length);
                break;
            }
            case Double:
            {
                WriteFloat(column_value, column_length);
                break;
            }
            case Fixed:
            {
                WriteFixed(column_value, column_length);
                break;
            }
            case Variable:
            {
                WriteVariable(column_value, column_length);
                break;
            }
            default:
            {
                assert(false);
            }
        }
    }

    WriteRowEnd();

    return true;
}
