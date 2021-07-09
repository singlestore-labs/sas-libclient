#include <sys/mman.h>
#include <string.h>
#include <string>
#include <memory>

#include "hdat/chunk_writer.hpp"
#include "hdat/common.hpp"

#include "s2_client_error.hpp"

bool
SuperChunkWriter::WriteFixed(
    const void *val,
    uint64_t len)
{
    RecordColumn();

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
    switch(len)
    {
        case 4:
        {
            int32_t int4 = 0;
            memcpy(&int4, (void *)val, len);
            int8Bytes = int4;
            break;
        }
        case 2:
        {
            int16_t int2 = 0;
            memcpy(&int2, (void *)val, len);
            int8Bytes = int2;
            break;
        }
        case 1:
        {
            int8_t int1 = 0;
            memcpy(&int1, (void *)val, len);
            int8Bytes = int1;
            break;
        }
        default:
        {
            return false;
        }
    }

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
    if (len == 4)
    {
        float val4;
        double val8;
        memcpy(&val4, (void *)val, len);
        val8 = val4;
        return Write8(&val8);
    }
    return false;
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

bool SuperChunkWriter::HasEnoughSpace(uint64_t requestedSize)
{
    assert(m_row_offset <= m_variable_offset);
    if (m_variable_offset - m_row_offset < requestedSize)
    {
        // not enough space
        return false;
    }

    return true;
}

void SuperChunkWriter::WriteRowEnd()
{
    // if we haven't calculated the fixed size of each row, do so now
    if (m_row_fixed_size == 0)
    {
        assert(m_row_offset == 0);  // should be the first row
        m_row_fixed_size = m_current_chunk->Offset();
    }
    // update the row count
    m_current_chunk->IncrRowCount();
    // update the current row offset for the next row
    m_row_offset = m_current_chunk->Offset();
    // reset the column counter for the next row
    m_row_column_count = 0;
}

bool
SuperChunkWriter::WriteRow(
    MYSQL_ROW row,
    unsigned long *lengths)
{
    uint64_t total_size = super_chunk::rowSize(m_row_schema, lengths);

    if (!HasEnoughSpace(total_size))
    {
        if (!RowCount())
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
                throw S2ClientError(
                    S2C_ERROR_UNS_DATA_TYPE,
                    "Trying to write unsupported data type to the chunk. Aborted.");
            }
        }
    }

    WriteRowEnd();

    return true;
}

extern "C"
{
    SuperChunkWriter* CreateWriter(Chunk* chunk, RowSchema* schema, S2ErrorCallback* cb)
    {
        if (!chunk || !schema)
        {
            cb->setError(cb, S2C_ERROR_INV_ARG, "NULL pointer passed as Chunk* or RowSchema*");
        }
        try
        {
            return new SuperChunkWriter(chunk, schema);
        }
        catch (std::bad_alloc &e)
        {
            cb->setError(cb, S2C_ERROR_MEMORY_ALLOCATION, "Memory allocation error in CreateWriter");
        }
    }

    void WriterFree(SuperChunkWriter* writer)
    {
        delete writer;
    }

    void ResetWriter(SuperChunkWriter* writer, Chunk* chunk, RowSchema* schema)
    {
        writer->Reset(chunk, schema);
    }

    bool WriteInteger(SuperChunkWriter* writer, int64_t val)
    {
        if(!writer->HasEnoughSpace(super_chunk::defaultAlignmentSize))
        {
            return false;
        }
        return writer->WriteIntegerNumeric(val);
    }

    bool WriteFloat(SuperChunkWriter* writer, double val)
    {
        if(!writer->HasEnoughSpace(super_chunk::defaultAlignmentSize))
        {
            return false;
        }
        return writer->WriteFloatNumeric(val);
    }

    bool WriteFixed(SuperChunkWriter* writer, const void *val, uint64_t len)
    {
        if(!writer->HasEnoughSpace(super_chunk::sizeofAligned8(len)))
        {
            return false;
        }
        return writer->WriteFixed(val, len);
    }

    bool WriteVariable(SuperChunkWriter* writer, const void *val, uint64_t len)
    {
        int space_needed = len + 2 * super_chunk::defaultAlignmentSize;
        if(!writer->HasEnoughSpace(space_needed))
        {
            return false;
        }
        return writer->WriteVariable(val, len);
    }

    void WriteRowEnd(SuperChunkWriter* writer)
    {
        writer->WriteRowEnd();
    }
}
