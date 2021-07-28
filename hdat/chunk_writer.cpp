#include <sys/mman.h>
#include <string.h>
#include <string>
#include <memory>

#include "hdat/chunk_writer.hpp"
#include "hdat/common.hpp"

#include "s2_client_error.hpp"

void
SuperChunkWriter::Reset(
    Chunk *chunk,
    RowSchema *rowSchema)
{
    m_row_schema = rowSchema;

    m_row_fixed_size = 0;
    m_column_count = 0;
    m_current_chunk = std::make_unique<SuperChunk>(chunk);
    m_row_offset = 0;
    m_row_column_count = 0;

    m_chunk_size = chunk->m_size;
    m_variable_offset = chunk->m_size;
}

bool SuperChunkWriter::HasEnoughSpace(uint64_t requestedSize)
{
    assert(m_current_chunk->Offset() <= m_variable_offset);
    if (m_variable_offset - m_current_chunk->Offset() < requestedSize)
    {
        // not enough space
        return false;
    }

    return true;
}

bool
SuperChunkWriter::WriteFixed(
    const void* val,
    const uint64_t len)
{
    RecordColumn();

    return m_current_chunk->WriteAligned8(val, len) >= 0;
}

bool
SuperChunkWriter::WriteInteger(
    const void* val,
    const uint64_t len)
{
    RecordColumn();
    if (len == super_chunk::defaultAlignmentSize)
    {
        m_current_chunk->Write8(val);
        return true;
    }

    int64_t int8Bytes = 0;
    const uint8_t* const input = (uint8_t*) val;
    for(int i = 0; i < len; ++i)
    {
        int8Bytes = ((int8Bytes << 8) | input[i]);
    }

    m_current_chunk->Write8Typed<int64_t>(int8Bytes);
    return true;
}

bool
SuperChunkWriter::WriteFloat(
    const void* val,
    const uint64_t len)
{
    RecordColumn();
    if (len == super_chunk::defaultAlignmentSize)
    {
        m_current_chunk->Write8(val);
        return true;
    }
    if (len == 4)
    {
        float val4;
        double val8;
        memcpy(&val4, (void*)val, len);
        val8 = val4;
        m_current_chunk->Write8Typed<double>(val8);
        return true;
    }
    return false;
}

bool
SuperChunkWriter::WriteVariable(
    const void* val,
    const uint64_t len)
{
    RecordColumn();

    // update m_variable_offset to store the new value
    m_variable_offset -= len;

    // write offset and length
    m_current_chunk->Write8Typed(m_variable_offset);
    m_current_chunk->Write8Typed(len);

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

inline void SuperChunkWriter::WriteIntegerNull()
{
    RecordColumn();
    return m_current_chunk->Write8Typed<int64_t>(int64Null);
}

inline void SuperChunkWriter::WriteFloatNull()
{
    RecordColumn();
    return m_current_chunk->Write8Typed<double>(doubleNull);
}

inline bool SuperChunkWriter::WriteFixedNull(const int len)
{
    char tmp[len];
    for (int i = 0; i < len; ++i)
    {
        tmp[i] = ' ';
    }
    return WriteFixed(tmp, len);
}

inline bool SuperChunkWriter::WriteVariableNull()
{
    return WriteVariable(&variableNull, 0);
}

template<typename T>
inline bool SuperChunkWriter::WriteIntegerNumeric(const T val)
{
    static_assert(std::is_arithmetic<T>::value, "WriteIntegerNumeric requires a numeric value");

    RecordColumn();
    int64_t val8 = (int64_t)val;
    m_current_chunk->Write8Typed<int64_t>(val8);
    return true;
}

template<typename T>
inline bool SuperChunkWriter::WriteFloatNumeric(const T val)
{
    static_assert(std::is_arithmetic<T>::value, "WriteFloatNumeric requires a numeric value");

    RecordColumn();
    double val8 = (double)val;
    m_current_chunk->Write8Typed<double>(val8);
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
    unsigned long* lengths)
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
                if (!column_value)
                {
                    WriteIntegerNull();
                }
                else
                {
                    WriteInteger(column_value, column_length);
                }
                break;
            }
            case Double:
            {
                if (!column_value)
                {
                    WriteFloatNull();
                }
                else
                {
                    WriteFloat(column_value, column_length);
                }
                break;
            }
            case Fixed:
            {
                if (!column_value)
                {
                    WriteFixedNull(column_length);
                }
                else
                {
                    WriteFixed(column_value, column_length);
                }
                break;
            }
            case Variable:
            {
                if (!column_value)
                {
                    WriteVariableNull();
                }
                else
                {
                    WriteVariable(column_value, column_length);
                }
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
    SuperChunkWriter *
    CreateWriter(
        Chunk *chunk,
        RowSchema *schema,
        S2ErrorCallback *cb)
    {
        if (!chunk || !schema)
        {
            cb->setError(cb, S2C_ERROR_INV_ARG, "NULL pointer passed as Chunk* or RowSchema*");
            return nullptr;
        }
        try
        {
            return new SuperChunkWriter(chunk, schema);
        }
        catch (std::bad_alloc& e)
        {
            cb->setError(cb, S2C_ERROR_MEMORY_ALLOCATION, "Memory allocation error in CreateWriter");
            return nullptr;
        }
    }

    void WriterFree(SuperChunkWriter *writer)
    {
        delete writer;
    }

    void
    ResetWriter(
        SuperChunkWriter *writer,
        Chunk *chunk,
        RowSchema *schema)
    {
        writer->Reset(chunk, schema);
    }

    bool
    WriteInteger(
        SuperChunkWriter *writer,
        int64_t val)
    {
        if (!writer->HasEnoughSpace(super_chunk::defaultAlignmentSize))
        {
            return false;
        }
        return writer->WriteIntegerNumeric(val);
    }

    bool
    WriteFloat(
        SuperChunkWriter *writer,
        double val)
    {
        if (!writer->HasEnoughSpace(super_chunk::defaultAlignmentSize))
        {
            return false;
        }
        return writer->WriteFloatNumeric(val);
    }

    bool
    WriteFixed(
        SuperChunkWriter *writer,
        const void *val,
        uint64_t len)
    {
        if (!writer->HasEnoughSpace(super_chunk::sizeofAligned8(len)))
        {
            return false;
        }
        return writer->WriteFixed(val, len);
    }

    bool
    WriteVariable(
        SuperChunkWriter *writer,
        const void *val,
        uint64_t len)
    {
        int space_needed = len + 2 * super_chunk::defaultAlignmentSize;
        if (!writer->HasEnoughSpace(space_needed))
        {
            return false;
        }
        return writer->WriteVariable(val, len);
    }

    void WriteRowEnd(SuperChunkWriter *writer)
    {
        writer->WriteRowEnd();
    }
}
