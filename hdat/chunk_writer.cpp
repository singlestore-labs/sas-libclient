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

void
SuperChunkWriter::WriteFixed(
    const void *val,
    const int64_t data_size,
    const int64_t field_size,
    const bool pad_with_zero)
{
    RecordColumn();
    m_current_chunk->Write(val, data_size);
    uint64_t alignedLen = sizeofAligned8(field_size);
    m_current_chunk->Pad(alignedLen - data_size, true, pad_with_zero ? '\0' : ' ');
}

void SuperChunkWriter::WriteInt64(const void *val)
{
    RecordColumn();
    m_current_chunk->Write8(val);
}

void SuperChunkWriter::WriteInt32(const void *val)
{
    RecordColumn();

    m_current_chunk->Write4(val);
    m_current_chunk->Pad(4, false, ' ');
}

void
SuperChunkWriter::WriteDouble(
    const void *val,
    const uint64_t len)
{
    RecordColumn();
    m_current_chunk->Write8(val);
}

void
SuperChunkWriter::WriteVariable(
    const void *val,
    const uint64_t len)
{
    RecordColumn();

    // update m_variable_offset to store the new value
    m_variable_offset -= len;

    // write offset and length
    m_current_chunk->Write8Typed(m_variable_offset);
    m_current_chunk->Write8Typed(len);

    // write the value into the variable length region if it's nonempty
    m_current_chunk->WriteAt(m_variable_offset, val, len);
}

inline void SuperChunkWriter::WriteInt64Null()
{
    RecordColumn();
    m_current_chunk->Write8Typed<int64_t>(int64Null);
}

inline void SuperChunkWriter::WriteInt32Null()
{
    RecordColumn();
    m_current_chunk->Write4Typed<int32_t>(int32Null);
    m_current_chunk->Pad(4, false, ' ');
}

inline void SuperChunkWriter::WriteDoubleNull()
{
    RecordColumn();
    m_current_chunk->Write8Typed<double>(doubleNull);
}

inline void SuperChunkWriter::WriteFixedNull(const int len)
{
    RecordColumn();
    uint64_t alignedLen = sizeofAligned8(len);
    m_current_chunk->Pad(alignedLen, true, '\0');
}

inline void SuperChunkWriter::WriteVariableNull()
{
    WriteVariable(&variableNull, 0);
}

template<typename T>
inline void SuperChunkWriter::WriteInt64Numeric(const T val)
{
    static_assert(std::is_arithmetic<T>::value, "WriteInt64Numeric requires a numeric value");

    RecordColumn();
    int64_t val8 = (int64_t)val;
    m_current_chunk->Write8Typed<int64_t>(val8);
}

template<typename T>
inline void SuperChunkWriter::WriteInt32Numeric(const T val)
{
    static_assert(std::is_arithmetic<T>::value, "WriteInt64Numeric requires a numeric value");

    RecordColumn();
    int32_t val4 = (int32_t)val;
    m_current_chunk->Write4Typed<int32_t>(val4);
    m_current_chunk->Pad(4, false, ' ');
}

template<typename T>
inline void SuperChunkWriter::WriteFloatNumeric(const T val)
{
    static_assert(std::is_arithmetic<T>::value, "WriteFloatNumeric requires a numeric value");

    RecordColumn();
    double val8 = (double)val;
    m_current_chunk->Write8Typed<double>(val8);
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
    uint64_t total_size = rowSize(m_row_schema, lengths);

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
    for (uint32_t i = 0; i < this->m_row_schema->numColumns; ++i)
    {
        const Column column_type = this->m_row_schema->ColumnInfo[i];

        const uint64_t data_length = lengths[i];

        const char *column_value = row[i];

        switch (column_type.type)
        {
            case Int64:
            {
                if (!column_value)
                {
                    WriteInt64Null();
                }
                else
                {
                    WriteInt64(column_value);
                }
                break;
            }
            case Int32:
            {
                if (!column_value)
                {
                    WriteInt32Null();
                }
                else
                {
                    WriteInt32(column_value);
                }
                break;
            }
            case DateTime:
            {
                if (!column_value)
                {
                    WriteInt64Null();
                }
                else
                {
                    WriteInt64Numeric(toDateTimeCAS(column_value));
                }
                break;
            }
            case Date:
            {
                if (!column_value)
                {
                    WriteInt32Null();
                }
                else
                {
                    WriteInt32Numeric(toDateCAS(column_value));
                }
                break;
            }
            case Time:
            {
                if (!column_value)
                {
                    WriteInt64Null();
                }
                else
                {
                    WriteInt64Numeric(toTimeCAS(column_value));
                }
                break;
            }
            case Double:
            {
                if (!column_value)
                {
                    WriteDoubleNull();
                }
                else
                {
                    WriteDouble(column_value, data_length);
                }
                break;
            }
            case Fixed:
            {
                if (!column_value)
                {
                    WriteFixedNull(column_type.size);
                }
                else
                {
                    // when we read data from db, fixed length binary data
                    // does not need padding, so we set pad_with_zero to false
                    WriteFixed(column_value, data_length, column_type.size, false);
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
                    WriteVariable(column_value, data_length);
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
            cb->setError(cb, S2C_ERROR_INV_ARG, "NULL pointer passed as Chunk* or RowSchema*", S2C_SEVERITY_ERROR);
            return nullptr;
        }
        try
        {
            return new SuperChunkWriter(chunk, schema);
        }
        catch (std::bad_alloc &e)
        {
            cb->setError(
                cb,
                S2C_ERROR_MEMORY_ALLOCATION,
                "Memory allocation error in CreateWriter",
                S2C_SEVERITY_ERROR);
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
    WriteInt64(
        SuperChunkWriter *writer,
        int64_t val)
    {
        if (!writer->HasEnoughSpace(defaultAlignmentSize))
        {
            return false;
        }
        writer->WriteInt64Numeric(val);
        return true;
    }

    bool
    WriteInt32(
        SuperChunkWriter *writer,
        int64_t val)
    {
        if (!writer->HasEnoughSpace(defaultAlignmentSize))
        {
            return false;
        }
        writer->WriteInt32Numeric(val);
        return true;
    }

    bool
    WriteDouble(
        SuperChunkWriter *writer,
        double val)
    {
        if (!writer->HasEnoughSpace(defaultAlignmentSize))
        {
            return false;
        }
        writer->WriteFloatNumeric(val);
        return true;
    }

    bool
    WriteFixed(
        SuperChunkWriter *writer,
        const void *val,
        int64_t data_size,
        int64_t field_size,
        const bool pad_with_zero)
    {
        if (!writer->HasEnoughSpace(sizeofAligned8(field_size)))
        {
            return false;
        }
        writer->WriteFixed(val, data_size, field_size, pad_with_zero);
        return true;
    }

    bool
    WriteVariable(
        SuperChunkWriter *writer,
        const void *val,
        uint64_t len)
    {
        int space_needed = len + 2 * defaultAlignmentSize;
        if (!writer->HasEnoughSpace(space_needed))
        {
            return false;
        }
        writer->WriteVariable(val, len);
        return true;
    }

    void WriteRowEnd(SuperChunkWriter *writer)
    {
        writer->WriteRowEnd();
    }
}
