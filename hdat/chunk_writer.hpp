#ifndef HDAT_CHUNK_WRITER_HPP
#define HDAT_CHUNK_WRITER_HPP

#include <vector>
#include <type_traits>
#include <memory>

#include "chunk_extern.h"

#include "hdat/super_chunk.hpp"

class SuperChunkWriter
{
  public:
    SuperChunkWriter() = default;

    SuperChunkWriter(
        Chunk *chunk,
        RowSchema *rowSchema)
    {
        Reset(chunk, rowSchema);
    }

    // disallow evil constructors
    SuperChunkWriter(const SuperChunkWriter &) = delete;
    void operator=(const SuperChunkWriter &) = delete;

    void
    Reset(
        Chunk *chunk,
        RowSchema *rowSchema);

    bool HasEnoughSpace(const uint64_t requestedSize);

    void
    WriteFixed(
        const void *val,
        const int64_t data_size,
        const int64_t field_size);

    void
    WriteVariable(
        const void *val,
        const uint64_t len);

    void WriteInt64(const void *val);

    void WriteInt32(const void *val);

    void
    WriteDouble(
        const void *val,
        const uint64_t len);

    inline void WriteInt64Null();

    inline void WriteInt32Null();

    inline void WriteDoubleNull();

    inline void WriteFixedNull(const int len);

    inline void WriteVariableNull();

    template<typename T>
    inline void WriteInt64Numeric(const T val);

    template<typename T>
    inline void WriteInt32Numeric(const T val);

    template<typename T>
    inline void WriteFloatNumeric(const T val);

    bool
    WriteRow(
        MYSQL_ROW row,
        unsigned long *lengths);

    // WriteRowEnd must be called after writing the values of each row.
    // When values are written to the chunk during parallel read, it's called from WriteRow
    void WriteRowEnd();

    void WriteRowCount();

    uint64_t RowCount()
    {
        return m_current_chunk->RowCount();
    }

  private:
    inline void RecordColumn()
    {
        // if we are still outputting the first row that this writer has ever seen
        // we need to update our metadata regarding column count and variable offsets
        if (m_row_fixed_size == 0)
        {
            m_column_count++;
        }

        // keep track of the number of columns we have
        // written for the current row
        m_row_column_count++;
    }

    uint64_t m_chunk_size;

    // m_row_fixed_size is the length of each row in the fixed length section
    // this value is not computed until the first row has been written
    // all chunks produced by this Writer will have the same schema so we only
    // update this value once
    uint64_t m_row_fixed_size;

    // m_column_count keeps track of the total number of columns in each row
    // all chunks produced by this Writer will have the same schema so we only
    // update this value once
    uint32_t m_column_count;

    // m_current_chunk is a pointer to the current chunk we are writing to
    std::unique_ptr<SuperChunk> m_current_chunk;

    // m_row_offset is the offset of the current row's fixed data in the chunk
    uint64_t m_row_offset;

    // m_row_column_count keeps track of the number of columns written to the current row
    uint32_t m_row_column_count;

    // m_variable_offset is the offset of the variable length section in the chunk
    // each time we write a new variable length value this offset will *decrease*
    // since we write each variable value in reverse from the end of the chunk
    uint64_t m_variable_offset;

    RowSchema *m_row_schema;
};

extern "C"
{
    SuperChunkWriter *
    CreateWriter(
        Chunk *chunk,
        RowSchema *schema,
        S2ErrorCallback *cb);

    void
    ResetWriter(
        SuperChunkWriter *writer,
        Chunk *chunk,
        RowSchema *schema);

    void WriterFree(SuperChunkWriter *writer);

    bool
    WriteInt64(
        SuperChunkWriter *writer,
        int64_t val);

    bool
    WriteDouble(
        SuperChunkWriter *writer,
        double val);

    bool
    WriteFixed(
        SuperChunkWriter *writer,
        const void *val,
        const int64_t data_size,
        const int64_t field_size);

    bool
    WriteVariable(
        SuperChunkWriter *writer,
        const void *val,
        uint64_t len);

    void WriteRowEnd(SuperChunkWriter *writer);
}

#endif  // HDAT_CHUNK_WRITER_HPP
