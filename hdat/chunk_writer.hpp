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
        const int64_t field_size,
        const bool pad_with_zero);

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

    RowSchema *m_row_schema;
};

#endif  // HDAT_CHUNK_WRITER_HPP
