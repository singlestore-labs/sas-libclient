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

#ifndef HDAT_CHUNK_READER_HPP
#define HDAT_CHUNK_READER_HPP

#include <vector>
#include <type_traits>
#include <memory>

#include "chunk_extern.h"

#include "hdat/super_chunk.hpp"

class SuperChunkReader
{
  public:
    SuperChunkReader() = default;

    SuperChunkReader(
        Chunk *chunk,
        RowSchema *rowSchema)
    {
        Reset(chunk, rowSchema);
    }

    // disallow evil constructors
    SuperChunkReader(const SuperChunkReader &) = delete;
    void operator=(const SuperChunkReader &) = delete;

    void
    Reset(
        Chunk *chunk,
        RowSchema *rowSchema)
    {
        m_current_chunk = std::make_unique<SuperChunk>(chunk);
        m_row_schema = rowSchema;
    }

    // read operations
    inline bool
    CanRead(
        uint64_t offset,
        uint64_t len)
    {
        return offset + len <= m_current_chunk->Size();
    }

    bool
    ReadFloat(
        double *out,
        bool *isnull);

    bool
    ReadInt64(
        int64_t *out,
        bool *isnull);

    bool
    ReadInt32(
        int32_t *out,
        bool *isnull);

    bool
    ReadVariable(
        const char **out,
        uint64_t *len,
        bool *isnull);

    bool
    ReadFixed(
        const char **out,
        uint64_t len,
        bool *isnull);

    std::string GetError()
    {
        return m_error;
    }

    RowSchema *m_row_schema;

  private:
    // m_current_chunk is a pointer to the current chunk we are reading from
    std::unique_ptr<SuperChunk> m_current_chunk;

    std::string m_error;
};

#endif  // HDAT_CHUNK_READER_HPP
