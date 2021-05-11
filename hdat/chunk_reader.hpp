#ifndef HDAT_CHUNK_READER_HPP
#define HDAT_CHUNK_READER_HPP

#include <vector>
#include <type_traits>
#include <memory>

#include "hdat/super_chunk.hpp"
#include "chunk_extern.h"

class SuperChunkReader
{
  public:
    SuperChunkReader(Chunk *chunk)
    {
        m_current_chunk = new SuperChunk(chunk);
    };

    // disallow evil constructors
    SuperChunkReader(const SuperChunkReader &) = delete;
    void operator=(const SuperChunkReader &) = delete;

    // read operations
    inline bool CanRead(uint64_t len)
    {
        return m_current_chunk->Offset() + len < m_current_chunk->Size();
    }

    bool
    ReadFloat(
        double *out,
        bool *isnull);

    bool
    ReadInteger(
        int64_t *out,
        bool *isnull);

    bool
    ReadVariable(
        const char **out,
        uint64_t *len,
        bool *isnull);

    void ReadRows();

    std::string get_error()
    {
        return m_error;
    }

  private:
    // m_current_chunk is a pointer to the current chunk we are reading from
    SuperChunk *m_current_chunk;

    std::string m_error;
};

#endif  // HDAT_CHUNK_READER_HPP
