#ifndef HDAT_SUPER_CHUNK_HPP
#define HDAT_SUPER_CHUNK_HPP

#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>

#include <cstdint>
#include <cassert>
#include <memory>

#include "chunk_extern.h"
#include "hdat/common.hpp"

class SuperChunk
{
  public:
    explicit SuperChunk(Chunk *chunk)
    {
        m_chunk = chunk;
        m_offset = 0;
    }

    // disallow evil constructors
    SuperChunk(const SuperChunk &) = delete;

    void operator=(const SuperChunk &) = delete;

    // metadata methods
    inline uint64_t Offset()
    {
        return m_offset;
    }

    inline uint64_t Size()
    {
        return m_chunk->m_size;
    }

    uint64_t RowCount()
    {
        return m_chunk->row_count;
    }

    void IncrRowCount()
    {
        m_chunk->row_count++;
    }

    // write methods
    void
    WriteAt(
        uint64_t offset,
        const void *src,
        uint64_t len)
    {
        if (len == 0)
        {
            return;
        }

        assert(src);
        assert(offset < Size());
        assert(offset + len <= Size());

        memcpy((void *)(m_chunk->m_ptr + offset), src, len);
        m_chunk->consumed_size += len;
    }

    void
    Write(
        const void *src,
        uint64_t len)
    {
        WriteAt(m_offset, src, len);
        m_offset += len;
    }

    void
    Pad(
        uint64_t len,
        bool fill_with_zero)
    {
        if (fill_with_zero)
        {
            for (int i = 0; i < len; ++i)
            {
                *(m_chunk->m_ptr + m_offset + i) = '\0';
            }
        }
        m_offset += len;
    }

    inline void Write8(const void *src)
    {
        Write(src, 8);
    }

    inline void Write4(const void *src)
    {
        Write(src, 4);
    }

    template<typename T>
    inline void Write8Typed(const T val)
    {
        static_assert(sizeof(T) == 8, "Write8Typed can only write 8 bytes");
        *((T *)(m_chunk->m_ptr + m_offset)) = val;
        m_chunk->consumed_size += 8;
        m_offset += 8;
    }

    template<typename T>
    inline void Write4Typed(const T val)
    {
        static_assert(sizeof(T) == 4, "Write8Typed can only write 4 bytes");
        *((T *)(m_chunk->m_ptr + m_offset)) = val;
        m_chunk->consumed_size += 8;
        m_offset += 4;
    }

    // read methods
    template<typename T>
    inline void Read8(T *out)
    {
        static_assert(sizeof(T) == 8, "Read8 can only read 8 bytes");
        *out = *(T *)(m_chunk->m_ptr + m_offset);
        m_offset += 8;
    }

    template<typename T>
    inline void Read4(T *out)
    {
        static_assert(sizeof(T) == 4, "Read4 can only read 4 bytes");
        *out = *(T *)(m_chunk->m_ptr + m_offset);
        m_offset += 4;
    }

    bool
    ReadAligned8(
        const char **out,
        uint64_t len)
    {
        uint64_t alignedLen = sizeofAligned8(len);
        if (Size() - m_offset < len)
        {
            return false;
        }
        *out = PtrAt(m_offset);
        // increment the offset to the next alignment boundary
        m_offset += alignedLen;
        return true;
    }

    char *PtrAt(uint64_t offset)
    {
        assert(offset < Size());
        return m_chunk->m_ptr + offset;
    }

  private:
    template<typename T>
    T ReadAt(const uint64_t offset)
    {
        if (Size() == offset)
        {
            return 0;
        }
        // fail if we are reading more bytes than are available
        assert(sizeof(T) < Size() - offset);
        // read the value
        return *reinterpret_cast<T *>((void *)(m_chunk->m_ptr + offset));
    }

    Chunk *m_chunk;
    uint64_t m_offset;
};

#endif  // HDAT_SUPER_CHUNK_HPP
