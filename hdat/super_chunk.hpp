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

    int64_t
    WriteAt(
        uint64_t offset,
        const void *src,
        uint64_t len)
    {
        if (len == 0)
        {
            return 0;
        }

        assert(src);
        assert(offset < Size());
        assert(offset + len <= Size());

        memcpy((void *)(m_chunk->m_ptr + offset), src, len);
        m_chunk->consumed_size += len;

        return len;
    }

    int64_t
    Write(
        const void *src,
        uint64_t len)
    {
        int64_t n = WriteAt(m_offset, src, len);
        if (n <= 0)
        {
            return n;
        }
        m_offset += n;
        return n;
    }

    inline void Write8(const void *src)
    {
        Write(src, 8);
    }

    template<typename T>
    inline void Write8Typed(const T val)
    {
        static_assert(sizeof(T) == 8, "Write8Typed can only write 8 bytes");
        *((T *)(m_chunk->m_ptr + m_offset)) = val;
        m_chunk->consumed_size += 8;
        m_offset += 8;
    }

    int64_t
    WriteAligned8(
        const void *src,
        uint64_t len)
    {
        int64_t n = Write(src, len);
        if (n < 0)
        {
            return n;
        }

        // increment the offset to the next alignment boundary
        uint64_t alignedLen = super_chunk::sizeofAligned8(n);
        m_offset += alignedLen - n;
        return alignedLen;
    }

    template<typename T>
    inline void Read8(T *out)
    {
        static_assert(sizeof(T) == 8, "Read8 can only read 8 bytes");
        *out = *(T *)(m_chunk->m_ptr + m_offset);
        m_offset += 8;
    }

    int64_t
    ReadAt(
        uint64_t offset,
        void *out,
        uint64_t len)
    {
        if (Size() == offset)
        {
            return 0;
        }
        if (Size() - offset < len)
        {
            len = Size() - offset;
        }
        memcpy(out, (void *)(m_chunk->m_ptr + offset), len);
        return len;
    }

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

    int64_t
    Read(
        void *out,
        uint64_t len)
    {
        int64_t n = ReadAt(m_offset, out, len);
        if (n <= 0)
        {
            return n;
        }
        m_offset += n;
        return n;
    }

    char *PtrAt(uint64_t offset)
    {
        assert(offset < Size());
        return m_chunk->m_ptr + offset;
    }

  private:
    Chunk *m_chunk;
    uint64_t m_offset;
};

#endif  // HDAT_SUPER_CHUNK_HPP
