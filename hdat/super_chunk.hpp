#ifndef HDAT_SUPER_CHUNK_HPP
#define HDAT_SUPER_CHUNK_HPP

#include <unistd.h>
#include <stdint.h>

#include <cstdint>
#include <cassert>
#include <memory>

#include "chunk_extern.h"

class SuperChunk
{
  public:
    SuperChunk(
        char *ptr,
        uint64_t size)
        :
        m_size(size),
        m_ptr(ptr),
        m_offset(0)
            {};

    explicit SuperChunk(Chunk *chunk)
    {
        m_ptr = chunk->m_ptr;
        m_size = chunk->m_size;
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
        return m_size;
    }

    template<typename T>
    inline void Write8(const T *src)
    {
        static_assert(sizeof(T) == 8, "Write8 can only write 8 bytes");
        *((T *)(m_ptr + m_offset)) = *src;
        m_offset += 8;
    }

    inline void Write8(const void *src)
    {
        Write(src, 8);
    }

    int64_t
    Write(
        const void *src,
        uint64_t len);

    int64_t
    WriteAt(
        uint64_t offset,
        const void *src,
        uint64_t len);

    int64_t
    WriteAligned8(
        const void *src,
        uint64_t len);

    template<typename T>
    inline void Read8(T *out)
    {
        static_assert(sizeof(T) == 8, "Read8 can only read 8 bytes");
        *out = *(T *)(m_ptr + m_offset);
        m_offset += 8;
    }

    int64_t
    Read(
        void *out,
        uint64_t len);

    int64_t
    ReadAt(
        uint64_t offset,
        void *out,
        uint64_t len);

    template<typename T>
    T ReadAt(const uint64_t offset)
    {
        if (m_size == offset)
        {
            return 0;
        }
        // fail if we are reading more bytes than are available
        assert(sizeof(T) < m_size - offset);
        // read the value
        return *reinterpret_cast<T *>((void *)(m_ptr + offset));
    }

    char *PtrAt(uint64_t offset);
    uint64_t m_size;

  private:
    char *m_ptr;
    uint64_t m_offset;
};

#endif  // HDAT_SUPER_CHUNK_HPP
