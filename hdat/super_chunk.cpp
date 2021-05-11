#include <sys/mman.h>

#include <cassert>
#include <cerrno>
#include <cstring>
#include <memory>
#include <string>

#include "hdat/super_chunk.hpp"
#include "hdat/common.hpp"

int64_t
SuperChunk::Write(
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

int64_t
SuperChunk::WriteAt(
    uint64_t offset,
    const void *src,
    uint64_t len)
{
    if (len == 0)
    {
        return 0;
    }

    assert(src);
    assert(offset < m_size);
    assert(offset + len <= m_size);

    memcpy((void *)(m_ptr + offset), src, len);
    return len;
}

int64_t
SuperChunk::WriteAligned8(
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

int64_t
SuperChunk::Read(
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

int64_t
SuperChunk::ReadAt(
    uint64_t offset,
    void *out,
    uint64_t len)
{
    if (m_size == offset)
    {
        return 0;
    }
    if (m_size - offset < len)
    {
        len = m_size - offset;
    }
    memcpy(out, (void *)(m_ptr + offset), len);
    return len;
}

char *SuperChunk::PtrAt(uint64_t offset)
{
    assert(offset < m_size);
    return m_ptr + offset;
}
