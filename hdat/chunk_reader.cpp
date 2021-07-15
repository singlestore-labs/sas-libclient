#include <sys/mman.h>

#include "hdat/chunk_reader.hpp"
#include "hdat/common.hpp"

bool
SuperChunkReader::ReadFloat(
    double *out,
    bool *isnull)
{
    if (!CanRead(8))
    {
        m_error = "no space available to read Float";
        return false;
    }

    m_current_chunk->Read8(out);
    *isnull = (*out == doubleNull);
    return true;
}

bool
SuperChunkReader::ReadInteger(
    int64_t *out,
    bool *isnull)
{
    if (!CanRead(8))
    {
        m_error = "no space available to read Integer";
        return false;
    }

    m_current_chunk->Read8(out);
    *isnull = (*out == int64Null);
    return true;
}

bool
SuperChunkReader::ReadVariable(
    const char **out,
    uint64_t *len,
    bool *isnull)
{
    if (!CanRead(16))
    {
        m_error = "no space available to read Variable length and offset";
        return false;
    }

    int64_t offset;
    m_current_chunk->Read8(&offset);
    assert(offset);
    m_current_chunk->Read8(len);

    if (*len == 0)
    {
        *isnull = true;
        return true;
    }

    if (!CanRead(*len))
    {
        m_error = "no space available to read Variable";
        return false;
    }

    *isnull = false;
    *out = m_current_chunk->PtrAt(offset);

    return true;
}
