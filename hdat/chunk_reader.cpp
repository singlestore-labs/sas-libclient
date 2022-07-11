#include <math.h>
#include <sys/mman.h>

#include "hdat/chunk_reader.hpp"
#include "utils.hpp"

bool
SuperChunkReader::ReadFloat(
    double* out,
    bool* isnull)
{
    if (!CanRead(m_current_chunk->Offset(), 8))
    {
        m_error = "no space available to read Float";
        return false;
    }

    m_current_chunk->Read8(out);
    *isnull = isnan(*out);
    return true;
}

bool
SuperChunkReader::ReadInt64(
    int64_t* out,
    bool* isnull)
{
    if (!CanRead(m_current_chunk->Offset(), 8))
    {
        m_error = "no space available to read Int64";
        return false;
    }

    m_current_chunk->Read8(out);
    *isnull = (*out == int64Null);
    return true;
}

bool
SuperChunkReader::ReadInt32(
    int32_t* out,
    bool* isnull)
{
    if (!CanRead(m_current_chunk->Offset(), 8))
    {
        m_error = "no space available to read Int32";
        return false;
    }
    m_current_chunk->Read4(out);
    m_current_chunk->Pad(4, false, ' ');

    *isnull = (*out == int32Null);
    return true;
}

bool
SuperChunkReader::ReadVariable(
    const char** out,
    uint64_t* len,
    bool* isnull)
{
    if (!CanRead(m_current_chunk->Offset(), 16))
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

    if (!CanRead(offset, *len))
    {
        m_error = "no space available to read Variable";
        return false;
    }

    *isnull = false;
    *out = m_current_chunk->PtrAt(offset);

    return true;
}

bool
SuperChunkReader::ReadFixed(
    const char** out,
    const uint64_t len,
    bool* isnull)
{
    if (len == 0)
    {
        *isnull = true;
        return true;
    }
    if (!m_current_chunk->ReadAligned8(out, len)) return false;
    *isnull = utils::IsNullBuffer(*out, len);
    return true;
}

extern "C"
{
    SuperChunkReader*
    CreateReader(
        Chunk* chunk,
        RowSchema* schema,
        S2ErrorCallback* cb)
    {
        if (!chunk)
        {
            cb->setError(cb, S2C_ERROR_INV_ARG, "NULL pointer passed as Chunk*", S2C_SEVERITY_ERROR);
            return nullptr;
        }
        try
        {
            return new SuperChunkReader(chunk, schema);
        }
        catch (std::bad_alloc& e)
        {
            cb->setError(
                cb,
                S2C_ERROR_MEMORY_ALLOCATION,
                "Memory allocation error in CreateWriter",
                S2C_SEVERITY_ERROR);
            return nullptr;
        }
    }

    void ReaderFree(SuperChunkReader* reader)
    {
        delete reader;
    }

    void
    ResetReader(
        SuperChunkReader* reader,
        Chunk* chunk,
        RowSchema* schema)
    {
        reader->Reset(chunk, schema);
    }

    bool
    ReadInt64(
        SuperChunkReader* reader,
        int64_t* val,
        bool* is_null /*out*/)
    {
        return reader->ReadInt64(val, is_null);
    }

    bool
    ReadInt32(
        SuperChunkReader* reader,
        int32_t* val,
        bool* is_null /*out*/)
    {
        return reader->ReadInt32(val, is_null);
    }

    bool
    ReadFloat(
        SuperChunkReader* reader,
        double* val,
        bool* is_null /*out*/)
    {
        return reader->ReadFloat(val, is_null);
    }

    bool
    ReadFixed(
        SuperChunkReader* reader,
        const char** val,
        const uint64_t len,
        bool* is_null /*out*/)
    {
        return reader->ReadFixed(val, len, is_null);
    }

    bool
    ReadVariable(
        SuperChunkReader* reader,
        const char** val,
        uint64_t* len,
        bool* is_null /*out*/)
    {
        return reader->ReadVariable(val, len, is_null);
    }
}
