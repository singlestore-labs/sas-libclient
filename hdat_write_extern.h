#ifndef HDAT_WRITE_EXTERN_H
#define HDAT_WRITE_EXTERN_H

#include "chunk_extern.h"

typedef struct SuperChunkWriter SuperChunkWriter;

SuperChunkWriter*
CreateWriter(
    Chunk* chunk,
    RowSchema* schema,
    S2ErrorCallback* cb);

void WriterFree(SuperChunkWriter* writer);

void
ResetWriter(
    SuperChunkWriter* writer,
    Chunk* chunk,
    RowSchema* schema);

bool
WriteInt64(
    SuperChunkWriter* writer,
    const int64_t val);

bool
WriteInt32(
    SuperChunkWriter* writer,
    const int32_t val);

bool
WriteDouble(
    SuperChunkWriter* writer,
    const double val);

bool
WriteFixed(
    SuperChunkWriter* writer,
    const void* val,
    const int64_t data_size,
    const int64_t field_size,
    const bool is_binary);

bool
WriteVariable(
    SuperChunkWriter* writer,
    const void* val,
    const uint64_t len);

void WriteRowEnd(SuperChunkWriter* writer);

#endif  // HDAT_WRITE_EXTERN_H
