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
WriteInteger(
    SuperChunkWriter* writer,
    int64_t val);

bool
WriteFloat(
    SuperChunkWriter* writer,
    double val);

bool
WriteFixed(
    SuperChunkWriter* writer,
    const void* val,
    uint64_t len);

bool
WriteVariable(
    SuperChunkWriter* writer,
    const void* val,
    uint64_t len);

void WriteRowEnd(SuperChunkWriter* writer);

#endif  // HDAT_WRITE_EXTERN_H
