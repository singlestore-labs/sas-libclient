#ifndef HDAT_READ_EXTERN_H
#define HDAT_READ_EXTERN_H

#include "chunk_extern.h"

typedef struct SuperChunkReader SuperChunkReader;

SuperChunkReader*
CreateReader(
    Chunk* chunk,
    RowSchema* schema,
    S2ErrorCallback* cb);

void ReaderFree(SuperChunkReader* reader);

void
ResetReader(
    SuperChunkReader* reader,
    Chunk* chunk,
    RowSchema* schema);

bool
ReadInt64(
    SuperChunkReader* reader,
    int64_t* val /*out*/,
    bool* is_null /*out*/);

bool
ReadInt32(
    SuperChunkReader* reader,
    int32_t* val /*out*/,
    bool* is_null /*out*/);

bool
ReadFloat(
    SuperChunkReader* reader,
    double* val /*out*/,
    bool* is_null /*out*/);

bool
ReadVariable(
    SuperChunkReader* reader,
    const char** val /*out*/,
    uint64_t* len /*out*/,
    bool* is_null /*out*/);

bool
ReadFixed(
    SuperChunkReader* reader,
    const char** val /*out*/,
    const uint64_t len,
    bool* is_null /*out*/);

#endif  // HDAT_READ_EXTERN_H
