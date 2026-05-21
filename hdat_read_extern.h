/*
 * Copyright 2021-2026 SingleStore, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
