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
