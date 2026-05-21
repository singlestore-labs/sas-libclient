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

#ifndef STREAMING_QUEUE_HPP
#define STREAMING_QUEUE_HPP

#include <vector>

#include "queue/chunk_queue.hpp"
#include "result_table_reader.hpp"
#include "queue/thread_safe_queue.hpp"
#include "queue/thread_safe_simple_queue.hpp"

class StreamingQueue : public ChunkQueue
{
  private:
    StreamingQueue()
        {};

  public:
    ~StreamingQueue();

    // CreateChunkQueue creates a StreamingQueue object,
    // creates Readers and starts them.
    // - cpapacity is the maximum number of chunks that can be stored in the queue
    // - chunkSize is the size of one Chunk in bytes
    static std::unique_ptr<StreamingQueue>
    CreateChunkQueue(
        S2Client *client,
        const char *resultTableName,
        const char *selectQuery,
        const char *sourceTable,
        const char *keyColumnName,
        ParallelReadType readType,
        const char *const *const partitionOrderByCols,
        const int partitionOrderByColsNumber,
        uint32_t capacity,
        uint64_t chunkSize,
        int nReaderThreads,
        bool doesParallelRead,
        bool usePreparedProtocol,
        S2ErrorCallback *cb);

    Chunk *
    GetById(
        int partiotionId,
        int chunkId,
        S2ClientError &error);

    Chunk *
    GetSingleRow(
        uint32_t partitionId,
        int64_t rowId,
        int threadId,
        S2ClientError &error);

    Chunk *
    GetMultipleRows(
        uint64_t chunkSize,
        uint32_t partitionId,
        int64_t *rowIds,
        int64_t rowIdsNum,
        int threadId,
        S2ClientError &error);
};

#endif  // STREAMING_QUEUE_HPP
