#ifndef S2_CLIENT_EXTERN_H
#define S2_CLIENT_EXTERN_H

#include <stdbool.h>
#include <stdint.h>
#include "chunk_extern.h"

typedef struct S2Client S2Client;
typedef struct ChunkQueue ChunkQueue;

// S2ClientInit can be called both from controller and workers.
// Controller should put workerId = -1 to distinguish itself from the actual workers
S2Client*
S2ClientInit(
    const char* host,
    int port,
    const char* db,
    const char* user,
    const char* password,
    const char* ssl_ca,
    int numWorkers,
    int workerId,
    S2ErrorCallback* cb);

// S2ClientFree is used to delete the client object
void S2ClientFree(S2Client* client);

// parallel read operations

/*
ParallelReadInit is called once by controller to initiate the processing of `selectQuery`
- `resultTableName` is used as an identifier in ParallelReadGetQueue
- `sourceTable` is supplied if query reads rows from a single table
- `readType` indicates which mode of reading should be used
- `materialized` is set to true for multi-pass reading
- `partitionByCols` is a pointer to an array of C strings indicating column names that are used for
partitioning of the result table
- `partitionByColsNumber` is the number of columns used for partitioning,
where 0 stands for no specific partitioning
- `partitionByCols` is a pointer to an array of C strings indicating column names that are used for
ordering within partition
- `partitionByColsNumber` is the number of columns used for ordering within partition,
where 0 stands for no specific ordering
*/
ParallelReadType
ParallelReadInit(
    S2Client* client,
    const char* resultTableName,
    const char* selectQuery,
    const char* sourceTable,
    const char* keyColumnName,
    ParallelReadType readType,
    bool materialized,
    const char* const* const partitionByCols,
    int partitionByColsNumber,
    const char* const* const partitionOrderByCols,
    const int partitionOrderByColsNumber);

// ParallelReadGetQueue is called once per worker.
// ChunkQueue is a helper object
// which does all the processing internally.
// client to process `resultTableName`
// must be initialized using S2ClientInit() by the worker
// before calling this function.
// `chunkSize` is the size of the single chunk in bytes.
// `queueCapacity` is the maximum number of chunks
// that can be present in a worker's ChunkQueue at
// the same time.
// `isMultiPass` is set to true if resultTableName
// will be read in multi-pass mode
// nReaderThreads is required for multi-pass to create
// a connection to server for each CAS reader thread
ChunkQueue*
ParallelReadGetQueue(
    S2Client* client,
    const char* resultTableName,
    const char* selectQuery,
    const char* sourceTable,
    const char* keyColumnName,
    ParallelReadType readType,
    const char* const* const partitionOrderByCols,
    int partitionOrderByColsNumber,
    uint64_t chunkSize,
    int queueCapacity,
    int nReaderThreads,
    bool isMultiPass,
    S2ErrorCallback* cb);

// GetNextChunk is called in a loop until false is returned meaning all results
// have been added to the queue and retrieved from it. This is used in
// single-pass and the first pass in multi-pass
bool
GetNextChunk(
    ChunkQueue* queue,
    int readerThreadId,  // from 0 to nReaderThreads-1
    uint32_t* partitionId /*out*/,
    Chunk* chunk /*out*/,
    S2ErrorCallback* cb);

// GetChunkMulti is called in a loop in the same way as GetNextChunk.
// It is used starting from the second pass in multi-pass
bool
GetChunkMulti(
    ChunkQueue* queue,
    uint32_t partitionId,
    uint32_t chunkId,
    Chunk* chunk /*out*/,
    S2ErrorCallback* cb);

// GetChunkRow can be called after a queue for multi-pass has been created and
// chunk with id `chunkId` has been read from partition `partitionId` by thread
// number `threadId`.
// `rowWithinPartition` is the number of the row in the 'partitionId`.
// `threadId` must be between 0 and `nReaderThreads` - 1, where
// `nReaderThreads` is the value that has been passed to ParallelReadGetQueue
bool
GetChunkRow(
    ChunkQueue* queue,
    uint32_t partitionId,
    int64_t rowWithinPartition,
    int threadId,
    Chunk* chunk /*out*/,
    S2ErrorCallback* cb);

// functions to free memory

// ChunkQueueFree is used to delete the queue object
void ChunkQueueFree(ChunkQueue* queue);

// ChunkFree is used to free memory used by the chunk object
void ChunkFree(Chunk* chunk);

// ParallelReadFree is called by the controller to clean the results of query in the database
void
ParallelReadFree(
    S2Client* client,
    const char* resultTableName,
    ParallelReadType readType,
    S2ErrorCallback* cb);

// general query execution functions

// ExecuteDDLQuery is used when no results need to be fetched (DDL or DML query).
// For INSERT, UPDATE, DELETE queries, the number of affected rows is returned
int64_t
ExecuteDDLQuery(
    S2Client* client,
    const char* query,
    S2ErrorCallback* cb);

// QueryGetQueue is used to execute queries that are not supported by parallel read.
// The results are fetched in the same manner as parallel read results
ChunkQueue*
QueryGetQueue(
    S2Client* client,
    const char* query,
    uint64_t chunkSize,
    int queueCapacity,
    S2ErrorCallback* cb);

// LoadDataWrite takes a chunk along with its Row Schema
// and writes it to the table using simulated LOAD DATA LOCAL INFILE statement
void
LoadDataWrite(
    S2Client* client,
    Chunk* chunk,
    RowSchema* schema,
    const char* table,
    S2ErrorCallback* cb);

// metadata functions

// RowSchema returns the types of the columns in the table. The pointer is invalidated and
// the memory is released when the corresponding queue object is destroyed by ChunkQueueFree
RowSchema* GetRowSchema(ChunkQueue* queue);

// GetTableRowSchema returns the types of the columns in the table
RowSchema*
GetTableRowSchema(
    S2Client* client,
    const char* table,
    S2ErrorCallback* cb);

// RowSchemaFree should be called when RowSchema*
// is returned from GetTableRowSchema
void RowSchemaFree(RowSchema* schema);

// GetPartitionsNumber returns the number of partitions in the database specified in S2ClientInit().
// The total number of parallel S2 readers will be equal to this number
int GetPartitionsNumber(S2Client* client);

// For the S2Client specified by client, S2Error() returns a null-terminated string
// containing the error message for the most recently invoked API function that failed
const char* S2Error(S2Client* client);

// For the S2Client specified by client, S2Errno() returns the error code for the most recently invoked API function.
// A return value of zero means that no error occurred
int S2Errno(S2Client* client);

// S2GetClientVersion returns the client version in the format 1.2.33
const char* S2GetClientVersion();

#endif  // S2_CLIENT_EXTERN_H
