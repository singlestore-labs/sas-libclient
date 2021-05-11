## Introduction

`libs2client` is a shared library which is supposed to be used by SAS backend (called CAS).
CAS is written in C. It will use `libs2client` to connect to SingleStore(S2) database and run read/write queries. The queries will be processed in parallel in a sense that
multiple connections (1 per each aggregator) will be used to retrieve the data from different database partitions.
`s2_client_extern.h` defines the functions which provide an API to the `libs2client` library. 

## Development
In order to run tests, S2 cluster must be up and running. Access credentials are supplied in the file `test/db_creds.h`.
Then the commands from `test/prepare_s2.sql` must be run, e.g.  `mysql -u root -h 127.0.0.1 -P 3306 -pp < test/prepare_s2.sql`.
The script `build_and_test.sh` contains several functoions used to build the library and run the tests.

- to build the library, run `./build_and_test.sh lib`
- to build the library and run the main test, run `./build_and_test.sh lib read`
- some other tests are available in the script

## Workflow

CAS will have multiple *workers* (kubernetes pods supposedly colocated with S2 aggregators) and each of them will establish a connection to S2 using `S2ConnectionInit()`.

### Parallel read 
1. Each worker calls `ParallelReadInit()` function to initiate a request to S2 providing 
  - `selectQuery` - an SQL query represented as string (`char*`).
  - `resultTableName` - a name of the table which S2 will use to store the results of the query execution
2. `ParallelReadInit` performs the following steps:

    1. Transform the query to `CREATE RESULT TABLE {resultTableName} AS {selectQuery}`.
    2. Send the transformed query to S2.

3. The results of `selectQuery` are streamed to CAS using a queue `ChunkQueue` which is a thread-safe queue of `(partitionId, Chunk)` pairs.
The queue is initialized using `ParallelReadGetQueue()` function.

4. When `ParallelReadGetQueue()` has been called by a CAS worker, `libs2client` spawns a number of readers equal to `AssignedPartitions`.

    - Each reader creates its own connection to S2
    - Each reader reads from its own partition by calling `SELECT * FROM resultTableName WHERE partition_id = {assigned_partition}` on its connection
    - The rows that are streamed from `MYSQL_STMT` object are transformed from `MYSQL_BIND` to SAS-HDAT (SuperChunk) format and grouped in chunks
    - Each reader appends the results of the read to the worker's chunkQueue

5. Each CAS worker calls `bool GetNextChunk(ChunkQueue* queue, uint32_t* partitionId /*out*/, Chunk* chunk /*out*/, int* err);` in an infinite loop. Memory for `chunk->m_ptr` data is allocated by `libs2client`. The pointer to it is copied to `chunk`. To free the memory, `ChunkFree(chunk)` must be called.
