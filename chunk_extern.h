#ifndef CHUNK_EXTERN_H
#define CHUNK_EXTERN_H

#include <unistd.h>
#include <stdint.h>

enum ColumnType
{
    BigInt,
    Double,
    Variable,
    Fixed,
    Unsupported,
};

// this Enum will be used if decide to do all the type conversions in libclient
// enum ColumnType
// {
//     Int64,  // supported
//     Int32,
//     Double,  // supported
//     Varchar,  // supported
//     Varbinary,
//     Char,
//     Binary,
//     Time,  // int64 number of microsenconds since midnight
//     Date,  // int32 number of days since 1 Jan 1960
//     DateTime, // int64 number of microseconds since midnight 1 Jan 1960
//     DecQuad, // Decimal
//     DecSext, // Decimal
// };

typedef struct Column
{
    enum ColumnType type;
    int64_t size;
    char* name;
} Column;

typedef struct RowSchema
{
    Column* ColumnInfo;
    uint32_t numColumns;
} RowSchema;

typedef struct Chunk
{
    char* m_ptr;
    uint64_t m_size;
    uint64_t row_count;
    uint64_t id;
    uint32_t partition_id;
} Chunk;

struct S2ErrorCallback;

typedef struct S2ErrorCallback
{
    void (*setError)(struct S2ErrorCallback* cb, int error, const char* errorString);
} S2ErrorCallback;

#define S2C_ERROR_INV_ARG 1            // invalid argument
#define S2C_ERROR_UNS_DATA_TYPE 2      // unsupported data type
#define S2C_ERROR_UNKNOWN_FAILURE 3    // unknown failure
#define S2C_ERROR_MEMORY_ALLOCATION 4  // memory allocation error
#define S2C_ERROR_READER_FAILED 5      // some of the readers failed

#endif  // CHUNK_EXTERN_H
