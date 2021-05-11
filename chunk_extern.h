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
} Chunk;

#endif  // CHUNK_EXTERN_H
