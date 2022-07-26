#ifndef CHUNK_EXTERN_H
#define CHUNK_EXTERN_H

#include <unistd.h>
#include <stdint.h>

enum ColumnType
{
    Int64,     // BIGINT
    Int32,     // INT
    Double,    // DOUBLE
    Variable,  // LONGTEXT, TEXT, BLOB, LONGBLOB
    Fixed,     // VARCHAR, VARBINARY, CHAR, BINARY
    Time,      // TIME, converted to int64 number of microsenconds since midnight
    Date,      // DATE, converted to int32 number of days since 1 Jan 1960
    DateTime,  // DATETIME, DATETIME(6) converted to int64 number of microseconds since midnight 1 Jan 1960
    Unsupported,
};

typedef struct Column
{
    enum ColumnType type;
    int64_t size;
    char* name;
    bool is_binary;
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
    uint64_t consumed_size;
    uint64_t id;
    uint32_t partition_id;  // S2 partition, serves as a part of key in multi-pass
} Chunk;

struct S2ErrorCallback;

typedef struct S2ErrorCallback
{
    void (*setError)(struct S2ErrorCallback* cb, int error, const char* errorString, int severityLevel);
} S2ErrorCallback;

static const int64_t int64Null = 0x8000000000000000;
static const int32_t int32Null = 0x80000000;
static const char* variableNull = "";
extern const double doubleNull;

#define S2C_ERROR_INV_ARG 1            // invalid argument
#define S2C_ERROR_UNS_DATA_TYPE 2      // unsupported data type
#define S2C_ERROR_UNKNOWN_FAILURE 3    // unknown failure
#define S2C_ERROR_MEMORY_ALLOCATION 4  // memory allocation error
#define S2C_ERROR_READER_FAILED 5      // some of the readers failed
#define S2C_ERROR_BAD_CONNECTION 6     // wrong connection options have been used
#define S2C_IO_ERROR 7                 // error during processing bytes on LOAD DATA
#define S2C_ERROR_BAD_CHUNK 8          // malformed chunk

#define S2C_SEVERITY_ERROR 0    // an error which makes fuction's result invalid occurred
#define S2C_SEVERITY_WARNING 1  // an error occurred, but it was circumvented

#endif  // CHUNK_EXTERN_H
