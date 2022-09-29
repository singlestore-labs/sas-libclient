#ifndef AVRO_WRITE_EXTERN_H
#define AVRO_WRITE_EXTERN_H

#include "avro.h"

/*
USAGE INSTRUCTIONS

1. Allocate `buf_size` bytes of memory for the buffer `buf`.
1. Create an `avro_writer_t` object `w`
    avro_writer_t w = avro_writer_memory(buf, buf_size);
2. Intialize a variable to indicate how many bytes are actually written to `buf`
    int total_bytes_written = 0;
3. Write data row by row. For each row, check that the corresponding funtion
  `Write<type>Avro` returns 0. If the return value is different from 0, the value
  has not been written.
  - If all values from the rows have been written successfully, update `total_bytes_written`:
    total_bytes_written = avro_writer_tell(w);
4. When all rows are written, or some value cannot be written (indicated by non-zero 
  return value of the corresponding write function), the writer object can be deleted and
  the buffer can be passed to `LOAD DATA...`:
    avro_writer_flush(w); 
    avro_writer_free(w);
    LoadDataAvro(s2Client, buf, total_bytes_written, row_schema, table_name, error_callback);
*/

int
WriteInt64Avro(
    avro_writer_t w,
    int64_t val);

int
WriteInt32Avro(
    avro_writer_t w,
    int32_t val);

int
WriteDoubleAvro(
    avro_writer_t w,
    double val);

int
WriteBytesAvro(
    avro_writer_t w,
    const void *val,
    const int64_t len);

#endif  // AVRO_WRITE_EXTERN_H
