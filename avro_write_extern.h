#ifndef AVRO_WRITE_EXTERN_H
#define AVRO_WRITE_EXTERN_H

#include "avro.h"

int
WriteInt64Avro(
    avro_writer_t w,
    int64_t val);

int
WriteInt32Avro(
    avro_writer_t w,
    int32_t val);

bool
WriteDoubleAvro(
    avro_writer_t w,
    double val);

bool
WriteBytesAvro(
    avro_writer_t w,
    const void *val,
    const int64_t len);

#endif  // AVRO_WRITE_EXTERN_H
