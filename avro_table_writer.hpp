#ifndef AVRO_TABLE_WRITER_HPP
#define AVRO_TABLE_WRITER_HPP

#include <iostream>
#include <memory>

#include "avro.h"
#include "encoding.h"

typedef struct AvroBuffer
{
    char *m_ptr;
    int64_t m_size;
    int64_t m_current_pos;
} AvroBuffer;

class AvroTableWriter
{
  public:
    AvroTableWriter(AvroBuffer *buf)
        :
        m_buf(buf)
            {};

    static int
    ss_local_infile_init(
        void **ptr,
        const char *filename,
        void *handler_data);

    static int
    ss_local_infile_read(
        void *ptr,
        char *buf,
        unsigned int buf_len);

    static void ss_local_infile_end(void *ptr);

    static int
    ss_local_infile_error(
        void *ptr,
        char *error_msg,
        unsigned int error_msg_len);

    AvroBuffer *m_buf;
};

#endif  // AVRO_TABLE_WRITER_HPP
