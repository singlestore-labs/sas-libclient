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

#ifndef AVRO_TABLE_WRITER_HPP
#define AVRO_TABLE_WRITER_HPP

#include <iostream>
#include <memory>

#include "avro.h"
#include "avro/encoding.h"

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
