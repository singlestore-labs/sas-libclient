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

#include <cstring>
#include <iomanip>

#include "avro_table_writer.hpp"
#include "hdat/common.hpp"

// ss_local_infile_init copies the pointer *caller_data which is passed to
// mysql_set_local_infile_handler before the LOAD DATA statment is issued.
// This pointer will be then used in ss_local_infile_read
int
AvroTableWriter::ss_local_infile_init(
    void **ptr,
    const char *filename,
    void *caller_data)
{
    *ptr = caller_data;
    return 0;
}

// ss_local_infile_read uses *ptr passed in ss_local_infile_init.
// It reads buf_len characters from the byte array pointed by ptr
// and returns the actual number of bytes read
int
AvroTableWriter::ss_local_infile_read(
    void *ptr,
    char *buf,
    unsigned int buf_len)
{
    AvroBuffer *sourceBuf = (AvroBuffer *)ptr;

    int n_bytes = buf_len <= (sourceBuf->m_size - sourceBuf->m_current_pos)
                      ? buf_len
                      : (sourceBuf->m_size - sourceBuf->m_current_pos);
    memcpy(buf, (char *)(sourceBuf->m_ptr + sourceBuf->m_current_pos), n_bytes);
    sourceBuf->m_current_pos += n_bytes;
    return n_bytes;
}

// ss_local_infile_end as long as we don't allocate additional
// memory in ss_local_infile_init
void AvroTableWriter::ss_local_infile_end(void *ptr)
{
    return;
}

// ss_local_infile_error is called to get a textual error message
// to return to the user in case any of ss_local_infile_* functions returns an error
int
AvroTableWriter::ss_local_infile_error(
    void *ptr,
    char *error_msg,
    unsigned int error_msg_len)
{
    if (ptr == nullptr)
    {
        strncpy(error_msg, "Invalid *caller_data pointer", error_msg_len);
        return S2C_ERROR_INV_ARG;
    }
    AvroBuffer *sourceBuf = (AvroBuffer *)ptr;

    if (sourceBuf->m_current_pos > sourceBuf->m_size)
    {
        strncpy(error_msg, "Invalid operation on source buffer", error_msg_len);
        return S2C_IO_ERROR;
    }

    strncpy(error_msg, "Unknown error in LOAD DATA LOCAL INFILE", error_msg_len);
    return S2C_ERROR_UNKNOWN_FAILURE;
}

extern "C"
{
    int
    WriteInt64Avro(
        avro_writer_t w,
        int64_t val)
    {
        return avro_binary_encoding.write_long(w, val);
    }

    int
    WriteInt32Avro(
        avro_writer_t w,
        int32_t val)
    {
        return avro_binary_encoding.write_int(w, val);
    }

    int
    WriteDoubleAvro(
        avro_writer_t w,
        double val)
    {
        return avro_binary_encoding.write_double(w, val);
    }

    int
    WriteBytesAvro(
        avro_writer_t w,
        const void *val,
        const int64_t len)
    {
        // this can be potentially used for ["bytes", "null"] union
        // if (len == 0)
        // {
        //     return avro_binary_encoding.write_int(w, 1);
        // }
        // avro_binary_encoding.write_int(w, 0);
        return avro_binary_encoding.write_bytes(w, (char *)val, len);
    }
}
