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

#ifndef TSV_TABLE_WRITER_HPP
#define TSV_TABLE_WRITER_HPP

#include "memory"
#include <iostream>
#include <sstream>

#include "chunk_extern.h"
#include "utils.hpp"

#include "hdat/chunk_writer.hpp"
#include "hdat/chunk_reader.hpp"

class TsvTableWriter
{
  public:
    TsvTableWriter(std::stringstream *rows)
    {
        m_tsv_rows = rows;
    };

    bool
    ChunkToTSV(
        std::unique_ptr<SuperChunkReader> &reader,
        int64_t row_count);

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

    std::stringstream *m_tsv_rows;
};

#endif  // TSV_TABLE_WRITER_HPP
