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
