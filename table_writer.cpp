#include <cstring>
#include <sstream>

#include "table_writer.hpp"

// ChunkToTSV writes
void
TableWriter::ChunkToTSV(
    std::unique_ptr<SuperChunkReader> &reader,
    int row_count)
{
    bool is_null;
    int64_t int_val;
    double float_val;
    const char *buf;
    uint64_t len;
    for (int row_num = 0; row_num < row_count; ++row_num)
    {
        if (row_num > 0)
        {
            *(this->tsv_rows) << "\n";
        }
        for (int col_num = 0; col_num < reader->m_row_schema->numColumns; ++col_num)
        {
            if (col_num > 0)
            {
                *(this->tsv_rows) << "\t";
            }
            switch (reader->m_row_schema->ColumnInfo[col_num].type)
            {
                case BigInt:
                    reader->ReadInteger(&int_val, &is_null);
                    if (!is_null)
                    {
                        *this->tsv_rows << int_val;
                    }
                    break;
                case Double:
                    reader->ReadFloat(&float_val, &is_null);
                    if (!is_null)
                    {
                        *this->tsv_rows << float_val;
                    }
                    break;
                case Variable:
                    reader->ReadVariable(&buf, &len, &is_null);
                    if (!is_null)
                    {
                        this->tsv_rows->write(buf, len);
                    }
                    break;
                default:
                    throw S2ClientError(S2C_ERROR_UNS_DATA_TYPE, "data type not supported yet");
            }
        }
    }
}

// in ss_local_infile_init we cpoy the pointer *caller_data which is passed to
// mysql_set_local_infile_handler before the LOAD DATA statment is issued.
// This pointer will be then used in ss_local_infile_read
int
TableWriter::ss_local_infile_init(
    void **ptr,
    const char *filename,
    void *caller_data)
{
    *ptr = caller_data;
    return 0;
}

// ss_local_infile_read uses *ptr passed in ss_local_infile_init.
// It reads buf_len characters from stringstream pointed by ptr
// and returns the actual number of bytes read
int
TableWriter::ss_local_infile_read(
    void *ptr,
    char *buf,
    unsigned int buf_len)
{
    std::stringstream *ss = static_cast<std::stringstream *>(ptr);
    ss->read(buf, buf_len);

    if (ss->bad())
    {
        return -1;
    }
    return ss->gcount();
}

// ss_local_infile_end as long as we don't allocate additional
// memory in ss_local_infile_init
void TableWriter::ss_local_infile_end(void *ptr)
{
    return;
}

// ss_local_infile_error is called to get a textual error message
// to return to the user in case any of ss_local_infile_* functions returns an error
int
TableWriter::ss_local_infile_error(
    void *ptr,
    char *error_msg,
    unsigned int error_msg_len)
{
    if (ptr == nullptr)
    {
        strncpy(error_msg, "invalid *caller_data pointer", error_msg_len);
        return S2C_ERROR_INV_ARG;
    }
    std::stringstream *ss = static_cast<std::stringstream *>(ptr);

    if (ss->bad())
    {
        strncpy(error_msg, "Read/write error on i/o operation", error_msg_len);
        return S2_IO_ERROR;
    }

    strncpy(error_msg, "Unknown error in LOAD DATA LOCAL INFILE", error_msg_len);
    return S2C_ERROR_UNKNOWN_FAILURE;
}
