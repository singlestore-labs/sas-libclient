#ifndef HDAT_COMMON_HPP
#define HDAT_COMMON_HPP

#include <unistd.h>
#include <stdint.h>

#include <chunk_extern.h>

namespace super_chunk
{
    static const int64_t int64Miss = 0x8000000000000000;
    static const double doubleMiss = 0xfffffe0000000000;
    static const char* variableMiss = "";

    static const int defaultAlignmentSize = 8;

    inline uint64_t sizeofAligned8(uint64_t len)
    {
        return (len + defaultAlignmentSize - 1) & ~(defaultAlignmentSize - 1);
    }

    inline uint64_t
    rowSize(
        RowSchema* schema,
        unsigned long* lengths)
    {
        uint64_t total_size = 0;
        uint64_t current_size;
        for (auto i = 0; i < schema->numColumns; ++i)
        {
            current_size = lengths[i];
            switch (schema->ColumnInfo[i].type)
            {
                case Variable:
                {
                    total_size += current_size + 2 * defaultAlignmentSize;
                    break;
                }
                case Fixed:
                {
                    total_size += sizeofAligned8(current_size);
                    break;
                }
                default:
                    total_size += defaultAlignmentSize;
            }
        }
        return total_size;
    }

}  // namespace super_chunk

#endif  // HDAT_COMMON_HPP
