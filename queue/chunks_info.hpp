#ifndef QUEUE_CHUNKS_INFO_HPP
#define QUEUE_CHUNKS_INFO_HPP

#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

#include "chunk_extern.h"
#include "s2_client_error.hpp"

class ChunksInfo
{
  public:
    ChunksInfo(std::vector<int> partitions)
    {
        for (auto p : partitions)
        {
            m_chunk_rows_sums[p] = {0};
        }
    }

    void Put(Chunk* chunk)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_chunk_rows_sums[chunk->partition_id].push_back(m_chunk_rows_sums[chunk->partition_id][chunk->id] + chunk->row_count);
    }

    int PartitionRowId(
        uint32_t partitionId,
        uint32_t chunkId,
        int64_t chunkRowNum)
    {
        if (chunkId >= m_chunk_rows_sums[partitionId].size())
        {
            throw S2ClientError(S2C_ERROR_INV_ARG, "chunkId " + std::to_string(chunkId) + " is too large");
        }
        return m_chunk_rows_sums[partitionId][chunkId] + chunkRowNum;
    }

  private:
    // m_chunk_size_sums maps partition ids to the vectors of partial
    // sums of chunk sizes
    std::unordered_map<int, std::vector<int>> m_chunk_rows_sums;

    std::mutex m_mutex;
};

#endif // QUEUE_CHUNKS_INFO_HPP
