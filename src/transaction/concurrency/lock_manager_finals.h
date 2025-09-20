#pragma once

#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config_finals.h"
#include "common/value_finals.h"
#include "storage/memory_pool_manager.h"
#include "transaction/transaction_finals.h"

class Gap
{
    friend class IndexScanExecutor;

public:
    Gap(TabMeta *tab_meta, char *upper, char *lower, std::vector<int> upper_is_closed, std::vector<int> lower_is_closed, const std::vector<int> &col_idx, PoolManager *memory_pool_manager) : memory_pool_manager_(memory_pool_manager), upper_(upper), lower_(lower), col_tot_len(tab_meta->col_tot_len), upper_is_closed_(std::move(upper_is_closed)), lower_is_closed_(std::move(lower_is_closed))
    {
        cols.reserve(col_idx.size());
        for (auto idx : col_idx)
        {
            cols.push_back(tab_meta->cols[idx]);
        }
    }

    ~Gap()
    {
        memory_pool_manager_->deallocate(upper_, col_tot_len);
        memory_pool_manager_->deallocate(lower_, col_tot_len);
    }

    bool overlap(const char *key) const
    {
        for (auto &col : cols)
        {
            switch (col.type)
            {
            case ColType::TYPE_INT:
            {
                auto up = *(int *)(upper_ + col.offset);
                auto low = *(int *)(lower_ + col.offset);
                auto value = *(int *)(key + col.offset);
                if (upper_is_closed_[col.idx])
                {
                    if (value > up)
                    {
                        return false;
                    }
                }
                else
                {
                    if (value >= up)
                    {
                        return false;
                    }
                }
                if (lower_is_closed_[col.idx])
                {
                    if (value < low)
                    {
                        return false;
                    }
                }
                else
                {
                    if (value <= low)
                    {
                        return false;
                    }
                }
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                auto up = *(float *)(upper_ + col.offset);
                auto low = *(float *)(lower_ + col.offset);
                auto value = *(float *)(key + col.offset);
                if (upper_is_closed_[col.idx])
                {
                    if (value > up)
                    {
                        return false;
                    }
                }
                else
                {
                    if (value >= up)
                    {
                        return false;
                    }
                }
                if (lower_is_closed_[col.idx])
                {
                    if (value < low)
                    {
                        return false;
                    }
                }
                else
                {
                    if (value <= low)
                    {
                        return false;
                    }
                }
                break;
            }
            case ColType::TYPE_STRING:
            {
                auto up = upper_ + col.offset;
                auto low = lower_ + col.offset;
                auto value = key + col.offset;

                auto upcmp = memcmp(value, up, col.len);
                auto lowcmp = memcmp(value, low, col.len);

                if (upper_is_closed_[col.idx])
                {
                    if (upcmp > 0)
                    {
                        return false;
                    }
                }
                else
                {
                    if (upcmp >= 0)
                    {
                        return false;
                    }
                }
                if (lower_is_closed_[col.idx])
                {
                    if (lowcmp < 0)
                    {
                        return false;
                    }
                }
                else
                {
                    if (lowcmp <= 0)
                    {
                        return false;
                    }
                }
                break;
            }
            }
        }
        return true;
    }

private:
    PoolManager *memory_pool_manager_;
    char *upper_;
    char *lower_;
    int col_tot_len;
    std::vector<ColMeta> cols;
    std::vector<int> upper_is_closed_;
    std::vector<int> lower_is_closed_;
};

class LockManager
{
public:
    explicit LockManager(PoolManager *memory_pool_manager) : memory_pool_manager_(memory_pool_manager) {}

    Gap *lock_shared_on_gap(const std::shared_ptr<Transaction> &txn, int fd, TabMeta *tab_meta, char *upper, char *lower, const std::vector<int> &upper_is_closed, const std::vector<int> &lower_is_closed, const std::vector<int> &col_idx)
    {
        auto gap = std::make_shared<Gap>(tab_meta, upper, lower, upper_is_closed, lower_is_closed, col_idx, memory_pool_manager_);
        while (lock_shared_on_gap(txn, gap, fd))
        {
            std::this_thread::yield();
        }
        {
            std::unique_lock lock(latch_[fd]);
            tab_lock_map_[fd][txn->txn_id_].push_back(std::move(gap));
            txn->gap_lock_map_.insert(fd);
            return tab_lock_map_[fd][txn->txn_id_].back().get();
        }
    }

    void lock_exclusive_on_data(const std::shared_ptr<Transaction> &txn, int fd, char *rid_)
    {
        while (lock_on_data(txn, fd, rid_))
        {
            std::this_thread::yield();
        }
        {
            std::unique_lock lock(latch_[fd]);
            data_lock_map_[fd][txn->txn_id_].push_back(rid_);
        }
        txn->data_lock_map_.emplace(fd);
    }

    void unlock(const std::shared_ptr<Transaction> &txn)
    {
        for (auto fd : txn->gap_lock_map_)
        {
            std::unique_lock lock(latch_[fd]);
            tab_lock_map_[fd].erase(txn->txn_id_);
        }
        for (auto fd : txn->data_lock_map_)
        {
            std::unique_lock lock(latch_[fd]);
            data_lock_map_[fd].erase(txn->txn_id_);
        }
    }

private:
    PoolManager *memory_pool_manager_;
    std::shared_mutex latch_[MAX_TABLE_NUMBER];
    std::map<txn_id_t, std::vector<std::shared_ptr<Gap>>> tab_lock_map_[MAX_TABLE_NUMBER];
    std::map<txn_id_t, std::vector<char *>> data_lock_map_[MAX_TABLE_NUMBER];

    bool lock_shared_on_gap(const std::shared_ptr<Transaction> &txn, std::shared_ptr<Gap> &gap, int fd)
    {
        std::shared_lock lock(latch_[fd]);
        for (auto &[data_txn_id_, lock_ids_] : data_lock_map_[fd])
        {
            if (data_txn_id_ == txn->txn_id_)
            {
                continue;
            }
            for (auto lock_id_ : lock_ids_)
            {
                if (gap->overlap(lock_id_))
                {
                    if (txn->txn_id_ < data_txn_id_)
                    {
                        return true;
                    }
                    else
                    {
                        throw TransactionAbortException();
                    }
                }
            }
        }
        return false;
    }

    bool lock_on_data(const std::shared_ptr<Transaction> &txn, int fd, char *rid_)
    {
        std::shared_lock lock(latch_[fd]);
        for (auto &[gap_txn_id_, gaps_] : tab_lock_map_[fd])
        {
            if (gap_txn_id_ == txn->txn_id_)
            {
                continue;
            }
            for (auto &gap_ : gaps_)
            {
                if (gap_->overlap(rid_))
                {
                    if (txn->txn_id_ < gap_txn_id_)
                    {
                        return true;
                    }
                    else
                    {
                        throw TransactionAbortException();
                    }
                }
            }
        }
        return false;
    }
};
