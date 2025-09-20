#pragma once

#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "common/ix_compare.h"
#include "transaction/transaction.h"

enum class State
{
    FREE = 0,
    READ_ONLY,
    WRITE_ONLY
};

class Gap : public std::enable_shared_from_this<Gap>
{
public:
    Gap(const txn_id_t &txn, bool txn_mode, const char *upper, const char *lower, bool upper_is_closed, bool lower_is_closed, std::vector<ColType> col_types, std::vector<int> col_lens) : txn_(txn), txn_mode_(txn_mode), upper_is_closed_(upper_is_closed), lower_is_closed_(lower_is_closed), col_types_(std::move(col_types)), col_lens_(std::move(col_lens))
    {
        auto tot_len = 0;
        for (const auto &l : col_lens_)
        {
            tot_len += l;
        }

        auto upper_ = new_char(tot_len);
        auto lower_ = new_char(tot_len);

        memcpy(upper_.get(), upper, tot_len);
        memcpy(lower_.get(), lower, tot_len);

        if (ix_compare(lower_.get(), upper_.get(), col_types_, col_lens_) == 1)
        {
            std::swap(lower_, upper_);
        }
    }

    bool need_wait_for_write(txn_id_t waiter_txn_, const Gap &gap)
    {
        if (now_state == State::FREE)
        {
            return false;
        }
        return judge_need_wait(waiter_txn_, gap);
    }

    bool need_wait_for_read(txn_id_t waiter_txn_, const Gap &gap)
    {
        if (now_state != State::WRITE_ONLY)
        {
            return false;
        }
        return judge_need_wait(waiter_txn_, gap);
    }

    void wait()
    {
        ++waiting_count_;
        latch_.lock_shared();
        latch_.unlock_shared();
        --waiting_count_;
    }

    void lock()
    {
        latch_.lock();
        now_state = State::WRITE_ONLY;
    }

    void lock_shared()
    {
        latch_.lock();
        now_state = State::READ_ONLY;
    }

    void unlock()
    {
        latch_.unlock();
        now_state = State::FREE;

        const auto wc_ = waiting_count_.load() << 2;
        for (int i = 0; i < wc_; i++)
        {
            std::this_thread::yield();
        }
    }

private:
    txn_id_t txn_;
    bool txn_mode_;

    char *upper_;
    char *lower_;

    bool upper_is_closed_;
    bool lower_is_closed_;

    std::vector<ColType> col_types_;
    std::vector<int> col_lens_;

    std::shared_mutex latch_;
    std::atomic<State> now_state = State::FREE;

    std::atomic<int> waiting_count_{0};

    bool overlap(const Gap &other) const
    {
        if (ix_compare(other.lower_, upper_, col_types_, col_lens_) == 1)
        {
            return false;
        }
        if (ix_compare(lower_, other.upper_, col_types_, col_lens_) == 1)
        {
            return false;
        }
        if (ix_compare(other.lower_, upper_, col_types_, col_lens_) == 0)
        {
            return other.lower_is_closed_ && upper_is_closed_;
        }
        if (ix_compare(lower_, other.upper_, col_types_, col_lens_) == 0)
        {
            return lower_is_closed_ && other.upper_is_closed_;
        }
        return true;
    }

    bool judge_need_wait(const txn_id_t &waiter_txn_, const Gap &gap) const
    {
        // 如果两个间隙没有交集，则直接通过
        if (!overlap(gap))
        {
            return false;
        }
        // 老事务向新事物申请等待，则阻塞
        if (waiter_txn_ < txn_)
        {
            return true;
        }
        // 自己向自己申请等待，则直接通过
        if (waiter_txn_ == txn_)
        {
            return false;
        }
        // 新事务向老事务申请等待，则回滚新事务
        if (waiter_txn_ > txn_)
        {
            if (txn_mode_)
            {
                throw TransactionAbortException(waiter_txn_, AbortReason::LOCK_ON_SHRINKING);
            }
            return true;
        }
        return false;
    }
};

class TxnGapLock
{
public:
    void lock(const txn_id_t &insert_txn_, Gap &insert_gap_)
    {
        std::vector<std::shared_ptr<Gap>> wait_gaps_;

        mutex_.lock();
        for (const auto &[txn_, gaps_] : txn_gaps_)
        {
            for (const auto &gap_ : gaps_)
            {
                try
                {
                    if (gap_->need_wait_for_write(insert_txn_, insert_gap_))
                    {
                        wait_gaps_.emplace_back(gap_);
                    }
                }
                catch (const TransactionAbortException &)
                {
                    mutex_.unlock();
                    throw;
                }
            }
        }
        mutex_.unlock();

        for (const auto &wg : wait_gaps_)
        {
            wg->wait();
        }

        insert_gap_.lock();

        mutex_.lock();
        if (txn_gaps_.find(insert_txn_) == txn_gaps_.end())
        {
            txn_gaps_.emplace(insert_txn_, std::vector<std::shared_ptr<Gap>>());
        }
        txn_gaps_.at(insert_txn_).emplace_back(&insert_gap_);
        mutex_.unlock();
    }

    void lock_shared(const txn_id_t &insert_txn_, Gap &insert_gap_)
    {
        std::vector<std::shared_ptr<Gap>> wait_gaps_;

        mutex_.lock();
        for (const auto &[txn_, gaps_] : txn_gaps_)
        {
            for (const auto &gap_ : gaps_)
            {
                try
                {
                    if (gap_->need_wait_for_read(insert_txn_, insert_gap_))
                    {
                        wait_gaps_.emplace_back(gap_);
                    }
                }
                catch (const TransactionAbortException &)
                {
                    mutex_.unlock();
                    throw;
                }
            }
        }
        mutex_.unlock();

        for (const auto &wg : wait_gaps_)
        {
            wg->wait();
        }

        insert_gap_.lock_shared();

        mutex_.lock();
        if (txn_gaps_.find(insert_txn_) == txn_gaps_.end())
        {
            txn_gaps_.emplace(insert_txn_, std::vector<std::shared_ptr<Gap>>());
        }
        txn_gaps_.at(insert_txn_).emplace_back(&insert_gap_);
        mutex_.unlock();
    }

    void unlock(const txn_id_t &txn_)
    {
        std::unique_lock lock(mutex_);
        if (txn_gaps_.find(txn_) == txn_gaps_.end())
        {
            return;
        }
        auto &gaps = txn_gaps_.at(txn_);
        for (const auto &gap : gaps)
        {
            gap->unlock();
        }
        txn_gaps_.erase(txn_);
    }

private:
    std::mutex mutex_;
    std::unordered_map<txn_id_t, std::vector<std::shared_ptr<Gap>>> txn_gaps_;
};

class TxnShardLock
{
public:
    TxnShardLock() = default;
    ~TxnShardLock() = default;

    void lock_shared(const std::string &tab_name, txn_id_t txn_, bool txn_mode)
    {
        std::shared_lock<std::shared_mutex> read_lock(latch_);
        std::unique_lock<std::mutex> consistency_lock(data_consistency_latch_);

        switch (now_state_)
        {
        case State::FREE:
        case State::READ_ONLY:
            grant_read_psm(txn_, txn_mode);
            break;
        case State::WRITE_ONLY:
            handle_write_state_for_read_lock(txn_, txn_mode);
            break;
        default:
            break;
        }
    }

    void lock(const std::string &tab_name, txn_id_t txn_, bool txn_mode)
    {
        std::unique_lock<std::shared_mutex> write_lock(latch_);
        std::unique_lock<std::mutex> consistency_lock(data_consistency_latch_);

        switch (now_state_)
        {
        case State::FREE:
            grant_write_psm(txn_, txn_mode);
            break;
        case State::READ_ONLY:
            handle_read_state_for_write_lock(txn_, txn_mode);
            break;
        case State::WRITE_ONLY:
            handle_write_state_for_write_lock(txn_, txn_mode);
            break;
        default:
            break;
        }
    }

    void unlock(const std::string &tab_name, txn_id_t txn_)
    {
        std::unique_lock<std::shared_mutex> lock(latch_);
        std::unique_lock<std::mutex> consistency_lock(data_consistency_latch_);

        switch (now_state_)
        {
        case State::FREE:
            return;
        case State::READ_ONLY:
            erase_read_psm(txn_);
            break;
        case State::WRITE_ONLY:
            erase_write_psm(txn_);
            break;
        default:
            break;
        }
    }

private:
    std::atomic<State> now_state_ = State::FREE;
    std::atomic<txn_id_t> write_pms_txn_{INVALID_TXN_ID};
    std::atomic<bool> write_pms_txn_mode_{false};
    std::unordered_map<txn_id_t, bool> read_pms_txns_;
    std::shared_mutex latch_;
    std::mutex data_consistency_latch_;

    void grant_read_psm(txn_id_t txn_, bool txn_mode)
    {
        if (read_pms_txns_.find(txn_) != read_pms_txns_.end())
        {
            return;
        }
        now_state_ = State::READ_ONLY;
        read_pms_txns_.emplace(txn_, txn_mode);
    }

    void grant_write_psm(txn_id_t txn_, bool txn_mode)
    {
        if (now_state_ == State::WRITE_ONLY && write_pms_txn_ == txn_)
        {
            return;
        }
        now_state_ = State::WRITE_ONLY;
        write_pms_txn_ = txn_;
        write_pms_txn_mode_ = txn_mode;
    }

    void erase_read_psm(txn_id_t txn_)
    {
        if (read_pms_txns_.find(txn_) == read_pms_txns_.end())
        {
            return;
        }
        read_pms_txns_.erase(txn_);
        if (read_pms_txns_.empty())
        {
            now_state_ = State::FREE;
        }
    }

    void erase_write_psm(txn_id_t txn_)
    {
        if (write_pms_txn_ != txn_)
        {
            return;
        }
        now_state_ = State::FREE;
    }

    void handle_write_state_for_read_lock(txn_id_t txn_, bool txn_mode)
    {
        const int write_psm_txn{write_pms_txn_.load()};
        if (write_psm_txn < txn_)
        {
            if (write_pms_txn_mode_)
            {
                throw TransactionAbortException(txn_, AbortReason::WAIT_DIE_ABORT);
            }
            grant_read_psm(txn_, txn_mode);
        }
        else if (write_psm_txn == txn_)
        {
            return;
        }
        else
        {
            grant_read_psm(txn_, txn_mode);
        }
    }

    void handle_read_state_for_write_lock(txn_id_t txn_, bool txn_mode)
    {
        const auto [max_psm_txn, pms_txn_mode_] = *read_pms_txns_.begin();
        if (max_psm_txn < txn_)
        {
            if (pms_txn_mode_)
            {
                throw TransactionAbortException(txn_, AbortReason::WAIT_DIE_ABORT);
            }
            grant_write_psm(txn_, txn_mode);
        }
        else if (max_psm_txn == txn_)
        {
            erase_read_psm(txn_);
            grant_write_psm(txn_, txn_mode);
        }
        else
        {
            grant_write_psm(txn_, txn_mode);
        }
    }

    void handle_write_state_for_write_lock(txn_id_t txn_, bool txn_mode)
    {
        const int write_psm_txn{write_pms_txn_.load()};
        if (write_psm_txn < txn_)
        {
            if (write_pms_txn_mode_)
            {
                throw TransactionAbortException(txn_, AbortReason::WAIT_DIE_ABORT);
            }
            grant_write_psm(txn_, txn_mode);
        }
        else if (write_psm_txn == txn_)
        {
            return;
        }
        else
        {
            grant_write_psm(txn_, txn_mode);
        }
    }
};

class LockManager
{
public:
    LockManager() = default;

    ~LockManager() = default;

    void lock_shared_on_table(Transaction *txn, const std::string &tab_name)
    {
        tab_lock_map_[tab_name].lock_shared(tab_name, txn->get_transaction_id(), txn->get_txn_mode());
        txn->append_tab_lock_set(tab_name);
    }

    void lock_exclusive_on_table(Transaction *txn, const std::string &tab_name)
    {
        tab_lock_map_[tab_name].lock(tab_name, txn->get_transaction_id(), txn->get_txn_mode());
        txn->append_tab_lock_set(tab_name);
    }

    void unlock_on_table(Transaction *txn, const std::string &tab_name)
    {
        if (tab_lock_map_.find(tab_name) != tab_lock_map_.end())
        {
            tab_lock_map_.at(tab_name).unlock(tab_name, txn->get_transaction_id());
        }
    }

    void lock_exclusive_gap_on_index(Transaction *txn, const std::string &idx_name, char *upper, char *lower, bool upper_is_closed, bool lower_is_closed, const std::vector<ColType> &col_types, const std::vector<int> &col_lens)
    {
        auto *gap = new Gap(txn->get_transaction_id(), txn->get_txn_mode(), upper, lower, upper_is_closed, lower_is_closed, col_types, col_lens);
        gap_lock_map_[idx_name].lock(txn->get_transaction_id(), *gap);
        txn->append_gap_lock_set(idx_name);
    }

    void lock_shared_gap_on_index(Transaction *txn, const std::string &idx_name, char *upper, char *lower, bool upper_is_closed, bool lower_is_closed, const std::vector<ColType> &col_types, const std::vector<int> &col_lens)
    {
        auto *gap = new Gap(txn->get_transaction_id(), txn->get_txn_mode(), upper, lower, upper_is_closed, lower_is_closed, col_types, col_lens);
        gap_lock_map_[idx_name].lock_shared(txn->get_transaction_id(), *gap);
        txn->append_gap_lock_set(idx_name);
    }

    void unlock_gap_on_index(Transaction *txn, const std::string &idx_name)
    {
        if (gap_lock_map_.find(idx_name) != gap_lock_map_.end())
        {
            gap_lock_map_.at(idx_name).unlock(txn->get_transaction_id());
        }
    }

private:
    std::unordered_map<std::string, TxnShardLock> tab_lock_map_;
    std::unordered_map<std::string, TxnGapLock> gap_lock_map_;
};
