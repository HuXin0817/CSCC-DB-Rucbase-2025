#pragma once

#include <climits>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "execution_manager_finals.h"
#include "executor_abstract_finals.h"
#include "record/rm_scan_finals.h"

class GapLockExecutor
{
public:
    TabMeta *tab_;
    RmFileHandle *fh_;
    std::vector<Condition> conds_;
    Context *context_;
    Gap *gap;
    PoolManager *memory_pool_manager_;

    char *lower_key_;
    char *upper_key_;

public:
    GapLockExecutor(SmManager *sm_manager, TabMeta *tab, const std::vector<Condition> &conds, Context *context) : memory_pool_manager_(sm_manager->memory_pool_manager_)
    {
        tab_ = tab;
        fh_ = sm_manager->fhs_[tab_->fd_].get();
        conds_ = conds;
        context_ = context;

        lower_key_ = memory_pool_manager_->allocate(fh_->record_size);
        upper_key_ = memory_pool_manager_->allocate(fh_->record_size);

        for (const auto &meta : tab_->cols)
        {
            auto &col_meta_ = tab_->get_col(meta.name);
            switch (col_meta_.type)
            {
            case ColType::TYPE_INT:
            {
                int min_int = std::numeric_limits<int>::min();
                memcpy(lower_key_ + col_meta_.offset, &min_int, col_meta_.len);
                int max_int = std::numeric_limits<int>::max();
                memcpy(upper_key_ + col_meta_.offset, &max_int, col_meta_.len);
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                float float_min = std::numeric_limits<float>::lowest();
                memcpy(lower_key_ + col_meta_.offset, &float_min, col_meta_.len);
                float max_float = std::numeric_limits<float>::max();
                memcpy(upper_key_ + col_meta_.offset, &max_float, col_meta_.len);
                break;
            }
            case ColType::TYPE_STRING:
                memset(lower_key_ + col_meta_.offset, 0x00, col_meta_.len);
                memset(upper_key_ + col_meta_.offset, 0xff, col_meta_.len);
                break;
            default:
                break;
            }
        }

        std::vector<int> upper_is_closed_(tab_->cols.size(), 0);
        std::vector<int> lower_is_closed_(tab_->cols.size(), 0);
        std::unordered_set<int> col_idx_set_;

        for (auto &cond : conds_)
        {
            auto &col_meta_ = tab_->get_col(cond.lhs_col.col_name);
            int offset = col_meta_.idx;
            col_idx_set_.insert(offset);

            switch (cond.op)
            {
            case CompOp::OP_EQ:
            {
                upper_copy(upper_key_ + col_meta_.offset, upper_is_closed_[offset], cond.rhs_val.raw->data, true, col_meta_.type, col_meta_.len);
                lower_copy(lower_key_ + col_meta_.offset, lower_is_closed_[offset], cond.rhs_val.raw->data, true, col_meta_.type, col_meta_.len);
                break;
            }
            case CompOp::OP_LT:
            {
                upper_copy(upper_key_ + col_meta_.offset, upper_is_closed_[offset], cond.rhs_val.raw->data, false, col_meta_.type, col_meta_.len);
                break;
            }
            case CompOp::OP_LE:
            {
                upper_copy(upper_key_ + col_meta_.offset, upper_is_closed_[offset], cond.rhs_val.raw->data, true, col_meta_.type, col_meta_.len);
                break;
            }
            case CompOp::OP_GE:
            {
                lower_copy(lower_key_ + col_meta_.offset, lower_is_closed_[offset], cond.rhs_val.raw->data, true, col_meta_.type, col_meta_.len);
                break;
            }
            case CompOp::OP_GT:
            {
                lower_copy(lower_key_ + col_meta_.offset, lower_is_closed_[offset], cond.rhs_val.raw->data, false, col_meta_.type, col_meta_.len);
                break;
            }
            }
        }

        std::vector<int> col_idx_;
        col_idx_.reserve(col_idx_set_.size());
        for (auto &col_id_ : col_idx_set_)
        {
            col_idx_.push_back(col_id_);
        }

        gap = context_->lock_mgr_->lock_shared_on_gap(context_->txn_, tab_->fd_, tab_, upper_key_, lower_key_, upper_is_closed_, lower_is_closed_, col_idx_);
    }

private:
    static void upper_copy(char *upper_key, int &upper_is_closed, char *key, int is_closed, ColType type, int len)
    {
        switch (type)
        {
        case TYPE_INT:
        {
            auto upper_value_ = *(int *)upper_key;
            auto value_ = *(int *)key;
            if (upper_value_ > value_)
            {
                memcpy(upper_key, key, len);
                upper_is_closed = is_closed;
            }
            if (upper_value_ == value_)
            {
                upper_is_closed |= is_closed;
            }
            break;
        }
        case TYPE_FLOAT:
        {
            auto upper_value_ = *(float *)upper_key;
            auto value_ = *(float *)key;
            if (upper_value_ > value_)
            {
                memcpy(upper_key, key, len);
                upper_is_closed = is_closed;
            }
            else if (upper_value_ == value_)
            {
                upper_is_closed |= is_closed;
            }
            break;
        }
        case TYPE_STRING:
        {
            auto cmp = memcmp(upper_key, key, len);
            if (cmp > 0)
            {
                memcpy(upper_key, key, len);
                upper_is_closed = is_closed;
            }
            else if (cmp == 0)
            {
                upper_is_closed |= is_closed;
            }
            break;
        }
        }
    }

    static void lower_copy(char *lower_key, int &lower_is_closed, char *key, int is_closed, ColType type, int len)
    {
        switch (type)
        {
        case TYPE_INT:
        {
            auto lower_value_ = *(int *)lower_key;
            auto value_ = *(int *)key;
            if (lower_value_ < value_)
            {
                memcpy(lower_key, key, len);
                lower_is_closed = is_closed;
            }
            else if (lower_value_ == value_)
            {
                lower_is_closed |= is_closed;
            }
            break;
        }
        case TYPE_FLOAT:
        {
            auto lower_value_ = *(float *)lower_key;
            auto value_ = *(float *)key;
            if (lower_value_ < value_)
            {
                memcpy(lower_key, key, len);
                lower_is_closed = is_closed;
            }
            else if (lower_value_ == value_)
            {
                lower_is_closed |= is_closed;
            }
            break;
        }
        case TYPE_STRING:
        {
            auto cmp = memcmp(lower_key, key, len);
            if (cmp < 0)
            {
                memcpy(lower_key, key, len);
                lower_is_closed = is_closed;
            }
            else if (cmp == 0)
            {
                lower_is_closed |= is_closed;
            }
            break;
        }
        }
    }
};
