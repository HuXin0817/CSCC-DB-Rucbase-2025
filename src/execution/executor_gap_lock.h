#pragma once

#include <climits>
#include <cstring>
#include <memory>
#include <vector>

#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @class GapReadLockExecutor
 * @brief 执行索引扫描的执行器
 */
class GapReadLockExecutor
{
private:
    TabMeta tab_;                              // 表的元数据
    std::string tab_name_;                     // 表名
    RmFileHandle *fh_;                         // 表文件句柄
    std::vector<ColMeta> cols_;                // 表的列元数据
    size_t len_;                               // 记录的总长度
    IxIndexHandle *ih_;                        // 索引句柄
    std::vector<std::string> index_col_names_; // 索引列名称
    IndexMeta index_meta_;                     // 索引的元数据
    std::unique_ptr<IxScan> ix_scan_;          // 索引扫描器
    std::unique_ptr<RecScan> scan_;            // 记录扫描器
    SmManager *sm_manager_;                    // 系统管理器
    bool is_range_query = false;               // 是否是范围查询
    std::vector<Condition> conds_;             // 查询条件
    std::vector<Condition> fed_conds_;         // 处理后的查询条件
    std::vector<Condition> other_conds_;       // 其他查询条件
    std::vector<Condition> range_cond_;        // 范围查询条件

    Context *context_;

    bool lower_is_closed_ = false; // 范围查询下界是否闭合
    bool upper_is_closed_ = false; // 范围查询上界是否闭合

public:
    /**
     * @brief 构造函数，初始化索引扫描执行器
     * @param sm_manager 系统管理器
     * @param tab_name 表名
     * @param conds 查询条件
     * @param index_col_names 索引列名称
     * @param context 上下文
     */
    GapReadLockExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Condition> &conds, const std::vector<std::string> &index_col_names, Context *context)
    {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_ = sm_manager_->db_.get_table(tab_name);
        conds_ = conds;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        tab_name_ = tab_name;

        // 操作符对调表，用于处理条件
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ},
            {OP_NE, OP_NE},
            {OP_LT, OP_GT},
            {OP_GT, OP_LT},
            {OP_LE, OP_GE},
            {OP_GE, OP_LE},
        };

        for (auto &cond : conds_)
        {
            if (cond.lhs_col.tab_name != tab_name_)
            {
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }

        fed_conds_ = conds_;

        for (int i = 0; i < static_cast<int>(std::min(conds_.size(), index_col_names_.size())); i++)
        {
            if (!is_range_query && conds_[i].is_rhs_val && conds_[i].lhs_col.col_name == index_col_names_[i] && conds_[i].op != CompOp::OP_NE)
            {
                if (conds_[i].op == CompOp::OP_EQ)
                {
                    // 精确查询条件
                }
                else if (conds_[i].op != CompOp::OP_EQ)
                {
                    range_cond_.push_back(conds_[i]);
                    is_range_query = true;
                }
            }
            else
            {
                other_conds_.push_back(conds_[i]);
            }
        }

        auto ix_file_name = IxManager::get_index_name(tab_.name, index_meta_.cols);
        ih_ = sm_manager_->ihs_.at(ix_file_name).get();
    }

    /**
     * @brief 开始索引扫描的初始化操作
     */
    void beginTuple()
    {
        RmRecord gap_lower_key_(index_meta_.col_tot_len), gap_upper_key_(index_meta_.col_tot_len);

        int offset = 0;
        for (const auto &col : index_meta_.cols)
        {
            Value max_value_, min_value_;
            switch (col.type)
            {
            case TYPE_INT:
            {
                max_value_.set_int(std::numeric_limits<int>::max());
                max_value_.init_raw(col.len);
                min_value_.set_int(std::numeric_limits<int>::min());
                min_value_.init_raw(col.len);
                break;
            }
            case TYPE_FLOAT:
            {
                max_value_.set_float(std::numeric_limits<float>::max());
                max_value_.init_raw(col.len);
                min_value_.set_float(std::numeric_limits<float>::lowest());
                min_value_.init_raw(col.len);
                break;
            }
            case TYPE_STRING:
            {
                max_value_.set_str(std::string(col.len, std::numeric_limits<char>::max()));
                max_value_.init_raw(col.len);
                min_value_.set_str(std::string(col.len, 0));
                min_value_.init_raw(col.len);
                break;
            }
            default:
            {
                throw InternalError("Type Not Find");
            }
            }

            for (const auto &cond_ : fed_conds_)
            {
                if (cond_.lhs_col.col_name == col.name && cond_.is_rhs_val)
                {
                    switch (cond_.op)
                    {
                    case OP_EQ:
                    {
                        if (compare(cond_.rhs_val, min_value_))
                        {
                            min_value_ = cond_.rhs_val;
                        }
                        if (!compare(cond_.rhs_val, max_value_))
                        {
                            max_value_ = cond_.rhs_val;
                        }
                        break;
                    }
                    case OP_GT:
                    {
                        lower_is_closed_ = false;
                        if (compare(cond_.rhs_val, min_value_))
                        {
                            min_value_ = cond_.rhs_val;
                        }
                        break;
                    }
                    case OP_GE:
                    {
                        lower_is_closed_ = true;
                        if (compare(cond_.rhs_val, min_value_))
                        {
                            min_value_ = cond_.rhs_val;
                        }
                        break;
                    }
                    case OP_LT:
                    {
                        upper_is_closed_ = false;
                        if (!compare(cond_.rhs_val, max_value_))
                        {
                            max_value_ = cond_.rhs_val;
                        }
                        break;
                    }
                    case OP_LE:
                    {
                        upper_is_closed_ = true;
                        if (!compare(cond_.rhs_val, max_value_))
                        {
                            max_value_ = cond_.rhs_val;
                        }
                        break;
                    }
                    case OP_NE:
                    {
                    }
                    default:
                    {
                        break;
                    }
                    }
                }
            }

            memcpy(gap_upper_key_.data + offset, max_value_.raw->data, col.len);
            memcpy(gap_lower_key_.data + offset, min_value_.raw->data, col.len);
            offset += col.len;
        }

        std::vector<ColType> col_tpyes_;
        std::vector<int> col_lens_;

        for (const auto &col_ : index_meta_.cols)
        {
            col_tpyes_.emplace_back(col_.type);
            col_lens_.emplace_back(col_.len);
        }

        auto idx_name = IxManager::get_index_name(tab_.name, index_meta_.cols);

        context_->lock_mgr_->lock_shared_gap_on_index(context_->txn_, idx_name, gap_upper_key_.data, gap_lower_key_.data, upper_is_closed_, lower_is_closed_, col_tpyes_, col_lens_);

        context_->txn_->append_gap_lock_set(idx_name);
    }

    bool compare(const Value &value1, const Value &value2) const
    {
        switch (value1.type)
        {
        case TYPE_INT:
        {
            if (value2.type == TYPE_INT)
            {
                return value1.int_val > value2.int_val;
            }
            else if (value2.type == TYPE_FLOAT)
            {
                return value1.int_val > value2.float_val;
            }
        }
        case TYPE_FLOAT:
        {
            if (value2.type == TYPE_INT)
            {
                return value1.float_val > value2.int_val;
            }
            else if (value2.type == TYPE_FLOAT)
            {
                return value1.float_val > value2.float_val;
            }
        }
        case TYPE_STRING:
            return strcmp(value1.str_val.c_str(), value2.str_val.c_str()) > 0 ? true : false;
        }
        throw InternalError("Type_Not Find");
    }
};
