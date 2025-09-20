#pragma once

#include <memory>
#include <string>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor
{
private:
    std::string tab_name_;             // 表的名称
    std::vector<Condition> conds_;     // scan的条件
    RmFileHandle *fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // scan后生成的记录的字段
    size_t len_;                       // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_; // 同conds_，两个字段相同

    Rid rid_{};
    std::unique_ptr<RecScan> scan_; // table_iterator

    SmManager *sm_manager_;

public:
    /**
     * @brief 构造函数
     *
     * @param sm_manager 存储管理器指针
     * @param tab_name 表名
     * @param conds 扫描条件
     * @param context 执行上下文
     */
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) : tab_name_(std::move(tab_name)), conds_(std::move(conds)), sm_manager_(sm_manager)
    {
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);

        if (sm_manager_->fhs_.find(tab_name_) == sm_manager_->fhs_.end())
        {
            throw InternalError("sm_manager_->fhs_.at (key) error");
        }
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = fh_->record_size();

        context_ = context;

        fed_conds_ = conds_;
    }

    size_t tupleLen() const override { return len_; };

    const std::vector<ColMeta> &cols() const override { return cols_; }

    void beginTuple() override
    {
        // 初始化子查询
        for (auto &cond : fed_conds_)
        {
            if (!cond.is_subquery)
                continue;
        }
        // 初始化扫描表
        scan_ = std::make_unique<RmScan>(fh_);
        context_->lock_mgr_->lock_shared_on_table(context_->txn_, tab_name_);
        find_next_valid_tuple();
    }

    void nextTuple() override
    {
        scan_->next();
        find_next_valid_tuple();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (!is_end())
        {
            std::unique_ptr<RmRecord> rid_record = fh_->get_record(rid_, context_);
            std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(rid_record->size, rid_record->data);
            sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), rid_.page_no}, false);
            return ret;
        }
        return nullptr;
    }

    bool is_end() const override { return rid_.page_no == RM_NO_PAGE; }

    Rid &rid() override { return rid_; }

private:
    void find_next_valid_tuple()
    {
        while (!scan_->is_end())
        {
            rid_ = scan_->rid();
            std::unique_ptr<RmRecord> rid_record = fh_->get_record(rid_, context_);
            if (satisfies_conds(rid_record))
            {
                sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), rid_.page_no}, false);
                return;
            }
            sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), rid_.page_no}, false);
            scan_->next();
        }
        rid_.page_no = RM_NO_PAGE; // 设置 rid_ 以指示结束
    }

    bool satisfies_conds(const std::unique_ptr<RmRecord> &record) const
    {
        return std::all_of(conds_.begin(), conds_.end(), [this, &record](const Condition &cond)
                           {
            if (!cond.is_subquery) {
                return compare(record, cond);
            } else {
                return compare_subquery(record, cond);
            } });
    }

    bool compare_subquery(const std::unique_ptr<RmRecord> &record, const Condition &cond) const
    {
        char *lhs_buf = record->data + cond.lhs->offset;
        Value lhs;

        switch (cond.lhs->type)
        {
        case TYPE_INT:
        {
            int lhs_value = *reinterpret_cast<int *>(lhs_buf);
            lhs.set_int(lhs_value);
            break;
        }
        case TYPE_FLOAT:
        {
            float lhs_value = *reinterpret_cast<float *>(lhs_buf);
            lhs.set_float(lhs_value);
            break;
        }
        case TYPE_STRING:
        {
            std::string lhs_value(lhs_buf, strnlen(lhs_buf, cond.lhs->len));
            lhs.set_str(lhs_value);
            break;
        }
        }

        if (is_scalar_subquery(cond))
        {
            if (cond.subQuery->result.size() != 1)
            {
                throw RMDBError("Scalar subquery result size is not 1");
            }

            const auto &subQueryResult = *cond.subQuery->result.begin();
            return compare_value(lhs, subQueryResult, cond.op);
        }
        if (lhs.type == TYPE_INT && cond.subQuery->subquery_type == TYPE_FLOAT)
        {
            lhs.set_float((float)lhs.int_val);
        }

        bool found = (cond.subQuery->result.find(lhs) != cond.subQuery->result.end());
        return (cond.op == OP_IN) == found;
    }

    static bool compare_value(const Value &lhs_, const Value &rhs_, CompOp op)
    {
        Value lhs = lhs_;
        Value rhs = rhs_;
        if (!cast_type(lhs, rhs))
        {
            throw RMDBError("Type mismatch");
        }

        switch (op)
        {
        case OP_EQ:
            return lhs == rhs;
        case OP_NE:
            return lhs != rhs;
        case OP_LT:
            return lhs < rhs;
        case OP_GT:
            return lhs > rhs;
        case OP_LE:
            return lhs <= rhs;
        case OP_GE:
            return lhs >= rhs;
        default:
            throw RMDBError("Invalid comparison operator");
        }
    }

    static bool cast_type(Value &lhs, Value &rhs)
    {
        // Add logic to determine if a type can be cast to another type
        if (lhs.type == rhs.type)
            return true;
        else if (lhs.type == TYPE_INT && rhs.type == TYPE_FLOAT)
        {
            lhs.set_float((float)lhs.int_val);
            return true;
        }
        else if (lhs.type == TYPE_FLOAT && rhs.type == TYPE_INT)
        {
            rhs.set_float((float)rhs.int_val);
            return true;
        }
        return false;
    }

    inline static bool is_scalar_subquery(const Condition &cond)
    {
        return cond.op != OP_IN && cond.op != OP_NOT_IN;
    }
};
