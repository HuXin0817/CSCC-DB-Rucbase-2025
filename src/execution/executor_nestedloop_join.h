#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> left_;  // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_; // 右儿子节点（需要join的表）
    size_t len_;                              // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;               // join后获得的记录的字段
    std::vector<Condition> fed_conds_;        // join条件
    bool isEnd;
    std::unique_ptr<RmRecord> left_record_; // 当前左表记录

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, std::vector<Condition> conds) : left_(std::move(left)), right_(std::move(right)), fed_conds_(std::move(conds)), isEnd(false)
    {
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols)
        {
            col.offset += left_->tupleLen(); // 调整右表字段偏移
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
    }

    void beginTuple() override
    {
        left_->beginTuple();
        right_->beginTuple();
        if (left_->is_end())
        {
            isEnd = true;
            return;
        }
        left_record_ = left_->Next();

        // 初始化时找到第一个符合条件的记录组合
        find_next_valid_tuple();
    }

    void find_next_valid_tuple()
    {
        while (!left_->is_end())
        {
            while (!right_->is_end())
            {
                auto right_record = right_->Next();
                if (right_record && satisfies_join_conds(left_record_.get(), right_record.get()))
                {
                    // 找到第一个符合条件的记录组合，退出
                    return;
                }
                right_->nextTuple();
            }
            right_->beginTuple(); // 重置右表的迭代器
            left_->nextTuple();   // 移动到左表的下一条记录
            if (!left_->is_end())
            {
                left_record_ = left_->Next(); // 获取左表的新记录
            }
        }
        isEnd = true; // 如果所有可能的记录组合都已检查完毕
    }

    void nextTuple() override
    {
        right_->nextTuple();
        find_next_valid_tuple();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        while (!is_end())
        {
            auto right_record = right_->Next();
            if (right_record && satisfies_join_conds(left_record_.get(), right_record.get()))
            {
                // 如果满足连接条件，合并记录并返回
                return merge_records(left_record_.get(), right_record.get());
            }
            nextTuple(); // 移动到下一个有效记录组合
        }
        return nullptr;
    }

    bool satisfies_join_conds(const RmRecord *left, const RmRecord *right) const
    {
        // 检查所有连接条件是否都满足
        return std::all_of(fed_conds_.begin(), fed_conds_.end(), [&](const Condition &cond)
                           { return evaluate_cond(left, right, cond); });
    }

    std::unique_ptr<RmRecord> merge_records(const RmRecord *left, const RmRecord *right)
    {
        auto combined = std::make_unique<RmRecord>();
        combined->data = new char[len_];
        std::memcpy(combined->data, left->data, left_->tupleLen());
        std::memcpy(combined->data + left_->tupleLen(), right->data, right_->tupleLen());
        return combined;
    }

    bool evaluate_cond(const RmRecord *left, const RmRecord *right, const Condition &cond) const
    {
        // 获取左侧字段数据
        char *lhs_buf = left->data + cond.lhs->offset;
        char *rhs_buf = nullptr;

        if (!cond.is_rhs_val)
        {
            // 获取右侧字段数据
            rhs_buf = right->data + cond.rhs->offset;
        }
        else
        {
            // 如果右侧是值，则根据值的类型处理
            rhs_buf = cond.rhs_val.raw->data;
        }

        // 根据列的类型来比较数据
        switch (cond.lhs->type)
        {
        case TYPE_INT:
        {
            int lhs_value = *reinterpret_cast<int *>(lhs_buf);
            int rhs_value = cond.is_rhs_val ? cond.rhs_val.int_val : *reinterpret_cast<int *>(rhs_buf);
            return compare_values(lhs_value, rhs_value, cond.op);
        }
        case TYPE_FLOAT:
        {
            float lhs_value = *reinterpret_cast<float *>(lhs_buf);
            float rhs_value = cond.is_rhs_val ? cond.rhs_val.float_val : *reinterpret_cast<float *>(rhs_buf);
            return compare_values(lhs_value, rhs_value, cond.op);
        }
        case TYPE_STRING:
        {
            // 使用 std::string::compare 实现字符串比较
            std::string lhs_value(lhs_buf, strnlen(lhs_buf, cond.lhs->len));
            std::string rhs_value = cond.is_rhs_val ? std::string(cond.rhs_val.str_val) : std::string(rhs_buf, strnlen(rhs_buf, cond.rhs->len));
            return compare_result(lhs_value.compare(rhs_value), cond.op);
        }
        default:
            return false; // 对于未知类型，返回 false
        }
    }

    Rid &rid() override { return _abstract_rid; }

    size_t tupleLen() const override
    {
        return left_->tupleLen() + right_->tupleLen(); // 返回左右节点的记录长度之和
    }

    const std::vector<ColMeta> &cols() const override
    {
        return cols_; // 直接返回已经合并和调整过的列元数据
    }

    bool is_end() const override
    {
        return isEnd; // 返回当前联接操作的结束状态
    }
};
