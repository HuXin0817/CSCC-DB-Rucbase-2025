#pragma once

#include <fstream>
#include <functional>
#include <memory>
#include <queue>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

#pragma once
#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>
#include <memory>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta col_;  // 单字段排序
    bool is_desc_; // 题目要求升序
    size_t tuple_num;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    size_t current_index;
    size_t len_;

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_col) : prev_(std::move(prev)), current_index(0)
    {
        col_ = *get_col(prev_->cols(), sel_col);
        len_ = prev_->tupleLen();
        tuple_num = 0;
        is_desc_ = false;
    }

    void beginTuple() override
    {
        current_index = 0;
        if (!tuples_.empty())
            return;
        get_sort_next_tuples();
    }

    void nextTuple() override
    {
        if (current_index < tuples_.size())
        {
            ++current_index;
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (is_end())
        {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*tuples_[current_index]);
    }

    bool is_end() const override { return tuples_.empty() || current_index >= tuples_.size(); }

    Rid &rid() override { return _abstract_rid; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    ~SortExecutor() override
    {
        // 清理资源
    }

private:
    void get_sort_next_tuples()
    {
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple())
        {
            tuple_num++;
            tuples_.emplace_back(prev_->Next());
        }
        if (tuples_.empty())
            return;

        std::sort(tuples_.begin(), tuples_.end(), [this](const std::unique_ptr<RmRecord> &a, const std::unique_ptr<RmRecord> &b)
                  { return compareRecords(a, b); });
    }

    bool compareRecords(const std::unique_ptr<RmRecord> &a, const std::unique_ptr<RmRecord> &b) const
    {
        Value lhs = getValue(a, col_.offset, col_.type);
        Value rhs = getValue(b, col_.offset, col_.type);
        if (lhs != rhs)
        {
            return is_desc_ ? lhs > rhs : lhs < rhs;
        }
        return false;
    }

    Value getValue(const std::unique_ptr<RmRecord> &record, size_t offset, ColType col_type) const
    {
        const char *buf = record->data + offset;
        Value value;
        switch (col_type)
        {
        case TYPE_INT:
        {
            int int_value = *reinterpret_cast<const int *>(buf);
            value.set_int(int_value);
            break;
        }
        case TYPE_FLOAT:
        {
            float float_value = *reinterpret_cast<const float *>(buf);
            value.set_float(float_value);
            break;
        }
        case TYPE_STRING:
        {
            std::string str_value(buf, strnlen(buf, 256)); // 假设最大字符串长度为256
            value.set_str(str_value);
            break;
        }
        default:
            throw RMDBError("Unsupported column type");
        }
        return value;
    }
};
