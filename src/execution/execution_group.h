//
// Created by root on 24-5-29.
//
// agg_plan_executor.h
#pragma once

#include <cfloat>
#include <climits>
#include <unordered_map>

#include "executor_abstract.h"

class AggPlanExecutor : public AbstractExecutor
{
private:
    std::vector<TabCol> sel_cols_;
    std::vector<TabCol> group_by_cols_;
    Context *context_;
    std::unique_ptr<AbstractExecutor> child_executor_;
    std::vector<ColMeta> output_cols_;

    std::vector<std::string> insert_order_;
    std::unordered_map<std::string, std::vector<Value>> group_map_; // 保证顺序的一致性

    std::vector<RmRecord> results_;
    std::vector<RmRecord>::iterator result_it_;
    int TupleLen;

public:
    AggPlanExecutor(std::unique_ptr<AbstractExecutor> child_executor, std::vector<TabCol> group_by_cols, std::vector<TabCol> sel_cols, Context *context) : sel_cols_(std::move(sel_cols)), group_by_cols_(std::move(group_by_cols)), context_(context), child_executor_(std::move(child_executor))
    {
        // Initialize output columns
        TupleLen = 0;
        int offset = 0;

        for (const auto &col : sel_cols_)
        {
            if (col.aggFuncType == ast::COUNT)
            {
                ColMeta col_meta(col.tab_name, col.col_name, TYPE_INT, col.aggFuncType, sizeof(int), offset, false);
                TupleLen += col_meta.len;
                offset += col_meta.len;
                output_cols_.push_back(col_meta);
            }
            else
            {
                if (col.col_name == "*")
                {
                    throw InvalidAggError("*", std::to_string(col.aggFuncType));
                }
                auto col_meta = *get_col(child_executor_->cols(), col);
                TupleLen += col_meta.len;
                col_meta.offset = offset;
                col_meta.agg_func_type = col.aggFuncType;
                offset += col_meta.len;
                output_cols_.push_back(col_meta);
            }
        }
    }

    size_t tupleLen() const override { return TupleLen; }

    const std::vector<ColMeta> &cols() const override { return output_cols_; }

    void beginTuple() override
    {
        child_executor_->beginTuple();
        performAggregation();
        result_it_ = results_.begin();
    }

    void nextTuple() override
    {
        if (result_it_ != results_.end())
        {
            ++result_it_;
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (result_it_ == results_.end())
        {
            return nullptr;
        }
        auto record = std::make_unique<RmRecord>(*result_it_);
        return record;
    }

    bool is_end() const override { return result_it_ == results_.end(); }

    Rid &rid() override
    {
        return _abstract_rid;
        ;
    }

private:
    // Perform aggregation on the child executor
    void performAggregation()
    {
        while (!child_executor_->is_end())
        {
            auto record = child_executor_->Next();
            child_executor_->nextTuple();
            if (record == nullptr)
            {
                break;
            }

            std::string key = generateGroupByKey(*record);
            aggregateValues(key, *record);
        }
        generateResults();
    }

    std::string generateGroupByKey(const RmRecord &record)
    {
        if (group_by_cols_.empty())
        {
            return "__no_group_by__";
        }

        std::string key;
        for (const auto &col : group_by_cols_)
        {
            auto col_meta = get_col(child_executor_->cols(), col);
            if (col_meta->type == TYPE_INT)
            {
                int value = *reinterpret_cast<const int *>(record.data + col_meta->offset);
                key.append(reinterpret_cast<const char *>(&value), sizeof(int));
            }
            else if (col_meta->type == TYPE_FLOAT)
            {
                float value = *reinterpret_cast<const float *>(record.data + col_meta->offset);
                key.append(reinterpret_cast<const char *>(&value), sizeof(float));
            }
            else if (col_meta->type == TYPE_STRING)
            {
                key.append(record.data + col_meta->offset, col_meta->len);
            }
            else
            {
                throw RMDBError("Unsupported column type in GROUP BY clause");
            }
        }
        return key;
    }

    void aggregateValues(const std::string &key, const RmRecord &record)
    {
        if (group_map_.find(key) == group_map_.end())
        {
            init_map(key, record);
        }

        for (size_t i = 0; i < sel_cols_.size(); ++i)
        {
            const auto &sel_col = sel_cols_[i];
            Value value;

            if (sel_col.aggFuncType == ast::COUNT && sel_col.col_name == "*")
            {
                group_map_[key][i].int_val += 1;
                continue;
            }

            auto col_meta = get_col(child_executor_->cols(), {sel_col.tab_name, sel_col.col_name});
            if (col_meta->type == TYPE_INT)
            {
                value.set_int(*reinterpret_cast<int *>(record.data + col_meta->offset));
            }
            else if (col_meta->type == TYPE_FLOAT)
            {
                value.set_float(*reinterpret_cast<float *>(record.data + col_meta->offset));
            }
            else if (col_meta->type == TYPE_STRING)
            {
                value.set_str(std::string(record.data + col_meta->offset, col_meta->len));
            }

            switch (sel_col.aggFuncType)
            {
            case ast::COUNT:
                if (value.type != TYPE_STRING || !is_all_null_chars(value.str_val))
                {
                    group_map_[key][i].int_val += 1;
                }
                break;
            case ast::SUM:
                if (value.type == TYPE_INT)
                {
                    group_map_[key][i].int_val += value.int_val;
                }
                else if (value.type == TYPE_FLOAT)
                {
                    group_map_[key][i].float_val += value.float_val;
                }
                break;
            case ast::MAX:
                if (value.type == TYPE_INT && group_map_[key][i].int_val < value.int_val)
                {
                    group_map_[key][i].int_val = value.int_val;
                }
                else if (value.type == TYPE_FLOAT && group_map_[key][i].float_val < value.float_val)
                {
                    group_map_[key][i].float_val = value.float_val;
                }
                else if (value.type == TYPE_STRING && group_map_[key][i].str_val < value.str_val)
                {
                    group_map_[key][i].set_str(value.str_val);
                }
                break;
            case ast::MIN:
                if (value.type == TYPE_INT && group_map_[key][i].int_val > value.int_val)
                {
                    group_map_[key][i].int_val = value.int_val;
                }
                else if (value.type == TYPE_FLOAT && group_map_[key][i].float_val > value.float_val)
                {
                    group_map_[key][i].float_val = value.float_val;
                }
                else if (value.type == TYPE_STRING && group_map_[key][i].str_val > value.str_val)
                {
                    group_map_[key][i].set_str(value.str_val);
                }
                break;
            case ast::default_type:
                break;
            default:
                throw RMDBError("Unsupported aggregate function");
            }
        }
    }

    void init_map(const std::string &key, const RmRecord &record)
    {
        insert_order_.push_back(key);
        group_map_[key] = std::vector<Value>(sel_cols_.size());
        // Initialize the default values for each aggregation function
        for (size_t i = 0; i < sel_cols_.size(); ++i)
        {
            auto agg_type = sel_cols_[i].aggFuncType;

            if (agg_type == ast::COUNT)
            {
                group_map_[key][i].set_int(0);
            }
            else if (agg_type == ast::SUM || agg_type == ast::MAX || agg_type == ast::MIN)
            {
                auto col = get_col(child_executor_->cols(), {sel_cols_[i].tab_name, sel_cols_[i].col_name});
                switch (col->type)
                {
                case TYPE_INT:
                    if (agg_type == ast::MIN)
                        group_map_[key][i].set_int(std::numeric_limits<int>::max());
                    else if (agg_type == ast::MAX)
                        group_map_[key][i].set_int(std::numeric_limits<int>::min());
                    else
                        group_map_[key][i].set_int(0);
                    break;

                case TYPE_FLOAT:
                    if (agg_type == ast::MIN)
                        group_map_[key][i].set_float(std::numeric_limits<float>::max());
                    else if (agg_type == ast::MAX)
                        group_map_[key][i].set_float(std::numeric_limits<float>::lowest());
                    else
                        group_map_[key][i].set_float(0.0f);
                    break;
                case TYPE_STRING:
                    if (agg_type == ast::MIN)
                    {
                        auto str_len = col->len;
                        std::string max_str(str_len, '~');
                        group_map_[key][i].set_str(max_str);
                    }
                    else if (agg_type == ast::MAX)
                    {
                        // Initialize with a low string value for MAX
                        group_map_[key][i].set_str("");
                    }
                    else
                    {
                        throw RMDBError("Unsupported aggregate function for string type");
                    }
                    break;
                default:
                    throw RMDBError("Unsupported column type");
                }
            }
            else
            {
                // 被group by的列,非聚合函数
                auto col = get_col(child_executor_->cols(), {sel_cols_[i].tab_name, sel_cols_[i].col_name});
                if (col->type == TYPE_INT)
                {
                    group_map_[key][i].set_int(*reinterpret_cast<int *>(record.data + col->offset));
                }
                else if (col->type == TYPE_FLOAT)
                {
                    group_map_[key][i].set_float(*reinterpret_cast<float *>(record.data + col->offset));
                }
                else if (col->type == TYPE_STRING)
                {
                    group_map_[key][i].set_str(std::string(record.data + col->offset, col->len));
                }
            }
        }
    }

    void generateResults()
    {
        if (group_map_.empty())
        {
            // Handle the case where no data was aggregated but we still need to return a result for COUNT()
            RmRecord record(TupleLen);
            char *data_ptr = record.data;

            bool has_count = false;

            // Initialize aggregated values to their default empty values
            for (const auto &sel_col : sel_cols_)
            {
                if (sel_col.aggFuncType == ast::COUNT)
                {
                    int count_value = 0;
                    has_count = true;
                    std::memcpy(data_ptr, &count_value, sizeof(count_value));
                    data_ptr += sizeof(count_value);
                }
                else
                {
                    auto col = get_col(child_executor_->cols(), {sel_col.tab_name, sel_col.col_name});
                    if (col->type == TYPE_INT)
                    {
                        int default_value = INT_MAX;
                        std::memcpy(data_ptr, &default_value, sizeof(default_value));
                        data_ptr += sizeof(default_value);
                    }
                    else if (col->type == TYPE_FLOAT)
                    {
                        float default_value = FLT_MAX;
                        std::memcpy(data_ptr, &default_value, sizeof(default_value));
                        data_ptr += sizeof(default_value);
                    }
                    else if (col->type == TYPE_STRING)
                    {
                        std::string default_value;
                        std::memcpy(data_ptr, default_value.c_str(), col->len);
                        data_ptr += col->len;
                    }
                }
            }
            if (has_count && sel_cols_.size() == 1 && group_by_cols_.empty())
                results_.push_back(std::move(record));
        }
        else
        {
            // Process the aggregated data as usual
            for (const auto &entry : insert_order_)
            {
                RmRecord record(TupleLen);
                char *data_ptr = record.data;

                // Copy aggregated values into result record
                for (const auto &val : group_map_[entry])
                {
                    if (val.type == TYPE_INT)
                    {
                        std::memcpy(data_ptr, &val.int_val, sizeof(val.int_val));
                        data_ptr += sizeof(val.int_val);
                    }
                    else if (val.type == TYPE_FLOAT)
                    {
                        std::memcpy(data_ptr, &val.float_val, sizeof(val.float_val));
                        data_ptr += sizeof(val.float_val);
                    }
                    else if (val.type == TYPE_STRING)
                    {
                        std::memcpy(data_ptr, val.str_val.c_str(), val.str_val.size());
                        data_ptr += val.str_val.size();
                    }
                }
                results_.push_back(std::move(record));
            }
        }
    }
    static bool is_all_null_chars(const std::string &str)
    {
        return std::all_of(str.begin(), str.end(), [](char c)
                           { return c == '\000'; });
    }
};

class HavingPlanExecutor : public AbstractExecutor
{
private:
    Context *context_;
    std::unique_ptr<AbstractExecutor> child_executor_;
    std::vector<TabCol> sel_cols_;
    std::vector<HavingCond> having_conds_;

    int tuplelen;
    std::vector<ColMeta> output_cols_;
    std::vector<RmRecord> results_;
    std::vector<RmRecord>::iterator result_it_;

public:
    HavingPlanExecutor(std::unique_ptr<AbstractExecutor> child_executor, std::vector<TabCol> sel_cols, std::vector<HavingCond> having_conds, Context *context) : context_(context), child_executor_(std::move(child_executor)), sel_cols_(std::move(sel_cols)), having_conds_(std::move(having_conds))
    {
        tuplelen = 0;
        int offset = 0;
        for (const auto &col : sel_cols_)
        {
            if (col.aggFuncType == ast::COUNT)
            {
                ColMeta col_meta(col.tab_name, col.col_name, TYPE_INT, col.aggFuncType, sizeof(int), offset, false);
                offset += col_meta.len;
                output_cols_.push_back(col_meta);
            }
            else
            {
                if (col.col_name == "*")
                {
                    throw InvalidAggError("*", std::to_string(col.aggFuncType));
                }
                auto col_meta = *get_col_type(child_executor_->cols(), col, col.aggFuncType);
                col_meta.offset = offset;
                offset += col_meta.len;
                output_cols_.push_back(col_meta);
            }
        }
        tuplelen = offset;
    }

    size_t tupleLen() const override { return tuplelen; }

    const std::vector<ColMeta> &cols() const override { return output_cols_; }

    void beginTuple() override
    {
        if (!dynamic_cast<AggPlanExecutor *>(child_executor_.get()))
        {
            throw RMDBError("HAVING clause must be used with GROUP BY clause");
        }
        child_executor_->beginTuple();
        applyHavingConditions();
        generateResults();
        result_it_ = results_.begin();
    }

    void nextTuple() override
    {
        if (result_it_ != results_.end())
        {
            ++result_it_;
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (result_it_ == results_.end())
        {
            return nullptr;
        }
        auto record = std::make_unique<RmRecord>(*result_it_);
        return record;
    }

    bool is_end() const override { return result_it_ == results_.end(); }

    Rid &rid() override { return _abstract_rid; }

private:
    void applyHavingConditions()
    {
        while (!child_executor_->is_end())
        {
            auto record = child_executor_->Next();
            child_executor_->nextTuple();
            if (record == nullptr)
            {
                break;
            }

            if (checkHavingConditions(record))
            {
                results_.push_back(std::move(*record));
            }
        }
    }

    bool checkHavingConditions(const std::unique_ptr<RmRecord> &record)
    {
        for (HavingCond &cond : having_conds_)
        {
            if (!evaluateCondition(record, cond))
            {
                return false;
            }
        }
        return true;
    }

    bool evaluateCondition(const std::unique_ptr<RmRecord> &record, HavingCond &cond)
    {
        // Evaluate the left-hand side column value from the record
        auto lhs_value = getColValue(record, cond.lhs_col);

        // Compare the lhs_value with rhs_val
        if (!can_cast_type(lhs_value.type, cond.rhs_val.type) && !can_cast_type(cond.rhs_val.type, lhs_value.type))
        {
            throw IncompatibleTypeError(coltype2str(lhs_value.type), coltype2str(cond.rhs_val.type));
        }

        if (lhs_value.type != cond.rhs_val.type)
        {
            if (can_cast_type(cond.rhs_val.type, lhs_value.type))
            {
                cast_value(cond.rhs_val, lhs_value.type);
            }
            else
            {
                throw RMDBError("Unsupported type casting");
            }
        }

        // Perform the comparison based on the operator
        switch (cond.op)
        {
        case OP_EQ:
            return lhs_value == cond.rhs_val;
        case OP_NE:
            return lhs_value != cond.rhs_val;
        case OP_LT:
            return lhs_value < cond.rhs_val;
        case OP_LE:
            return lhs_value <= cond.rhs_val;
        case OP_GT:
            return lhs_value > cond.rhs_val;
        case OP_GE:
            return lhs_value >= cond.rhs_val;
        default:
            throw RMDBError("Unsupported comparison operator");
        }
    }

    Value getColValue(const std::unique_ptr<RmRecord> &record, const TabCol &col)
    {
        auto col_meta = get_col_type(child_executor_->cols(), col, col.aggFuncType);
        Value value;
        if (col_meta->type == TYPE_INT)
        {
            value.set_int(*reinterpret_cast<const int *>(record->data + col_meta->offset));
        }
        else if (col_meta->type == TYPE_FLOAT)
        {
            value.set_float(*reinterpret_cast<const float *>(record->data + col_meta->offset));
        }
        else if (col_meta->type == TYPE_STRING)
        {
            value.set_str(std::string(record->data + col_meta->offset, col_meta->len));
        }
        else
        {
            throw RMDBError("Unsupported column type in HAVING clause");
        }
        return value;
    }

    bool can_cast_type(ColType from, ColType to)
    {
        if (from == to)
            return true;
        if (from == TYPE_INT && to == TYPE_FLOAT)
            return true;
        if (from == TYPE_FLOAT && to == TYPE_INT)
            return true;
        return false;
    }

    void cast_value(Value &val, ColType to)
    {
        if (val.type == TYPE_INT && to == TYPE_FLOAT)
        {
            int int_val = val.int_val;
            val.type = TYPE_FLOAT;
            val.float_val = static_cast<float>(int_val);
        }
        else if (val.type == TYPE_FLOAT && to == TYPE_INT)
        {
            float float_val = val.float_val;
            val.type = TYPE_INT;
            val.int_val = static_cast<int>(float_val);
        }
        else
        {
            throw IncompatibleTypeError(coltype2str(val.type), coltype2str(to));
        }
    }

    std::vector<ColMeta>::const_iterator get_col_type(const std::vector<ColMeta> &rec_cols, const TabCol &target, const ast::AggFuncType aggFuncType)
    {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col)
                                {
            if (col.tab_name == target.tab_name && col.name == target.col_name && col.agg_func_type == target.aggFuncType) {
                return true;
            }
            return false; });

        if (pos == rec_cols.end())
        {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }

        return pos;
    }

    void generateResults()
    {
        auto child_col = child_executor_->cols();
        std::vector<ColMeta> sel_col = output_cols_;

        std::vector<RmRecord> final_results;
        for (const auto &result_it : results_)
        {
            RmRecord new_record(tupleLen());
            char *data_ptr = new_record.data;

            for (const auto &col : sel_col)
            {
                TabCol temp;
                temp.tab_name = col.tab_name;
                temp.col_name = col.name;
                temp.aggFuncType = col.agg_func_type;
                auto col_meta_it = get_col_type(child_col, temp, col.agg_func_type);

                const ColMeta &col_meta = *col_meta_it;
                std::memcpy(data_ptr, result_it.data + col_meta.offset, col_meta.len);
                data_ptr += col_meta.len;
            }

            final_results.push_back(std::move(new_record));
        }

        results_ = std::move(final_results);
    }
};
