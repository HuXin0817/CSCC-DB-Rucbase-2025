//
// Created by 明泰 on 2025/8/12.
//

#ifndef RMDB_EXECUTION_SCALER_GROUP_FINALS_H
#define RMDB_EXECUTION_SCALER_GROUP_FINALS_H

#include "executor_abstract_finals.h"
#include "executor_index_scan_finals.h"


class ScalerAggPlanExecutor : public AbstractExecutor {
private:
    TabCol sel_col_;
    std::unique_ptr<AbstractExecutor> child_executor_;
    std::vector<ColMeta> output_cols_;
    Value result_;
    bool computed_;
    size_t len_;

public:
    ScalerAggPlanExecutor(std::unique_ptr<AbstractExecutor> child_executor, TabCol tc,
                          Context *context) : sel_col_(std::move(tc)), child_executor_(std::move(child_executor)), computed_(false) {
        initialize();
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return output_cols_; }

    void beginTuple() override {
        if (!computed_) {
            performAggregation();
            computed_ = true;
        }
    }

    void nextTuple() override {
        // 标量聚合只有一个结果，调用nextTuple后就结束
        computed_ = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (computed_) {
            return nullptr;
        }
        
        auto record = std::make_unique<RmRecord>(len_);
        
        // 将聚合结果写入记录
        switch (result_.type) {
            case TYPE_INT:
                memcpy(record->data, &result_.int_val, sizeof(int));
                break;
            case TYPE_FLOAT:
                memcpy(record->data, &result_.float_val, sizeof(float));
                break;
            case TYPE_STRING:
                memcpy(record->data, result_.str_val.c_str(), result_.str_val.length());
                break;
        }
        
        return record;
    }

    bool is_end() const override { return computed_; }

private:
    void can_fetch_from_index() {
        if (child_executor_) {
            // 修复：使用 dynamic_cast 而不是 dynamic_pointer_cast
            if (auto child = dynamic_cast<IndexScanExecutor*>(child_executor_.get())) {
                // if ()
                return;
            }

        }
    }

    void initialize() {
        // 初始化输出列
        ColMeta col_meta;
        col_meta.tab_name = sel_col_.tab_name;
        col_meta.name = sel_col_.col_name;
        col_meta.agg_func_type = sel_col_.aggFuncType;
        col_meta.offset = 0;
        
        // 根据聚合函数类型设置数据类型和长度
        switch (sel_col_.aggFuncType) {
            case ast::COUNT:
                col_meta.type = TYPE_INT;
                col_meta.len = sizeof(int);
                break;
            case ast::AVG:
                col_meta.type = TYPE_FLOAT;
                col_meta.len = sizeof(float);
                break;
            default: {
                // 对于SUM、MAX、MIN，类型与原列相同
                auto child_col = get_col(child_executor_->cols(), sel_col_);
                col_meta.type = child_col->type;
                col_meta.len = child_col->len;
                break;
            }
        }
        
        len_ = col_meta.len;
        output_cols_.push_back(col_meta);
    }

    void performAggregation() {
        child_executor_->beginTuple();
        
        // 初始化聚合值
        bool first = true;
        int count = 0;
        float sum = 0.0f;
        
        for (; !child_executor_->is_end(); child_executor_->nextTuple()) {
            auto record = child_executor_->Next();
            
            if (sel_col_.aggFuncType == ast::COUNT) {
                count++;
                continue;
            }
            
            // 获取列值
            auto col_meta = get_col(child_executor_->cols(), sel_col_);
            Value value;
            
            switch (col_meta->type) {
                case TYPE_INT:
                    value.set_int(*reinterpret_cast<int*>(record->data + col_meta->offset));
                    break;
                case TYPE_FLOAT:
                    value.set_float(*reinterpret_cast<float*>(record->data + col_meta->offset));
                    break;
                case TYPE_STRING:
                    value.set_str(std::string(record->data + col_meta->offset, col_meta->len));
                    break;
            }
            
            // 执行聚合操作
            switch (sel_col_.aggFuncType) {
                case ast::SUM:
                    if (first) {
                        result_ = value;
                        first = false;
                    } else {
                        if (value.type == TYPE_INT) {
                            result_.int_val += value.int_val;
                        } else if (value.type == TYPE_FLOAT) {
                            result_.float_val += value.float_val;
                        }
                    }
                    break;
                case ast::AVG:
                    count++;
                    if (value.type == TYPE_INT) {
                        sum += value.int_val;
                    } else if (value.type == TYPE_FLOAT) {
                        sum += value.float_val;
                    }
                    break;
                case ast::MAX:
                    if (first) {
                        result_ = value;
                        first = false;
                    } else {
                        if ((value.type == TYPE_INT && result_.int_val < value.int_val) ||
                            (value.type == TYPE_FLOAT && result_.float_val < value.float_val) ||
                            (value.type == TYPE_STRING && result_.str_val < value.str_val)) {
                            result_ = value;
                        }
                    }
                    break;
                case ast::MIN:
                    if (first) {
                        result_ = value;
                        first = false;
                    } else {
                        if ((value.type == TYPE_INT && result_.int_val > value.int_val) ||
                            (value.type == TYPE_FLOAT && result_.float_val > value.float_val) ||
                            (value.type == TYPE_STRING && result_.str_val > value.str_val)) {
                            result_ = value;
                        }
                    }
                    break;
            }
        }
        
        // 设置最终结果
        if (sel_col_.aggFuncType == ast::COUNT) {
            result_.set_int(count);
        } else if (sel_col_.aggFuncType == ast::AVG) {
            result_.set_float(count > 0 ? sum / count : 0.0f);
        }
    }
};

#endif //RMDB_EXECUTION_SCALER_GROUP_FINALS_H
