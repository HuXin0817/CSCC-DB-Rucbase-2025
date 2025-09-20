#pragma once

#include <climits>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>
#include <unordered_set>
#include <cmath>
#include <limits>

#include "execution_manager_finals.h"
#include "executor_abstract_finals.h"
#include "index/ix_memory_scan_finals.h"
#include "record/rm_scan_finals.h"

class IndexScanExecutor : public AbstractExecutor
{
private:
    TabMeta *tab_;
    RmFileHandle *fh_;
    std::vector<ColMeta> *cols_;
    IxIndexHandle *ih_;
    std::unique_ptr<IxScan> scan_;
    Context *context_;
    char *lower_key_ = nullptr;
    char *upper_key_ = nullptr;
    PoolManager *memory_pool_manager_;
    
    // 添加成员变量支持精确查找模式
    bool exact_match_mode_ = false;
    char *exact_key_ = nullptr;
    bool exact_key_found_ = false;
    bool exact_key_consumed_ = false;

public:
    IndexScanExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Condition> &conds, const IndexMeta &index_meta_, Context *context)
    {
        context_ = context;
        tab_ = sm_manager->db_.get_table(tab_name);
        fh_ = sm_manager->fhs_[tab_->fd_].get();
        cols_ = &tab_->cols;
        ih_ = sm_manager->ihs_[index_meta_.fd_].get();
        memory_pool_manager_ = sm_manager->memory_pool_manager_;



        // 检查是否为全等值查询（所有索引列都有等值条件）
        if (is_exact_match_query(conds, index_meta_.cols_)) {
            setup_exact_match_mode(conds, index_meta_.cols_);
            return;
        }

        // Initialize bounds directly
        lower_key_ = memory_pool_manager_->allocate(fh_->record_size);
        upper_key_ = memory_pool_manager_->allocate(fh_->record_size);

        // 只初始化索引涉及的列，减少不必要的初始化
        for (const auto &col : index_meta_.cols_) {
            switch (col.type)
            {
            case ColType::TYPE_INT:
            {
                int min_int = std::numeric_limits<int>::min();
                memcpy(lower_key_ + col.offset, &min_int, col.len);
                int max_int = std::numeric_limits<int>::max();
                memcpy(upper_key_ + col.offset, &max_int, col.len);
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                float float_min = std::numeric_limits<float>::lowest();
                memcpy(lower_key_ + col.offset, &float_min, col.len);
                float max_float = std::numeric_limits<float>::max();
                memcpy(upper_key_ + col.offset, &max_float, col.len);
                break;
            }
            case ColType::TYPE_STRING:
                memset(lower_key_ + col.offset, 0x00, col.len);
                memset(upper_key_ + col.offset, 0xff, col.len);
                break;
            default:
                break;
            }
        }

        // Apply conditions to refine bounds
        for (const auto &cond : conds)
        {
            auto &col_meta_ = tab_->get_col(cond.lhs_col.col_name);

            switch (cond.op)
            {
            case CompOp::OP_EQ:
            {
                update_bounds(upper_key_ + col_meta_.offset, lower_key_ + col_meta_.offset, 
                             cond.rhs_val.raw->data, col_meta_.type, col_meta_.len, true, true);
                break;
            }
            case CompOp::OP_LT:
            {
                update_upper_bound(upper_key_ + col_meta_.offset, cond.rhs_val.raw->data, 
                                  col_meta_.type, col_meta_.len, false);
                break;
            }
            case CompOp::OP_LE:
            {
                update_upper_bound(upper_key_ + col_meta_.offset, cond.rhs_val.raw->data, 
                                  col_meta_.type, col_meta_.len, true);
                break;
            }
            case CompOp::OP_GE:
            {
                update_lower_bound(lower_key_ + col_meta_.offset, cond.rhs_val.raw->data, 
                                  col_meta_.type, col_meta_.len, true);
                break;
            }
            case CompOp::OP_GT:
            {
                update_lower_bound(lower_key_ + col_meta_.offset, cond.rhs_val.raw->data, 
                                  col_meta_.type, col_meta_.len, false);
                break;
            }
            }
        }

        auto lower_position_ = ih_->lower_bound(lower_key_);
        auto upper_position_ = ih_->upper_bound(upper_key_);
        scan_ = std::make_unique<IxScan>(lower_position_, upper_position_);
    }

    ~IndexScanExecutor() override
    {
        // 优化：使用 RAII 管理内存，避免重复的空指针检查
        if (lower_key_) {
            memory_pool_manager_->deallocate(lower_key_, fh_->record_size);
            lower_key_ = nullptr;
        }
        if (upper_key_) {
            memory_pool_manager_->deallocate(upper_key_, fh_->record_size);
            upper_key_ = nullptr;
        }
        if (exact_key_) {
            memory_pool_manager_->deallocate(exact_key_, fh_->record_size);
            exact_key_ = nullptr;
        }
    }

    void beginTuple() override
    {
        // 精确匹配模式下，不需要额外操作
        if (exact_match_mode_) {
            return;
        }
        // Scan is already initialized in constructor, no need to call next() here
    }

    void nextTuple() override
    {
        if (exact_match_mode_) {
            // 精确匹配模式下，只有一个结果，nextTuple 后就结束
            exact_key_consumed_ = true;
            return;
        }
        scan_->next();
    }

    std::unique_ptr<RmRecord> Next() override { 
        if (exact_match_mode_) {
            if (!exact_key_found_ || exact_key_consumed_) {
                return nullptr;
            }
            // 从 find_entry 返回的迭代器获取 RID（存储在key中）
            auto it = ih_->find_entry(exact_key_);
            if (it != ih_->end()) {
                return fh_->get_record(*it);  // *it 就是 char* 形式的 RID
            }
            return nullptr;
        }
        
        if (!scan_->is_end()) {
            return fh_->get_record(scan_->rid()); 
        }
        return nullptr;
    }

    bool is_end() const override { 
        if (exact_match_mode_) {
            return !exact_key_found_ || exact_key_consumed_;
        }
        return scan_->is_end(); 
    }

    char *rid() const override { 
        if (exact_match_mode_) {
            if (!exact_key_found_ || exact_key_consumed_) {
                return nullptr;
            }
            auto it = ih_->find_entry(exact_key_);
            if (it != ih_->end()) {
                return *it;  // *it 就是 char* 形式的 RID
            }
            return nullptr;
        }
        
        if (!scan_->is_end()) {
            return scan_->rid();
        }
        return nullptr; 
    }

    size_t tupleLen() const override { return fh_->record_size; }

    const std::vector<ColMeta> &cols() const override { return *cols_; }

private:
    // 检查是否为精确匹配查询（所有索引列都有等值条件）
    static bool is_exact_match_query(const std::vector<Condition> &conds, const std::vector<ColMeta> &index_cols)
    {
        std::unordered_set<std::string> eq_cols;
        
        // 收集所有等值条件的列名
        for (const auto &cond : conds) {
            if (cond.op == CompOp::OP_EQ) {
                eq_cols.insert(cond.lhs_col.col_name);
            }
        }
        
        // 检查是否所有索引列都有等值条件
        for (const auto &col : index_cols) {
            if (eq_cols.find(col.name) == eq_cols.end()) {
                return false;
            }
        }
        
        return true;
    }
    
    // 设置精确匹配模式
    void setup_exact_match_mode(const std::vector<Condition> &conds, const std::vector<ColMeta> &index_cols)
    {
        exact_match_mode_ = true;
        exact_key_ = memory_pool_manager_->allocate(fh_->record_size);
        
        // 构建精确匹配的键
        for (const auto &col : index_cols) {
            for (const auto &cond : conds) {
                if (cond.lhs_col.col_name == col.name && cond.op == CompOp::OP_EQ) {
                    memcpy(exact_key_ + col.offset, cond.rhs_val.raw->data, col.len);
                    break;
                }
            }
        }
        
        // 检查键是否存在
        exact_key_found_ = ih_->exists_entry(exact_key_);
        exact_key_consumed_ = false;
    }

    // 优化的边界更新函数，减少分支和函数调用开销
    static inline void update_bounds(char *upper_key, char *lower_key, char *key, ColType type, int len, bool update_upper, bool update_lower)
    {
        switch (type)
        {
        case TYPE_INT:
        {
            int value = *(int *)key;
            if (update_upper) {
                int *upper_val = (int *)upper_key;
                if (*upper_val > value) *upper_val = value;
            }
            if (update_lower) {
                int *lower_val = (int *)lower_key;
                if (*lower_val < value) *lower_val = value;
            }
            break;
        }
        case TYPE_FLOAT:
        {
            float value = *(float *)key;
            if (update_upper) {
                float *upper_val = (float *)upper_key;
                if (*upper_val > value) *upper_val = value;
            }
            if (update_lower) {
                float *lower_val = (float *)lower_key;
                if (*lower_val < value) *lower_val = value;
            }
            break;
        }
        case TYPE_STRING:
        {
            if (update_upper && memcmp(upper_key, key, len) > 0) {
                memcpy(upper_key, key, len);
            }
            if (update_lower && memcmp(lower_key, key, len) < 0) {
                memcpy(lower_key, key, len);
            }
            break;
        }
        }
    }

    inline void update_upper_bound(char *upper_key, char *key, ColType type, int len, bool inclusive)
    {
        switch (type)
        {
        case TYPE_INT:
        {
            int value = *(int *)key;
            int *upper_val = (int *)upper_key;
            if (*upper_val > value || (*upper_val == value && !inclusive)) {
                *upper_val = inclusive ? value : value - 1;
            }
            break;
        }
        case TYPE_FLOAT:
        {
            float value = *(float *)key;
            float *upper_val = (float *)upper_key;
            if (*upper_val > value || (*upper_val == value && !inclusive)) {
                *upper_val = inclusive ? value : std::nextafter(value, -std::numeric_limits<float>::infinity());
            }
            break;
        }
        case TYPE_STRING:
        {
            if (memcmp(upper_key, key, len) > 0 || (!inclusive && memcmp(upper_key, key, len) == 0)) {
                memcpy(upper_key, key, len);
                if (!inclusive) {
                    // 对于非包含边界，需要调整字符串
                    for (int i = len - 1; i >= 0; --i) {
                        if (upper_key[i] > 0) {
                            upper_key[i]--;
                            break;
                        }
                    }
                }
            }
            break;
        }
        }
    }

    inline void update_lower_bound(char *lower_key, char *key, ColType type, int len, bool inclusive)
    {
        switch (type)
        {
        case TYPE_INT:
        {
            int value = *(int *)key;
            int *lower_val = (int *)lower_key;
            if (*lower_val < value || (*lower_val == value && !inclusive)) {
                *lower_val = inclusive ? value : value + 1;
            }
            break;
        }
        case TYPE_FLOAT:
        {
            float value = *(float *)key;
            float *lower_val = (float *)lower_key;
            if (*lower_val < value || (*lower_val == value && !inclusive)) {
                *lower_val = inclusive ? value : std::nextafter(value, std::numeric_limits<float>::infinity());
            }
            break;
        }
        case TYPE_STRING:
        {
            if (memcmp(lower_key, key, len) < 0 || (!inclusive && memcmp(lower_key, key, len) == 0)) {
                memcpy(lower_key, key, len);
                if (!inclusive) {
                    // 对于非包含边界，需要调整字符串
                    for (int i = len - 1; i >= 0; --i) {
                        if (lower_key[i] < 0xFF) {
                            lower_key[i]++;
                            break;
                        }
                    }
                }
            }
            break;
        }
        }
    }

    // 保留旧的函数作为备份，但标记为已废弃
    [[deprecated("Use update_bounds instead")]]
    static void upper_copy(char *upper_key, char *key, ColType type, int len)
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
            }
            break;
        }
        case TYPE_STRING:
        {
            auto cmp = memcmp(upper_key, key, len);
            if (cmp > 0)
            {
                memcpy(upper_key, key, len);
            }
            break;
        }
        }
    }

    [[deprecated("Use update_bounds instead")]]
    static void lower_copy(char *lower_key, char *key, ColType type, int len)
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
            }
            break;
        }
        case TYPE_STRING:
        {
            auto cmp = memcmp(lower_key, key, len);
            if (cmp < 0)
            {
                memcpy(lower_key, key, len);
            }
            break;
        }
        }
    }
};
