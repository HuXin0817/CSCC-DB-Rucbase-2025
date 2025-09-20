#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
/**
 * @class UpdateExecutor
 * @brief 执行更新操作的执行器
 */
class UpdateExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;
    bool is_locked = false;

public:
    /**
     * @brief 构造函数
     *
     * @param sm_manager 存储管理器指针
     * @param tab_name 表名
     * @param set_clauses 更新操作的设置子句
     * @param conds 更新操作的条件
     * @param rids 需要更新的记录的位置
     * @param context 执行上下文
     */
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses, std::vector<Condition> conds, std::vector<Rid> rids, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
    }
    /**
     * @brief 执行更新操作
     * @return 更新后的记录
     */
    std::unique_ptr<RmRecord> Next() override
    {
        for (const Rid &rid : rids_)
        {
            // 获取当前记录
            std::unique_ptr<RmRecord> old_record = fh_->get_record(rid, context_);

            RmRecord new_record = *old_record;

            // 检查记录是否满足条件
            if (satisfies_conds(old_record))
            {
                // 先删除旧的索引
                char *record_data = old_record->data;

                // 删除每个索引中对应的条目
                for (auto &index : tab_.indexes)
                {
                    auto idx_name = IxManager::get_index_name(tab_name_, index.cols);
                    auto key = new_char(index.col_tot_len);
                    int offset = 0;
                    for (size_t i = 0; i < static_cast<unsigned long>(index.col_num); ++i)
                    {
                        std::memcpy(key.get() + offset, record_data + index.cols[i].offset, index.cols[i].len);
                        offset += index.cols[i].len;
                    }

                    std::vector<ColType> col_types;
                    std::vector<int> col_lens;
                    for (const auto &col : index.cols)
                    {
                        col_types.push_back(col.type);
                        col_lens.push_back(col.len);
                    }

                    context_->lock_mgr_->lock_exclusive_gap_on_index(context_->txn_, idx_name, key.get(), key.get(), true, true, col_types, col_lens);
                }

                // 更新记录
                update_record(&new_record);

                // 插入新的索引
                char *new_record_data = new_record.data;

                // 插入每个索引中对应的条目
                for (auto &index : tab_.indexes)
                {
                    auto idx_name = IxManager::get_index_name(tab_name_, index.cols);
                    auto key = new_char(index.col_tot_len);
                    int offset = 0;
                    for (size_t i = 0; i < static_cast<unsigned long>(index.col_num); ++i)
                    {
                        std::memcpy(key.get() + offset, new_record_data + index.cols[i].offset, index.cols[i].len);
                        offset += index.cols[i].len;
                    }

                    std::vector<ColType> col_types;
                    std::vector<int> col_lens;
                    for (const auto &col : index.cols)
                    {
                        col_types.push_back(col.type);
                        col_lens.push_back(col.len);
                    }

                    context_->lock_mgr_->lock_exclusive_gap_on_index(context_->txn_, idx_name, key.get(), key.get(), true, true, col_types, col_lens);
                }
            }
        }

        if (!is_locked)
        {
            context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, tab_name_);
            is_locked = true;
        }

        for (const Rid &rid : rids_)
        {
            const auto old_record_ = fh_->get_record(rid, nullptr);
            auto new_record_ = std::make_unique<RmRecord>(*old_record_);
            if (satisfies_conds(old_record_))
            {
                update_record(new_record_.get());
                std::vector<IndexMeta> deleted_indexes_;
                std::vector<IndexMeta> inserted_indexes_;
                try
                {
                    for (const auto &index : tab_.indexes)
                    {
                        auto idx_name = IxManager::get_index_name(tab_name_, index.cols);
                        auto ih_ = sm_manager_->ihs_.at(IxManager::get_index_name(tab_name_, index.cols)).get();
                        std::vector<ColType> col_types_;
                        std::vector<int> col_lens_;
                        for (const auto &col : index.cols)
                        {
                            col_types_.push_back(col.type);
                            col_lens_.push_back(col.len);
                        }
                        ih_->delete_entry(old_record_->data, rid, context_);
                        deleted_indexes_.push_back(index);
                        ih_->insert_entry(new_record_->data, rid, context_);
                        inserted_indexes_.push_back(index);
                    }
                }
                catch (const IndexEntryAlreadyExistError &)
                {
                    for (const auto &index : deleted_indexes_)
                    {
                        auto ih_ = sm_manager_->ihs_.at(IxManager::get_index_name(tab_name_, index.cols)).get();
                        ih_->insert_entry(old_record_->data, rid, context_);
                    }
                    for (const auto &index : inserted_indexes_)
                    {
                        auto ih_ = sm_manager_->ihs_.at(IxManager::get_index_name(tab_name_, index.cols)).get();
                        ih_->delete_entry(new_record_->data, rid, context_);
                    }
                    throw;
                }
                fh_->update_record(rid, new_record_->data, context_);
            }
        }

        return nullptr;
    }
    /**
     * @brief 获取当前更新的记录位置
     * @return 记录位置的引用
     */
    Rid &rid() override { return _abstract_rid; }

private:
    /**
     * @brief 检查记录是否满足条件
     * @param record 记录指针
     * @return 是否满足条件
     */
    bool satisfies_conds(const std::unique_ptr<RmRecord> &record) const
    {
        for (const auto &cond : conds_)
        {
            if (!compare(record, cond))
            {
                return false;
            }
        }
        return true;
    }
    /**
     * @brief 更新记录
     * @param record 记录指针
     */
    void update_record(RmRecord *record)
    {
        for (const auto &set_clause : set_clauses_)
        {
            auto set_table = tab_.get_col(set_clause.lhs.col_name);
            char *data_ptr = record->data + set_table->offset;
            switch (set_table->type)
            {
            case TYPE_INT:
                *reinterpret_cast<int *>(data_ptr) = set_clause.rhs.int_val;
                break;
            case TYPE_FLOAT:
                *reinterpret_cast<float *>(data_ptr) = set_clause.rhs.float_val;
                break;
            case TYPE_STRING:
                int len = set_table->len;
                std::memset(data_ptr, 0, len);
                std::memcpy(data_ptr, set_clause.rhs.str_val.c_str(), len);
                break;
            }
        }
    }
};
