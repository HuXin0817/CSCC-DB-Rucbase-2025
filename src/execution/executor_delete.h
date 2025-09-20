#pragma once

#include <utility>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @class DeleteExecutor
 * @brief 执行删除操作的执行器
 */
class DeleteExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // 删除操作的条件
    RmFileHandle *fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SmManager *sm_manager_;        // 存储管理器指针
    bool is_locked = false;

public:
    /**
     * @brief 构造函数
     *
     * @param sm_manager 存储管理器指针
     * @param tab_name 表名
     * @param conds 删除操作的条件
     * @param rids 需要删除的记录的位置
     * @param context 执行上下文
     */
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds, std::vector<Rid> rids, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
    }

    /**
     * @brief 执行删除操作，删除满足条件的记录及其索引
     * @return std::unique_ptr<RmRecord> 返回指向记录的智能指针（此处为 nullptr，表示删除操作没有返回记录）
     */
    std::unique_ptr<RmRecord> Next() override
    {
        // 遍历需要删除的记录的位置
        for (auto rid_ : rids_)
        {
            // 先获取记录数据
            std::unique_ptr<RmRecord> record = fh_->get_record(rid_, context_);
            char *record_data = record->data;

            // 删除每个索引中对应的 entry
            for (auto &index : tab_.indexes)
            {
                auto idx_name = IxManager::get_index_name(tab_name_, index.cols);
                // 为索引键值分配内存
                auto key = new_char(index.col_tot_len);
                int offset = 0;
                // 遍历索引列，将记录中的对应值复制到索引键值中
                for (size_t i = 0; i < static_cast<unsigned long>(index.col_num); ++i)
                {
                    std::memcpy(key.get() + offset, record_data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }

                std::vector<ColType> col_types_;
                std::vector<int> col_lens_;
                for (const auto &col : index.cols)
                {
                    col_types_.push_back(col.type);
                    col_lens_.push_back(col.len);
                }

                context_->lock_mgr_->lock_exclusive_gap_on_index(context_->txn_, idx_name, key.get(), key.get(), true, true, col_types_, col_lens_);
            }
        }

        if (!is_locked)
        {
            context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, tab_name_);
            is_locked = true;
        }

        // 获取记录的大小，并创建一个记录对象
        RmRecord rec(fh_->get_file_hdr().record_size);

        // 遍历需要删除的记录的位置
        for (auto rid_ : rids_)
        {
            // 先获取记录数据
            std::unique_ptr<RmRecord> record = fh_->get_record(rid_, context_);
            char *record_data = record->data;

            // 删除每个索引中对应的 entry
            for (auto &index : tab_.indexes)
            {
                // 获取当前索引句柄
                auto ih = sm_manager_->ihs_.at(IxManager::get_index_name(tab_name_, index.cols)).get();
                // 为索引键值分配内存
                auto key = new_char(index.col_tot_len);
                int offset = 0;
                // 遍历索引列，将记录中的对应值复制到索引键值中
                for (size_t i = 0; i < static_cast<unsigned long>(index.col_num); ++i)
                {
                    std::memcpy(key.get() + offset, record_data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                // 删除索引中的键值对
                ih->delete_entry(key.get(), rid_, context_);
            }

            // 删除记录
            fh_->delete_record(rid_, context_);
        }

        // 返回 nullptr 表示删除操作没有返回记录
        return nullptr;
    }

    /**
     * @brief 获取当前删除的记录位置
     * @return 记录位置的引用
     */
    Rid &rid() override { return _abstract_rid; }
};
