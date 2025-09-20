#pragma once

#include "execution_manager_finals.h"
#include "executor_abstract_finals.h"

class DeleteExecutor : public AbstractExecutor
{
public:
    DeleteExecutor(SmManager *sm_manager_, const std::string &tab_name, const std::vector<char *> &rids_, Context *context)
    {
        auto tab_ = sm_manager_->db_.get_table(tab_name);
        auto fh_ = sm_manager_->fhs_[tab_->fd_].get();

        // 遍历需要删除的记录的位置
        for (auto rid_ : rids_)
        {
            auto &indexes = tab_->indexes;
            // 删除每个索引中对应的 entry
            for (const auto &index : indexes)
            {
                // 获取当前索引句柄
                auto ih = sm_manager_->ihs_[index.fd_].get();
                // 删除索引中的键值对
                ih->delete_entry(rid_);
            }
            // 删除记录
            fh_->delete_record(rid_);
            context->txn_->append_write_record(WriteType::DELETE_TUPLE, tab_->fd_, rid_);
        }
    }
};
