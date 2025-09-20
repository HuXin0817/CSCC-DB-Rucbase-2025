#pragma once

#include <cmath>

#include "execution_manager_finals.h"
#include "executor_abstract_finals.h"

class InsertExecutor : public AbstractExecutor
{
public:
    InsertExecutor(SmManager *sm_manager_, const std::string &tab_name, std::vector<Value> &values_, Context *context_)
    {
        auto tab_ = sm_manager_->db_.get_table(tab_name);
        auto fh_ = sm_manager_->fhs_[tab_->fd_].get();

        // Make record buffer
        auto rid_ = sm_manager_->memory_pool_manager_->allocate(fh_->record_size);
        std::memset(rid_, '\0', fh_->record_size);

        for (size_t i = 0; i < values_.size(); i++)
        {
            auto &col = tab_->cols[i];
            auto &val = values_[i];
            if (col.type != val.type)
            {
                if (!can_cast_type(val.type, col.type))
                    throw RMDBError();
                else
                    cast_value(val, col.type);
            }

            switch (val.type)
            {
            case ColType::TYPE_INT:
            {
                std::memcpy(rid_ + col.offset, &val.int_val, col.len);
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                std::memcpy(rid_ + col.offset, &val.float_val, col.len);
                break;
            }
            case ColType::TYPE_STRING:
            {
                std::memcpy(rid_ + col.offset, val.str_val.c_str(), val.str_val.length());
                break;
            }
            }
        }

        // Insert into index
        auto &indexes = tab_->indexes;

        if (IxIndexHandle::unique_check)
        {
            for (auto &index : indexes)
            {
                auto ih = sm_manager_->ihs_[index.fd_].get();
                if (ih->exists_entry(rid_))
                {
                    sm_manager_->memory_pool_manager_->deallocate(rid_, fh_->record_size);
                    throw IndexEntryAlreadyExistError();
                }
            }
        }


        for (auto &index : indexes)
        {
            auto ih = sm_manager_->ihs_[index.fd_].get();
            ih->insert_entry(rid_);
        }

        // Insert into record file
        fh_->insert_record(rid_);
        context_->txn_->append_write_record(WriteType::INSERT_TUPLE, tab_->fd_, rid_);
    };
};
