#pragma once

#include <cmath>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle *fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_;                   // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;
    bool is_locked = false;

public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Value> &values, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size())
        {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override
    {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++)
        {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type)
            {
                if (!can_cast_type(val.type, col.type))
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                else
                    cast_value(val, col.type);
            }
            if (val.type == TYPE_FLOAT && (val.float_val == 0.0f && std::signbit(val.float_val)))
            {
                val.float_val = 0.0f; // 使用 = 进行赋值操作
            }
            val.init_raw(col.len);
            std::memset(rec.data + col.offset, 0, col.len);
            std::memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // Insert into index
        for (auto &index : tab_.indexes)
        {
            auto idx_name = IxManager::get_index_name(tab_name_, index.cols);
            auto key = new_char(index.col_tot_len);
            int offset = 0;
            for (size_t i = 0; i < static_cast<unsigned long>(index.col_num); ++i)
            {
                std::memcpy(key.get() + offset, rec.data + index.cols[i].offset, index.cols[i].len);
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

        if (!is_locked)
        {
            context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, tab_name_);
            is_locked = true;
        }

        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_);

        // Insert into index
        for (auto &index : tab_.indexes)
        {
            auto idx_name = IxManager::get_index_name(tab_name_, index.cols);
            auto ih = sm_manager_->ihs_.at(idx_name).get();
            auto key = new_char(index.col_tot_len);
            int offset = 0;
            for (size_t i = 0; i < static_cast<unsigned long>(index.col_num); ++i)
            {
                std::memcpy(key.get() + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }

            try
            {
                ih->insert_entry(key.get(), rid_, context_);
            }
            catch (const IndexEntryAlreadyExistError &)
            {
                // delete record file
                fh_->delete_record(rid_, context_);
                throw;
            }
        }

        return nullptr;
    }

    Rid &rid() override { return rid_; }
};
