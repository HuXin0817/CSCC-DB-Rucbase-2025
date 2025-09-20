#include "log_recovery.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>

void RecoveryManager::recovery()
{
    std::vector<LogRecord *> log_records_ = log_manager_->read_logs_from_disk_without_lock(sm_manager_->db_.log_offset_);
    std::unordered_set<txn_id_t> undo_txns_;

    for (const auto *log_record : log_records_)
    {
        if (log_record->log_type_ == LogType::BEGIN)
        {
            undo_txns_.insert(log_record->log_tid_);
        }
        if (log_record->log_type_ == LogType::COMMIT)
        {
            undo_txns_.erase(log_record->log_tid_);
        }
    }

    std::unordered_map<std::string, int> tab_max_page_number;
    txn_id_t max_txn_id = 0;

#define COMPARE_TAB_MAX_PAGE_NUMBER(LogType)                          \
    auto record_ = dynamic_cast<LogType *>(log_record);               \
    auto tab_name_ = record_->table_name_.to_string();                \
    if (sm_manager_->fhs_.find(tab_name_) == sm_manager_->fhs_.end()) \
    {                                                                 \
        continue;                                                     \
    }                                                                 \
    tab_max_page_number[tab_name_] = std::max(tab_max_page_number[tab_name_], record_->rid_.page_no);

    for (auto *log_record : log_records_)
    {
        if (log_record->log_type_ == LogType::BEGIN)
        {
            undo_txns_.insert(log_record->log_tid_);
        }
        else if (log_record->log_type_ == LogType::COMMIT)
        {
            undo_txns_.erase(log_record->log_tid_);
        }
        else if (log_record->log_type_ == LogType::DELETE)
        {
            COMPARE_TAB_MAX_PAGE_NUMBER(DeleteLogRecord)
        }
        else if (log_record->log_type_ == LogType::INSERT)
        {
            COMPARE_TAB_MAX_PAGE_NUMBER(InsertLogRecord)
        }
        else if (log_record->log_type_ == LogType::UPDATE)
        {
            COMPARE_TAB_MAX_PAGE_NUMBER(UpdateLogRecord)
        }

        max_txn_id = std::max(max_txn_id, log_record->log_tid_);
    }

    transaction_manager_->set_next_txn_id(max_txn_id + 1);

    for (const auto &[tab_name, max_page_number] : tab_max_page_number)
    {
        auto fh_ = sm_manager_->fhs_.at(tab_name).get();
        while (fh_->file_hdr_.num_pages <= max_page_number)
        {
            auto page_hdr_ = fh_->create_new_page_handle();
            buffer_pool_manager_->unpin_page(page_hdr_.page->id_, false);
        }
    }

    for (auto *record : log_records_)
    {
        redo_one_log(record);
    }

    for (auto i = static_cast<long long>(log_records_.size()) - 1; i >= 0; i--)
    {
        auto record = log_records_[i];
        if (undo_txns_.find(record->log_tid_) != undo_txns_.end())
        {
            undo_one_log(record);
        }
    }

    for (auto &tab : sm_manager_->db_.tabs_)
    {
        std::vector<IndexMeta> indexes;
        indexes.reserve(tab.second.indexes.size());
        for (auto &index_ : tab.second.indexes)
        {
            indexes.emplace_back(index_);
        }
        for (auto &index_ : indexes)
        {
            sm_manager_->drop_index(index_.tab_name, index_.cols, nullptr);
            std::vector<std::string> col_names_;
            col_names_.reserve(index_.cols.size());
            for (auto &col : index_.cols)
            {
                col_names_.emplace_back(col.name);
            }
            sm_manager_->create_index(index_.tab_name, col_names_, nullptr);
        }
    }

    save_into_disk();
}

void RecoveryManager::create_static_check_point()
{
    std::unique_lock lock_(latch_);
    std::unique_lock lock(log_manager_->latch_);

    auto log_records_ = log_manager_->read_logs_from_disk_without_lock(sm_manager_->db_.log_offset_);
    log_manager_->flush_log_to_disk_without_lock();

    save_into_disk();

    std::unordered_set<int> finish_txns_;
    for (const auto &log_record : log_records_)
    {
        if (log_record->log_type_ == LogType::COMMIT)
        {
            finish_txns_.insert(log_record->log_tid_);
        }
    }

    for (const auto &log_record : log_records_)
    {
        if (finish_txns_.find(log_record->log_tid_) == finish_txns_.end())
        {
            log_manager_->add_log_to_buffer_without_lock(log_record);
        }
    }

    log_manager_->flush_log_to_disk_without_lock();
    exit(1);
}

void RecoveryManager::crash() { exit(1); }

void RecoveryManager::redo_one_log(LogRecord *log_record)
{
    if (log_record->log_type_ == LogType::INSERT)
    {
        auto *record_ = dynamic_cast<InsertLogRecord *>(log_record);
        if (sm_manager_->fhs_.find(record_->table_name_.to_string()) == sm_manager_->fhs_.end())
        {
            return;
        }
        sm_manager_->fhs_.at(record_->table_name_.to_string())->reset_data_on_rid(record_->rid_, record_->insert_value_.data);
    }
    else if (log_record->log_type_ == LogType::UPDATE)
    {
        auto *record_ = dynamic_cast<UpdateLogRecord *>(log_record);
        if (sm_manager_->fhs_.find(record_->table_name_.to_string()) == sm_manager_->fhs_.end())
        {
            return;
        }
        sm_manager_->fhs_.at(record_->table_name_.to_string())->reset_data_on_rid(record_->rid_, record_->update_value_.data);
    }
    else if (log_record->log_type_ == LogType::DELETE)
    {
        auto *record_ = dynamic_cast<DeleteLogRecord *>(log_record);
        if (sm_manager_->fhs_.find(record_->table_name_.to_string()) == sm_manager_->fhs_.end())
        {
            return;
        }
        sm_manager_->fhs_.at(record_->table_name_.to_string())->delete_record(record_->rid_);
    }
}

void RecoveryManager::undo_one_log(LogRecord *log_record)
{
    if (log_record->log_type_ == LogType::INSERT)
    {
        auto record_ = dynamic_cast<InsertLogRecord *>(log_record);
        if (sm_manager_->fhs_.find(record_->table_name_.to_string()) == sm_manager_->fhs_.end())
        {
            return;
        }
        sm_manager_->fhs_.at(record_->table_name_.to_string())->delete_record(record_->rid_);
    }
    else if (log_record->log_type_ == LogType::DELETE)
    {
        auto record_ = dynamic_cast<DeleteLogRecord *>(log_record);
        if (sm_manager_->fhs_.find(record_->table_name_.to_string()) == sm_manager_->fhs_.end())
        {
            return;
        }
        sm_manager_->fhs_.at(record_->table_name_.to_string())->reset_data_on_rid(record_->rid_, record_->delete_value_.data);
    }
    else if (log_record->log_type_ == LogType::UPDATE)
    {
        auto record_ = dynamic_cast<UpdateLogRecord *>(log_record);
        if (sm_manager_->fhs_.find(record_->table_name_.to_string()) == sm_manager_->fhs_.end())
        {
            return;
        }
        sm_manager_->fhs_.at(record_->table_name_.to_string())->reset_data_on_rid(record_->rid_, record_->old_value_.data);
    }
}
