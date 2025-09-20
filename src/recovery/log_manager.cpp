

#include "log_manager.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 */
void LogManager::add_log_to_buffer(LogRecord *log_record)
{
    std::lock_guard<std::mutex> lock(latch_); // 加锁以确保线程安全
    add_log_to_buffer_without_lock(log_record);
}

void LogManager::add_log_to_buffer_without_lock(LogRecord *log_record)
{
    if (log_record->log_type_ == LogType::COMMIT)
    {
        auto commit_log_record_ = dynamic_cast<CommitLogRecord *>(log_record);
        log_buffer_.offset_ += commit_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    }
    else if (log_record->log_type_ == LogType::BEGIN)
    {
        auto begin_log_record_ = dynamic_cast<BeginLogRecord *>(log_record);
        log_buffer_.offset_ += begin_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    }
    else if (log_record->log_type_ == LogType::INSERT)
    {
        auto insert_log_record_ = dynamic_cast<InsertLogRecord *>(log_record);
        log_buffer_.offset_ += insert_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    }
    else if (log_record->log_type_ == LogType::DELETE)
    {
        auto delete_log_record_ = dynamic_cast<DeleteLogRecord *>(log_record);
        log_buffer_.offset_ += delete_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    }
    else if (log_record->log_type_ == LogType::UPDATE)
    {
        auto update_log_record_ = dynamic_cast<UpdateLogRecord *>(log_record);
        log_buffer_.offset_ += update_log_record_->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
    }

    if (log_buffer_.offset_ > (LOG_BUFFER_SIZE >> 1))
    {
        flush_log_to_disk_without_lock();
    }
}

std::vector<LogRecord *> LogManager::read_logs_from_disk_without_lock(size_t offset)
{
    std::vector<LogRecord *> log_records_;
    const auto file_size_ = disk_manager_->get_file_size(LOG_FILE_NAME);
    auto buffer = new_char(std::max(file_size_, 1));
    disk_manager_->read_log(buffer.get(), file_size_, static_cast<int>(offset));
    auto *tmp_log_ = new LogRecord();

    auto start_offset = offset;
    while (offset < file_size_)
    {
        tmp_log_->deserialize(buffer.get() + offset - start_offset);
        if (tmp_log_->log_type_ == LogType::BEGIN)
        {
            auto log_record_ = new BeginLogRecord();
            offset += log_record_->deserialize(buffer.get() + offset - start_offset);
            log_records_.emplace_back(log_record_);
        }
        else if (tmp_log_->log_type_ == LogType::COMMIT)
        {
            auto log_record_ = new CommitLogRecord();
            offset += log_record_->deserialize(buffer.get() + offset - start_offset);
            log_records_.emplace_back(log_record_);
        }
        else if (tmp_log_->log_type_ == LogType::DELETE)
        {
            auto log_record_ = new DeleteLogRecord();
            offset += log_record_->deserialize(buffer.get() + offset - start_offset);
            log_records_.emplace_back(log_record_);
        }
        else if (tmp_log_->log_type_ == LogType::INSERT)
        {
            auto log_record_ = new InsertLogRecord();
            offset += log_record_->deserialize(buffer.get() + offset - start_offset);
            log_records_.emplace_back(log_record_);
        }
        else if (tmp_log_->log_type_ == LogType::UPDATE)
        {
            auto log_record_ = new UpdateLogRecord();
            offset += log_record_->deserialize(buffer.get() + offset - start_offset);
            log_records_.emplace_back(log_record_);
        }
    }

    delete tmp_log_;
    return log_records_;
}

/**
 * @description: 添加插入操作的日志记录
 * @param {txn_id_t} txn_id_ 事务ID
 * @param {RmRecord&} insert_value 插入的记录数据
 * @param {Rid} rid 插入的记录ID
 * @param {std::string&} table_name 表名
 */
void LogManager::add_insert_log_to_buffer(txn_id_t txn_id, const RmRecord &insert_value, const Rid &rid, const std::string &table_name)
{
    auto log_ = new InsertLogRecord(txn_id, insert_value, rid, table_name);
    add_log_to_buffer(log_);
    delete log_;
}

/**
 * @description: 添加删除操作的日志记录
 * @param {txn_id_t} txn_id_ 事务ID
 * @param {RmRecord&} delete_value 删除的记录数据
 * @param {Rid} rid 删除的记录ID
 * @param {std::string&} table_name 表名
 */
void LogManager::add_delete_log_to_buffer(txn_id_t txn_id, const RmRecord &delete_value, const Rid &rid, const std::string &table_name)
{
    auto log_ = new DeleteLogRecord(txn_id, delete_value, rid, table_name);
    add_log_to_buffer(log_);
    delete log_;
}

/**
 * @description: 添加更新操作的日志记录
 * @param {txn_id_t} txn_id_ 事务ID
 * @param {RmRecord&} update_value 更新后的记录数据
 * @param {RmRecord&} old_value 更新前的记录数据
 * @param {Rid} rid 更新的记录ID
 * @param {std::string&} table_name 表名
 */
void LogManager::add_update_log_to_buffer(txn_id_t txn_id, const RmRecord &update_value, const RmRecord &old_value, const Rid &rid, const std::string &table_name)
{
    auto log_ = new UpdateLogRecord(txn_id, update_value, old_value, rid, table_name);
    add_log_to_buffer(log_);
    delete log_;
}

/**
 * @description: 添加事务开始的日志记录
 * @param {txn_id_t} txn_id_ 事务ID
 */
void LogManager::add_begin_log_to_buffer(txn_id_t txn_id)
{
    auto log_ = new BeginLogRecord(txn_id);
    add_log_to_buffer(log_);
    delete log_;
}

/**
 * @description: 添加事务提交的日志记录
 * @param {txn_id_t} txn_id_ 事务ID
 */
void LogManager::add_commit_log_to_buffer(txn_id_t txn_id)
{
    auto log_ = new CommitLogRecord(txn_id);
    add_log_to_buffer(log_);
    delete log_;
}
