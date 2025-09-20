#pragma once

#include <mutex>
#include <vector>

#include "common/config.h"
#include "record/rm_defs.h"

namespace LogType
{
    static const RmRecord NONE("NONE");
    static const RmRecord BEGIN("BEGIN");
    static const RmRecord COMMIT("COMMIT");
    static const RmRecord UPDATE("UPDATE");
    static const RmRecord INSERT("INSERT");
    static const RmRecord DELETE("DELETE");
}; // namespace LogType

/**
 * @class LogRecord
 * @brief 日志记录的基类，定义了通用的日志记录接口和数据成员
 *
 * LogRecord类是所有日志记录类型的基类，提供了通用的接口用于序列化和反序列化日志记录。
 * 派生类可以扩展该类以支持特定类型的日志记录（例如，插入日志记录、更新日志记录等）。
 */
class LogRecord
{
public:
    RmRecord log_type_ = LogType::NONE; /* 日志对应操作的类型 */
    txn_id_t log_tid_ = INVALID_TXN_ID; /* 创建当前日志的事务ID，用于标识所属事务 */

    /**
     * @brief 默认构造函数
     */
    LogRecord() = default;

    /**
     * @brief 带参数的构造函数
     * @param log_type 日志对应操作的类型
     */
    explicit LogRecord(const RmRecord &log_type) : log_type_(log_type) {}

    /**
     * @brief 序列化日志记录，将其转换为字节流存储在目标地址
     * @param dest 序列化后的字节流存储地址
     * @return size_t 序列化后的字节数
     */
    virtual size_t serialize(char *dest) const
    {
        size_t offset = 0;
        offset += log_type_.serialize(dest + offset);
        offset += encode::serialize(", log_tid_: ", dest + offset);
        offset += encode::serialize(log_tid_, dest + offset);
        return offset;
    }

    /**
     * @brief 反序列化日志记录，将字节流转换为日志记录对象
     * @param dest 字节流存储地址
     * @return size_t 反序列化后的字节数
     */
    virtual size_t deserialize(const char *dest)
    {
        size_t offset = 0;
        offset += log_type_.deserialize(dest + offset);
        offset += encode::deserialize(", log_tid_: ", dest + offset);
        offset += encode::deserialize(log_tid_, dest + offset);
        return offset;
    }

    /**
     * @brief 析构函数
     */
    virtual ~LogRecord() = default;
};

/**
 * @class BeginLogRecord
 * @brief 表示事务开始的日志记录
 */
class BeginLogRecord final : public LogRecord
{
public:
    BeginLogRecord() : LogRecord(LogType::BEGIN) {}

    explicit BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() { log_tid_ = txn_id; }

    size_t serialize(char *dest) const override
    {
        size_t offset = 0;
        offset += LogRecord::serialize(dest);
        offset += encode::serialize("\n\n", dest + offset);
        return offset;
    }

    size_t deserialize(const char *src) override
    {
        size_t offset = 0;
        offset += LogRecord::deserialize(src);
        offset += encode::deserialize("\n\n", src + offset);
        return offset;
    }
};

/**
 * @class CommitLogRecord
 * @brief 表示事务提交的日志记录
 */
class CommitLogRecord final : public LogRecord
{
public:
    CommitLogRecord() : LogRecord(LogType::COMMIT) {}

    explicit CommitLogRecord(txn_id_t txn_id) : CommitLogRecord() { log_tid_ = txn_id; }

    size_t serialize(char *dest) const override
    {
        size_t offset = 0;
        offset += LogRecord::serialize(dest);
        offset += encode::serialize("\n\n", dest + offset);
        return offset;
    }

    size_t deserialize(const char *src) override
    {
        size_t offset = 0;
        offset += LogRecord::deserialize(src);
        offset += encode::deserialize("\n\n", src + offset);
        return offset;
    }
};

/**
 * @class InsertLogRecord
 * @brief 表示插入操作的日志记录
 */
class InsertLogRecord final : public LogRecord
{
public:
    InsertLogRecord() : LogRecord(LogType::INSERT) {}

    InsertLogRecord(txn_id_t txn_id, const RmRecord &insert_value, const Rid &rid, const std::string &table_name) : InsertLogRecord()
    {
        log_tid_ = txn_id;
        insert_value_ = insert_value;
        rid_ = rid;
        table_name_.reset(table_name);
    }

    size_t serialize(char *dest) const override
    {
        size_t offset = 0;
        offset += LogRecord::serialize(dest + offset);
        offset += encode::serialize(", insert_value_: ", dest + offset);
        offset += insert_value_.serialize(dest + offset);
        offset += encode::serialize(", rid_: ", dest + offset);
        offset += rid_.serialize(dest + offset);
        offset += encode::serialize(", table_name_: ", dest + offset);
        offset += table_name_.serialize(dest + offset);
        offset += encode::serialize("\n\n", dest + offset);
        return offset;
    }

    size_t deserialize(const char *dest) override
    {
        size_t offset = 0;
        offset += LogRecord::deserialize(dest + offset);
        offset += encode::deserialize(", insert_value_: ", dest + offset);
        offset += insert_value_.deserialize(dest + offset);
        offset += encode::deserialize(", rid_: ", dest + offset);
        offset += rid_.deserialize(dest + offset);
        offset += encode::deserialize(", table_name_: ", dest + offset);
        offset += table_name_.deserialize(dest + offset);
        offset += encode::deserialize("\n\n", dest + offset);
        return offset;
    }

    RmRecord insert_value_;
    Rid rid_{};
    RmRecord table_name_;
};

/**
 * @class DeleteLogRecord
 * @brief 表示删除操作的日志记录
 */
class DeleteLogRecord final : public LogRecord
{
public:
    DeleteLogRecord() : LogRecord(LogType::DELETE) {}

    DeleteLogRecord(txn_id_t txn_id, const RmRecord &delete_value, Rid rid, const std::string &table_name) : DeleteLogRecord()
    {
        log_tid_ = txn_id;
        delete_value_ = delete_value;
        rid_ = rid;
        table_name_.reset(table_name);
    }

    size_t serialize(char *dest) const override
    {
        size_t offset = 0;
        offset += LogRecord::serialize(dest + offset);
        offset += encode::serialize(", delete_value_: ", dest + offset);
        offset += delete_value_.serialize(dest + offset);
        offset += encode::serialize(", rid_: ", dest + offset);
        offset += rid_.serialize(dest + offset);
        offset += encode::serialize(", table_name_: ", dest + offset);
        offset += table_name_.serialize(dest + offset);
        offset += encode::serialize("\n\n", dest + offset);
        return offset;
    }

    size_t deserialize(const char *dest) override
    {
        size_t offset = 0;
        offset += LogRecord::deserialize(dest + offset);
        offset += encode::deserialize(", delete_value_: ", dest + offset);
        offset += delete_value_.deserialize(dest + offset);
        offset += encode::deserialize(", rid_: ", dest + offset);
        offset += rid_.deserialize(dest + offset);
        offset += encode::deserialize(", table_name_: ", dest + offset);
        offset += table_name_.deserialize(dest + offset);
        offset += encode::deserialize("\n\n", dest + offset);
        return offset;
    }

    RmRecord delete_value_;
    Rid rid_{};
    RmRecord table_name_;
};

/**
 * @class UpdateLogRecord
 * @brief 表示更新操作的日志记录
 */
class UpdateLogRecord final : public LogRecord
{
public:
    UpdateLogRecord() : LogRecord(LogType::UPDATE) {}

    UpdateLogRecord(txn_id_t txn_id, const RmRecord &update_value, const RmRecord &old_value, Rid rid, const std::string &table_name) : UpdateLogRecord()
    {
        log_tid_ = txn_id;
        old_value_ = old_value;
        update_value_ = update_value;
        rid_ = rid;
        table_name_.reset(table_name);
    }

    size_t serialize(char *dest) const override
    {
        size_t offset = 0;
        offset += LogRecord::serialize(dest + offset);
        offset += encode::serialize(", old_value_: ", dest + offset);
        offset += old_value_.serialize(dest + offset);
        offset += encode::serialize(", update_value_: ", dest + offset);
        offset += update_value_.serialize(dest + offset);
        offset += encode::serialize(", rid_: ", dest + offset);
        offset += rid_.serialize(dest + offset);
        offset += encode::serialize(", table_name_: ", dest + offset);
        offset += table_name_.serialize(dest + offset);
        offset += encode::serialize("\n\n", dest + offset);
        return offset;
    }

    size_t deserialize(const char *dest) override
    {
        size_t offset = 0;
        offset += LogRecord::deserialize(dest + offset);
        offset += encode::deserialize(", old_value_: ", dest + offset);
        offset += old_value_.deserialize(dest + offset);
        offset += encode::deserialize(", update_value_: ", dest + offset);
        offset += update_value_.deserialize(dest + offset);
        offset += encode::deserialize(", rid_: ", dest + offset);
        offset += rid_.deserialize(dest + offset);
        offset += encode::deserialize(", table_name_: ", dest + offset);
        offset += table_name_.deserialize(dest + offset);
        offset += encode::deserialize("\n\n", dest + offset);
        return offset;
    }

    RmRecord old_value_;
    RmRecord update_value_;
    Rid rid_{};
    RmRecord table_name_;
};

class LogManager;

/**
 * @class LogBuffer
 * @brief 日志缓冲区类，存储日志数据并负责日志数据的序列化和反序列化
 */
class LogBuffer
{
    friend class LogManager;
    friend class RecoveryManager;

private:
    /**
     * @description: 清空缓冲区
     */
    void clear() { offset_ = 0; }

    char buffer_[LOG_BUFFER_SIZE];
    std::atomic<size_t> offset_;
};

/**
 * @class LogManager
 * @brief 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中
 */
class LogManager
{
    friend class RecoveryManager;

public:
    /**
     * @description: 构造函数，初始化日志管理器
     * @param {DiskManager*} disk_manager 磁盘管理器指针
     */
    explicit LogManager(DiskManager *disk_manager) : disk_manager_(disk_manager) {}

    ~LogManager() = default;

    /**
     * @description: 添加插入操作日志到缓冲区
     * @param {txn_id_t} txn_id_ 事务ID
     * @param {RmRecord&} insert_value 插入的记录
     * @param {Rid} rid 记录ID
     * @param {std::string&} table_name 表名
     */
    void add_insert_log_to_buffer(txn_id_t txn_id, const RmRecord &insert_value, const Rid &rid, const std::string &table_name);

    /**
     * @description: 添加删除操作日志到缓冲区
     * @param {txn_id_t} txn_id_ 事务ID
     * @param {RmRecord&} delete_value 删除的记录
     * @param {Rid} rid 记录ID
     * @param {std::string&} table_name 表名
     */
    void add_delete_log_to_buffer(txn_id_t txn_id, const RmRecord &delete_value, const Rid &rid, const std::string &table_name);

    /**
     * @description: 添加更新操作日志到缓冲区
     * @param {txn_id_t} txn_id_ 事务ID
     * @param {RmRecord&} update_value 更新后的记录
     * @param {RmRecord&} old_value 更新前的记录
     * @param {Rid} rid 记录ID
     * @param {std::string&} table_name 表名
     */
    void add_update_log_to_buffer(txn_id_t txn_id, const RmRecord &update_value, const RmRecord &old_value, const Rid &rid, const std::string &table_name);

    /**
     * @description: 添加事务开始日志到缓冲区
     * @param {txn_id_t} txn_id_ 事务ID
     */
    void add_begin_log_to_buffer(txn_id_t txn_id);

    /**
     * @description: 添加事务提交日志到缓冲区
     * @param {txn_id_t} txn_id_ 事务ID
     */
    void add_commit_log_to_buffer(txn_id_t txn_id);

    void flush_log_to_disk_without_lock()
    {
        disk_manager_->write_log(log_buffer_.buffer_, static_cast<int>(log_buffer_.offset_)); // 将缓冲区内容写入磁盘
        log_buffer_.clear();                                                                  // 清空缓冲区
    }

    void flush_log_to_disk()
    {
        std::lock_guard<std::mutex> lock(latch_); // 加锁以确保线程安全
        flush_log_to_disk_without_lock();
    }

private:
    LogBuffer log_buffer_{};    /* 日志缓冲区 */
    DiskManager *disk_manager_; /* 磁盘管理器指针 */
    std::mutex latch_;          /* 互斥锁，保护对缓冲区的访问 */

    /**
     * @description: 将日志记录添加到日志缓冲区
     * @param {LogRecord*} log_record 要添加的日志记录
     */
    void add_log_to_buffer(LogRecord *log_record);

    void add_log_to_buffer_without_lock(LogRecord *log_record);

    std::vector<LogRecord *> read_logs_from_disk_without_lock(size_t offset);
};
