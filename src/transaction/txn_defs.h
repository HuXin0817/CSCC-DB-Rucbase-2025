#pragma once

#include <atomic>
#include <cassert>
#include <utility>

#include "common/config.h"
#include "defs.h"
#include "record/rm_defs.h"
#include "storage/page.h"

/* 标识事务状态 */
enum class TransactionState
{
    DEFAULT,   // 默认状态，事务刚创建时的状态
    GROWING,   // 成长状态，事务可以获取新的锁以进行更多的操作
    SHRINKING, // 收缩状态，事务只能释放锁，不能获取新锁，准备提交或中止
    COMMITTED, // 提交状态，事务已成功提交，所有更改已持久化
    ABORTED    // 中止状态，事务已被中止，所有更改将被回滚
};

/* 事务写操作类型，包括插入、删除、更新三种操作 */
enum WriteType : int
{
    INSERT_TUPLE = 0, // 插入操作，向表中插入一条新的记录
    DELETE_TUPLE,     // 删除操作，从表中删除一条记录
    UPDATE_TUPLE,     // 更新操作，更新表中的一条记录
    IX_INSERT_TUPLE,  // 插入索引操作
    IX_DELETE_TUPLE   // 删除索引操作
};

/* 事务中止的原因 */
enum class AbortReason
{
    LOCK_ON_SHRINKING = 0, // 在收缩阶段请求锁，违反两阶段锁协议
    WAIT_DIE_ABORT         // wait-die 策略中止，年轻事务被中止以避免等待老事务
};

/**
 * @class WriteRecord
 * @brief 事务写记录类，记录事务的写操作类型、表名、记录ID及记录内容
 */
class WriteRecord
{
public:
    WriteRecord() = default;

    /**
     * @brief 构造函数，不包含记录内容
     * @param wtype 写操作类型
     * @param tab_name 表名
     * @param rid 记录ID
     */
    WriteRecord(WriteType wtype, std::string tab_name, const Rid &rid) : wtype_(wtype), file_name_(std::move(tab_name)), rid_(rid) {}

    /**
     * @brief 构造函数，包含记录内容
     * @param wtype 写操作类型
     * @param tab_name 表名
     * @param rid 记录ID
     * @param record 记录内容
     */
    WriteRecord(WriteType wtype, std::string tab_name, const Rid &rid, const RmRecord &record) : wtype_(wtype), file_name_(std::move(tab_name)), rid_(rid), record_(record) {}

    /**
     * @brief 获取记录内容
     * @return RmRecord& 记录内容的引用
     */
    RmRecord &get_record() { return record_; }

    /**
     * @brief 获取记录ID
     * @return Rid& 记录ID的引用
     */
    Rid &get_rid() { return rid_; }

    /**
     * @brief 获取写操作类型
     * @return WriteType& 写操作类型的引用
     */
    WriteType &get_write_type() { return wtype_; }

    /**
     * @brief 获取表名
     * @return std::string& 表名的引用
     */
    std::string &get_file_name() { return file_name_; }

private:
    WriteType wtype_{};       // 写操作类型
    std::string file_name_{}; // 表名
    Rid rid_{};               // 记录ID
    RmRecord record_{};       // 记录内容
};

/**
 * @class TransactionAbortException
 * @brief 事务中止异常类，记录事务ID和中止原因
 */
class TransactionAbortException : public std::exception
{
    txn_id_t txn_id_;          // 事务ID
    AbortReason abort_reason_; // 中止原因

public:
    /**
     * @brief 构造函数
     * @param txn_id 事务ID
     * @param abort_reason 中止原因
     */
    explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason) : txn_id_(txn_id), abort_reason_(abort_reason) {}

    /**
     * @brief 获取事务ID
     * @return txn_id_t 事务ID
     */
    txn_id_t get_transaction_id() const { return txn_id_; }
};
