#pragma once

#include <atomic>

#include "system/sm_manager.h"
#include "transaction.h"

// TransactionManager 类管理数据库系统中的事务，包括开始、提交和中止事务等操作
class TransactionManager
{
public:
    /**
     * @brief 构造函数，初始化 TransactionManager 对象
     * @param lock_manager 锁管理器指针
     * @param sm_manager 存储管理器指针
     */
    explicit TransactionManager(SmManager *sm_manager, LockManager *lock_manager) : sm_manager_(sm_manager), lock_manager_(lock_manager) {}

    /**
     * @brief 析构函数，默认析构 TransactionManager 对象
     */
    ~TransactionManager() = default;

    /**
     * @brief 开始一个新的事务
     * @param txn 事务对象指针
     * @param log_manager 日志管理器指针
     * @return 新事务对象指针
     */
    Transaction *begin(Transaction *txn, LogManager *log_manager);

    /**
     * @brief 提交一个事务
     * @param txn 事务对象指针
     * @param log_manager 日志管理器指针
     */
    void commit(Transaction *txn, LogManager *log_manager) const;

    /**
     * @brief 中止一个事务
     * @param txn 事务对象指针
     * @param log_manager 日志管理器指针
     */
    void abort(Transaction *txn, LogManager *log_manager);

    /**
     * @brief 获取事务ID为 txn_id 的事务对象
     * @param txn_id 事务ID
     * @return 事务对象的指针
     */
    Transaction *get_transaction(const txn_id_t &txn_id)
    {
        std::unique_lock lock(latch_);
        if (txn_id == INVALID_TXN_ID)
        {
            return nullptr;
        }
        if (txn_map_.find(txn_id) == txn_map_.end())
        {
            return nullptr;
        }
        return txn_map_.at(txn_id);
    }

    /**
     * @brief 获取下一个事务ID
     * @return 下一个事务ID
     */
    txn_id_t get_next_txn_id() { return next_txn_id_++; }

    void set_next_txn_id(txn_id_t txn_id) { next_txn_id_ = txn_id; }

    std::atomic<txn_id_t> next_txn_id_{0};                // 用于分发事务ID
    SmManager *sm_manager_;                               // 存储管理器指针
    LockManager *lock_manager_;                           // 锁管理器指针
    std::unordered_map<txn_id_t, Transaction *> txn_map_; // 全局事务表，存放事务ID与事务对象的映射关系
    std::mutex latch_;
};
