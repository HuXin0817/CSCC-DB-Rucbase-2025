#pragma once

#include <atomic>

#include "system/sm_manager_finals.h"
#include "transaction_finals.h"

static constexpr int MAX_TXN_SIZE = 0x2000;

// TransactionManager 类管理数据库系统中的事务，包括开始、提交和中止事务等操作
class TransactionManager
{
public:
    explicit TransactionManager(SmManager *sm_manager, LockManager *lock_manager) : sm_manager_(sm_manager), lock_manager_(lock_manager), memory_pool_manager_(sm_manager->memory_pool_manager_)
    {
        for (int i = 0; i < MAX_TXN_SIZE; i++)
        {
            txn_map_[i] = std::make_shared<Transaction>(i);
        }
    }

    std::shared_ptr<Transaction> begin(const std::shared_ptr<Transaction> &txn);

    void commit(const std::shared_ptr<Transaction> &txn);

    void abort(const std::shared_ptr<Transaction> &txn);

    std::shared_ptr<Transaction> get_transaction(const txn_id_t &txn_id)
    {
        if (txn_id == INVALID_TXN_ID)
        {
            return nullptr;
        }
        return txn_map_[txn_id];
    }

    void finished(const std::shared_ptr<Transaction> &txn);

    txn_id_t get_next_txn_id()
    {
        return next_txn_id_++ % MAX_TXN_SIZE;
    }

    std::atomic<txn_id_t> next_txn_id_{0}; // 用于分发事务ID
    SmManager *sm_manager_;                // 存储管理器指针
    LockManager *lock_manager_;            // 锁管理器指针
    PoolManager *memory_pool_manager_;
    std::shared_ptr<Transaction> txn_map_[MAX_TXN_SIZE]; // 全局事务表，存放事务ID与事务对象的映射关系
};
