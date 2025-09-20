#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>

#include "txn_defs.h"

// Transaction 类表示一个数据库事务
class Transaction
{
public:
    /**
     * @brief 构造函数，初始化 Transaction 对象
     * @param txn_id 事务ID
     */
    explicit Transaction(txn_id_t txn_id) : state_(TransactionState::DEFAULT), txn_id_(txn_id)
    {
        // 初始化写集合、锁集合、索引加锁页面集合、索引删除页面集合
        write_set_ = std::make_shared<std::deque<WriteRecord>>();
        tab_lock_set_ = std::make_shared<std::unordered_set<std::string>>();
        gap_lock_set = std::make_shared<std::unordered_set<std::string>>();
    }

    /**
     * @brief 析构函数，默认析构 Transaction 对象
     */
    ~Transaction() = default;

    /**
     * @brief 获取事务ID
     * @return 事务ID
     */
    txn_id_t get_transaction_id() const { return txn_id_; }

    /**
     * @brief 设置事务模式（显式事务或隐式事务）
     * @param txn_mode 事务模式
     */
    void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }

    /**
     * @brief 获取事务模式
     * @return 事务模式
     */
    bool get_txn_mode() const { return txn_mode_; }

    /**
     * @brief 获取事务状态
     * @return 事务状态
     */
    TransactionState get_state() const { return state_; }

    /**
     * @brief 设置事务状态
     * @param state 事务状态
     */
    void set_state(TransactionState state) { state_ = state; }

    /**
     * @brief 获取写集合
     * @return 写集合的共享指针
     */
    std::shared_ptr<std::deque<WriteRecord>> get_write_set() { return write_set_; }

    /**
     * @brief 追加一个写记录到写集合
     * @param write_record 写记录
     */
    void append_write_record(const WriteRecord &write_record) { write_set_->push_back(write_record); }

    /**
     * @brief 获取锁集合
     * @return 锁集合的共享指针
     */
    std::shared_ptr<std::unordered_set<std::string>> get_tab_lock_set() { return tab_lock_set_; }

    /**
     * @brief 追加一个锁到锁集合
     * @param lock_id 锁的ID
     */
    void append_tab_lock_set(const std::string &lock_id) const { tab_lock_set_->insert(lock_id); }

    /**
     * @brief 获取锁集合
     * @return 锁集合的共享指针
     */
    std::shared_ptr<std::unordered_set<std::string>> get_gap_lock_set() { return gap_lock_set; }

    /**
     * @brief 追加一个锁到锁集合
     * @param lock_id 锁的ID
     */
    void append_gap_lock_set(const std::string &lock_id) const { gap_lock_set->insert(lock_id); }

    /**
     * @brief 清空写集合
     */
    void clear_write_set() { write_set_->clear(); }

private:
    bool txn_mode_{};        // 用于标识当前事务为显式事务还是单条SQL语句的隐式事务
    TransactionState state_; // 事务状态
    txn_id_t txn_id_;        // 事务的ID，唯一标识符

    std::shared_ptr<std::deque<WriteRecord>> write_set_;            // 事务包含的所有写操作
    std::shared_ptr<std::unordered_set<std::string>> tab_lock_set_; // 事务申请的所有锁
    std::shared_ptr<std::unordered_set<std::string>> gap_lock_set;
};
