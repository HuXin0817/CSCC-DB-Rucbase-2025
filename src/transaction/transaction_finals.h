#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>

#include "txn_defs_finals.h"

// Transaction 类表示一个数据库事务
class Transaction
{
public:
    explicit Transaction(txn_id_t txn_id) : state_(TransactionState::DEFAULT), txn_id_(txn_id) {}

    void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }

    bool get_txn_mode() const { return txn_mode_; }

    TransactionState get_state() const { return state_; }

    void set_state(TransactionState state) { state_ = state; }

    void append_write_record(WriteType wtype, int tab_name, char *rid) { write_set_.emplace_back(wtype, tab_name, rid); }

    void append_write_record(WriteType wtype, int tab_name, char *rid, char *old_record) { write_set_.emplace_back(wtype, tab_name, rid, old_record); }

    bool txn_mode_{};        // 用于标识当前事务为显式事务还是单条SQL语句的隐式事务
    TransactionState state_; // 事务状态
    txn_id_t txn_id_;        // 事务的ID，唯一标识符

    std::deque<WriteRecord> write_set_;     // 事务包含的所有写操作
    std::unordered_set<int> gap_lock_map_;  // 事务申请的所有锁
    std::unordered_set<int> data_lock_map_; // 事务申请的所有锁
};
