#pragma once

#include <utility>

#include "transaction/concurrency/lock_manager_finals.h"
#include "transaction/transaction_finals.h"

class Context
{
public:
    Context(LockManager *lock_mgr, std::shared_ptr<Transaction> txn, char *data_send, int *offset) : lock_mgr_(lock_mgr), txn_(std::move(txn)), data_send_(data_send), offset_(offset) {}

    static int MAX_OFFSET_LENGTH;

    bool data_send_is_full() const { return *offset_ > MAX_OFFSET_LENGTH; }

    LockManager *lock_mgr_;
    std::shared_ptr<Transaction> txn_;
    char *data_send_;
    int *offset_;
};
