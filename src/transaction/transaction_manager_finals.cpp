#include "transaction_manager_finals.h"

#include "concurrency/lock_manager_finals.h"

std::shared_ptr<Transaction> TransactionManager::begin(const std::shared_ptr<Transaction> &txn)
{
    if (txn == nullptr)
    {
        return txn_map_[get_next_txn_id()];
    }
    else
    {
        return txn;
    }
}

void TransactionManager::commit(const std::shared_ptr<Transaction> &txn)
{
    // 获取事务的写集合
    auto &write_set = txn->write_set_;

    // 回滚所有写操作
    while (!write_set.empty())
    {
        auto write_record = write_set.back(); // 获取最后一个写记录
        write_set.pop_back();                 // 移除最后一个写记录

        auto fh_ = sm_manager_->fhs_[write_record.fd_].get();

        // 根据写操作类型进行回滚
        switch (write_record.wtype_)
        {
        case WriteType::INSERT_TUPLE:
        {
            break;
        }
        case WriteType::DELETE_TUPLE:
        {
            [[fallthrough]];
        }
        case WriteType::UPDATE_TUPLE:
        {
            memory_pool_manager_->deallocate(write_record.old_rid_, fh_->record_size);
            break;
        }
        }
    }

    finished(txn);
}

void TransactionManager::abort(const std::shared_ptr<Transaction> &txn)
{
    // 获取事务的写集合
    auto &write_set = txn->write_set_;

    // 回滚所有写操作
    while (!write_set.empty())
    {
        auto write_record = write_set.back(); // 获取最后一个写记录
        write_set.pop_back();                 // 移除最后一个写记录
        auto &indexes = sm_manager_->db_.get_table(NameManager::get_name(write_record.fd_))->indexes;
        auto fh_ = sm_manager_->fhs_[write_record.fd_].get();

        // 根据写操作类型进行回滚
        switch (write_record.wtype_)
        {
        case WriteType::INSERT_TUPLE:
        {
            fh_->delete_record(write_record.old_rid_);
            for (const auto &index : indexes)
            {
                auto ih_ = sm_manager_->ihs_[index.fd_].get();
                ih_->delete_entry(write_record.old_rid_);
            }
            memory_pool_manager_->deallocate(write_record.old_rid_, fh_->record_size);
            break;
        }
        case WriteType::DELETE_TUPLE:
        {
            sm_manager_->fhs_[write_record.fd_]->insert_record(write_record.old_rid_);
            for (const auto &index : indexes)
            {
                auto ih_ = sm_manager_->ihs_[index.fd_].get();
                ih_->insert_entry(write_record.old_rid_);
            }
            break;
        }
        case WriteType::UPDATE_TUPLE_ON_INDEX:
        {
            for (const auto &index : indexes)
            {
                auto ih_ = sm_manager_->ihs_[index.fd_].get();
                ih_->delete_entry(write_record.new_rid_);
                ih_->insert_entry(write_record.old_rid_);
            }
            sm_manager_->fhs_[write_record.fd_]->update_record(write_record.new_rid_, write_record.old_rid_);
            memory_pool_manager_->deallocate(write_record.new_rid_, fh_->record_size);
            break;
        }
        case::UPDATE_TUPLE:
            {
            memcpy( write_record.new_rid_,write_record.old_rid_, fh_->record_size);
            memory_pool_manager_->deallocate(write_record.old_rid_, fh_->record_size);
            break;
            }
        }
    }

    finished(txn);
}

void TransactionManager::finished(const std::shared_ptr<Transaction> &txn)
{
    lock_manager_->unlock(txn);
    txn->set_state(TransactionState::COMMITTED); // 设置事务状态为COMMITTED
    txn_map_[txn->txn_id_] = std::make_shared<Transaction>(txn->txn_id_);
}
