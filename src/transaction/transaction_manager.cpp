#include "transaction_manager.h"

#include "record/rm_file_handle.h"
#include "storage/buffer_pool_manager.h"

/**
 * @description: 事务的开始方法
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 * @return {Transaction*} 开始事务的指针
 */
Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager)
{
    std::unique_lock lock(latch_);

    auto txn_ = txn;
    if (txn == nullptr)
    {
        // 创建新事务
        txn_ = new Transaction(get_next_txn_id());
        txn_->set_state(TransactionState::GROWING);
        // 将新事务加入全局事务表
        txn_map_.insert_or_assign(txn_->get_transaction_id(), txn_);
    }

    // 添加事务开始日志记录到缓冲区
    log_manager->add_begin_log_to_buffer(txn_->get_transaction_id());
    return txn_;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction *txn, LogManager *log_manager) const
{
    txn->set_state(TransactionState::SHRINKING); // 设置事务状态为SHRINKING

    auto tab_lock_set = txn->get_tab_lock_set();
    for (const auto &tab_name : *tab_lock_set)
    {
        lock_manager_->unlock_on_table(txn, tab_name);
    }

    auto gap_lock_set = txn->get_gap_lock_set();
    for (const auto &tab_name : *gap_lock_set)
    {
        lock_manager_->unlock_gap_on_index(txn, tab_name);
    }

    txn->clear_write_set(); // 清空写集合
    tab_lock_set->clear();  // 清空锁集合
    gap_lock_set->clear();

    // 添加事务提交日志记录到缓冲区并刷新到磁盘
    log_manager->add_commit_log_to_buffer(txn->get_transaction_id());
    log_manager->flush_log_to_disk();

    txn->set_state(TransactionState::COMMITTED); // 设置事务状态为COMMITTED
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction*} txn 需要回滚的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction *txn, LogManager *log_manager)
{
    // 获取事务的写集合
    auto write_set = txn->get_write_set();

    // 回滚所有写操作
    while (!write_set->empty())
    {
        auto write_record = write_set->back(); // 获取最后一个写记录
        write_set->pop_back();                 // 移除最后一个写记录

        // 根据写操作类型进行回滚
        switch (write_record.get_write_type())
        {
        case WriteType::INSERT_TUPLE:
        {
            /**
             * @description: 回滚插入操作
             * 1. 记录删除日志：将插入的记录删除。
             * 2. 执行删除操作：删除插入的记录。
             * 3. 更新缓冲区：取消页面的固定。
             */
            auto fh_ = sm_manager_->fhs_[write_record.get_file_name()].get();
            log_manager->add_delete_log_to_buffer(txn->get_transaction_id(), write_record.get_record(), write_record.get_rid(), write_record.get_file_name());
            fh_->delete_record(write_record.get_rid(), nullptr);
            sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), write_record.get_rid().page_no}, true);
            break;
        }
        case WriteType::DELETE_TUPLE:
        {
            /**
             * @description: 回滚删除操作
             * 1. 记录插入日志：将删除的记录插回去。
             * 2. 执行插入操作：插入被删除的记录。
             * 3. 更新缓冲区：取消页面的固定。
             */
            log_manager->add_insert_log_to_buffer(txn->get_transaction_id(), write_record.get_record(), write_record.get_rid(), write_record.get_file_name());
            auto fh_ = sm_manager_->fhs_[write_record.get_file_name()].get();
            sm_manager_->fhs_[write_record.get_file_name()]->reset_data_on_rid(write_record.get_rid(), write_record.get_record().data);
            sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), write_record.get_rid().page_no}, true);
            break;
        }
        case WriteType::UPDATE_TUPLE:
        {
            /**
             * @description: 回滚更新操作
             * 1. 获取更新前的记录。
             * 2. 记录更新日志：将记录回滚到更新前的状态。
             * 3. 执行更新操作：恢复记录的旧值。
             * 4. 更新缓冲区：取消页面的固定。
             * 5. 更新索引条目：将记录的索引条目恢复到旧值。
             */
            auto fh_ = sm_manager_->fhs_[write_record.get_file_name()].get();
            auto old_record_ = fh_->get_record(write_record.get_rid(), nullptr);
            log_manager->add_update_log_to_buffer(txn->get_transaction_id(), write_record.get_record(), *old_record_, write_record.get_rid(), write_record.get_file_name());
            fh_->update_record(write_record.get_rid(), write_record.get_record().data, nullptr);
            sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), write_record.get_rid().page_no}, true);
            break;
        }
        case WriteType::IX_INSERT_TUPLE:
        {
            /**
             * @description: 回滚索引插入操作
             * 1. 执行删除操作：删除插入的索引条目。
             */
            // 获取索引句柄并删除插入的索引条目
            sm_manager_->ihs_[write_record.get_file_name()]->delete_entry(write_record.get_record().data, write_record.get_rid(), nullptr);
            break;
        }
        case WriteType::IX_DELETE_TUPLE:
        {
            /**
             * @description: 回滚索引删除操作
             * 1. 执行插入操作：插回删除的索引条目。
             */
            // 获取索引句柄并插入删除的索引条目
            sm_manager_->ihs_[write_record.get_file_name()]->insert_entry(write_record.get_record().data, write_record.get_rid(), nullptr);
            break;
        }
        }
    }

    commit(txn, log_manager);
}
