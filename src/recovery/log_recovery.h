#pragma once

#include <transaction/transaction_manager.h>

#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

class RecoveryManager
{
public:
    RecoveryManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, SmManager *sm_manager, LogManager *log_manager, TransactionManager *transaction_manager)
    {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
        log_manager_ = log_manager;
        transaction_manager_ = transaction_manager;
    }

    void recovery();

    void create_static_check_point();

    void crash();

private:
    DiskManager *disk_manager_;              // 用来读写文件
    BufferPoolManager *buffer_pool_manager_; // 对页面进行读写
    SmManager *sm_manager_;                  // 访问数据库元数据
    LogManager *log_manager_;                // 维护元数据
    TransactionManager *transaction_manager_;

    std::shared_mutex latch_;

    void redo_one_log(LogRecord *log_record);

    void undo_one_log(LogRecord *log_record);

    void save_into_disk()
    {
        for (const auto &[tab_name_, fh_] : sm_manager_->fhs_)
        {
            auto file_hdr_ = fh_->get_file_hdr();
            disk_manager_->write_page(fh_->GetFd(), RM_FILE_HDR_PAGE, (char *)(&file_hdr_), sizeof(file_hdr_));
            buffer_pool_manager_->flush_all_pages(fh_->GetFd());
        }

        disk_manager_->write_log(STATIC_CHECK_POINT_STR, std::strlen(STATIC_CHECK_POINT_STR));
        sm_manager_->set_log_offset(disk_manager_->get_file_size(LOG_FILE_NAME));
    }

    static constexpr auto STATIC_CHECK_POINT_STR = "[[STATIC_CHECK_POINT]]\n\n";
};
