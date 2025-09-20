#pragma once

#include <memory>
#include <string>
#include <vector>

#include "execution_manager_finals.h"
#include "executor_gap_lock_finals.h"
#include "executor_abstract_finals.h"
#include "record/rm_scan_finals.h"

class SeqScanExecutor : public AbstractExecutor
{
private:
    std::string tab_name_; // 表的名称
    TabMeta *tab_;
    RmFileHandle *fh_;           // 表的数据文件句柄
    std::vector<ColMeta> *cols_; // scan后生成的记录的字段
    size_t len_;                 // scan后生成的每条记录的长度

    char *rid_ = nullptr;
    std::unique_ptr<RecScan> scan_; // table_iterator

    SmManager *sm_manager_;

    std::unique_ptr<GapLockExecutor> gap_lock;
    Context *context_;

public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, const std::vector<Condition> &conds, Context *context) : tab_name_(std::move(tab_name)), sm_manager_(sm_manager)
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);

        fh_ = sm_manager_->fhs_[tab_->fd_].get();
        cols_ = &tab_->cols;
        len_ = fh_->record_size;

        context_ = context;

        gap_lock = std::make_unique<GapLockExecutor>(sm_manager, tab_, conds, context_);
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return *cols_; }

    void beginTuple() override
    {
        // 初始化扫描表
        if (!tab_->indexes.empty() && fh_->ban)
        {
            auto ih_ = sm_manager_->ihs_[tab_->indexes.begin()->fd_].get();
            scan_ = std::make_unique<IxScan>(ih_->begin(), ih_->end());
        }
        else
        {
            scan_ = std::make_unique<RmScan>(fh_);
        }

        find_next_valid_tuple();
    }

    void nextTuple() override
    {
        scan_->next();
        find_next_valid_tuple();
    }

    std::unique_ptr<RmRecord> Next() override { return fh_->get_record(rid_); }

    bool is_end() const override { return scan_->is_end(); }

    char *rid() const override { return rid_; }

private:
    void find_next_valid_tuple()
    {
        while (!scan_->is_end())
        {
            rid_ = scan_->rid();
            if (gap_lock->gap->overlap(rid_))
            {
                return;
            }
            scan_->next();
        }
    }
};
