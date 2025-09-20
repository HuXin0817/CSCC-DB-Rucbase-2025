#pragma once

#include <climits>
#include <cstring>
#include <memory>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "executor_gap_lock.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @class IndexScanExecutor
 * @brief 执行索引扫描的执行器
 */
class IndexScanExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;                              // 表的元数据
    RmFileHandle *fh_;                         // 表的数据文件句柄
    std::vector<ColMeta> cols_;                // 表的列元数据
    size_t len_;                               // 每个记录的长度
    IxIndexHandle *ih_;                        // 索引句柄
    std::vector<std::string> index_col_names_; // 索引列的名称
    IndexMeta index_meta_;                     // 索引的元数据
    std::unique_ptr<IxScan> ix_scan_;          // 索引扫描器
    Rid rid_{};                                // 当前记录的标识符
    std::unique_ptr<RecScan> scan_;            // 记录扫描器
    SmManager *sm_manager_;                    // 系统管理器
    bool is_range_query = false;               // 是否是范围查询
    std::vector<Condition> conds_;             // 查询条件
    std::vector<Condition> fed_conds_;         // 已经使用的条件
    std::vector<Condition> other_conds_;       // 其他条件
    std::vector<Condition> range_cond_;        // 范围条件

public:
    /**
     * @brief 构造函数，初始化索引扫描执行器
     * @param sm_manager 系统管理器
     * @param tab_name 表名
     * @param conds 查询条件
     * @param index_col_names 索引列名称
     * @param context 上下文
     */
    IndexScanExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names, Context *context)
    {
        sm_manager_ = sm_manager;                    // 初始化系统管理器
        context_ = context;                          // 初始化上下文
        tab_ = sm_manager_->db_.get_table(tab_name); // 获取表的元数据
        conds_ = std::move(conds);                   // 移动查询条件到成员变量

        index_col_names_ = std::move(index_col_names);          // 移动索引列名称到成员变量
        index_meta_ = *(tab_.get_index_meta(index_col_names_)); // 获取索引元数据
        fh_ = sm_manager_->fhs_.at(tab_name).get();             // 获取表的数据文件句柄
        cols_ = tab_.cols;                                      // 获取表的列元数据
        len_ = cols_.back().offset + cols_.back().len;          // 计算每个记录的长度

        // 遍历查询条件和索引列名称，分类条件
        for (int i = 0; i < static_cast<int>(std::min(conds_.size(), index_col_names_.size())); i++)
        {
            // 检查条件是否为前缀查询条件
            if (!is_range_query && conds_[i].is_rhs_val && conds_[i].lhs_col.col_name == index_col_names_[i] && conds_[i].op != CompOp::OP_NE)
            {
                if (conds_[i].op == CompOp::OP_EQ)
                {
                    fed_conds_.push_back(conds_[i]); // 添加到前缀查询条件
                }
                else if (conds_[i].op != CompOp::OP_EQ)
                {
                    range_cond_.push_back(conds_[i]); // 添加到范围查询条件
                    is_range_query = true;            // 设置为范围查询
                }
                else
                {
#ifdef DEBUG
                    assert(0); // 在调试模式下，如果操作符不匹配，则断言失败
#endif
                }
            }
            else
            {
                other_conds_.push_back(conds_[i]); // 添加到其他条件
            }
        }

        // 获取索引文件名称并初始化索引句柄
        auto ix_file_name = IxManager::get_index_name(tab_.name, index_meta_.cols);
        ih_ = sm_manager_->ihs_.at(ix_file_name).get();
    }

    /**
     * @brief 开始索引扫描的初始化操作
     */
    void beginTuple() override
    {
        GapReadLockExecutor gap_read_lock_executor(sm_manager_, tab_.name, conds_, index_col_names_, context_);
        gap_read_lock_executor.beginTuple();

        // 分配内存存储索引键的最小值和最大值
        auto lower_key_ = new_char(ih_->file_hdr_->col_tot_len_);
        auto upper_key_ = new_char(ih_->file_hdr_->col_tot_len_);

        Iid lower_position_{}; // 用于存储索引扫描的起始位置
        Iid upper_position_{}; // 用于存储索引扫描的结束位置

        auto curr_offset_ = 0; // 当前索引键的偏移量

        // 处理前缀查询条件
        for (auto &fed_cond : fed_conds_)
        {
            auto col_meta_ = tab_.get_col(fed_cond.lhs_col.col_name);                            // 获取列元数据
            memcpy(lower_key_.get() + curr_offset_, fed_cond.rhs_val.raw->data, col_meta_->len); // 将条件中的值复制到lower_key_
            memcpy(upper_key_.get() + curr_offset_, fed_cond.rhs_val.raw->data, col_meta_->len); // 将条件中的值复制到upper_key_
            curr_offset_ += col_meta_->len;                                                      // 更新偏移量
        }

        // 处理范围查询条件
        if (is_range_query)
        {
            auto col_meta_ = tab_.get_col(range_cond_[0].lhs_col.col_name);                            // 获取列元数据
            memcpy(lower_key_.get() + curr_offset_, range_cond_[0].rhs_val.raw->data, col_meta_->len); // 将条件中的值复制到lower_key_
            memcpy(upper_key_.get() + curr_offset_, range_cond_[0].rhs_val.raw->data, col_meta_->len); // 将条件中的值复制到upper_key_
            curr_offset_ += col_meta_->len;                                                            // 更新偏移量
        }

        // 处理索引列的最小值和最大值
        for (auto i = fed_conds_.size() + static_cast<size_t>(is_range_query); i < index_col_names_.size(); i++)
        {
            auto col_meta_ = tab_.get_col(index_meta_.cols[i].name); // 获取列元数据

            // 内联处理类型最小值
            switch (col_meta_->type)
            {
            case ColType::TYPE_INT:
            {
                int min_int = std::numeric_limits<int>::min();                     // 获取int类型的最小值
                memcpy(lower_key_.get() + curr_offset_, &min_int, col_meta_->len); // 将最小值复制到lower_key_
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                float float_min = std::numeric_limits<float>::lowest();              // 获取float类型的最小值
                memcpy(lower_key_.get() + curr_offset_, &float_min, col_meta_->len); // 将最小值复制到lower_key_
                break;
            }
            case ColType::TYPE_STRING:
                memset(lower_key_.get() + curr_offset_, 0x00, col_meta_->len); // 将字符串类型的最小值设置为全零
                break;
            default:
                break;
            }

            // 内联处理类型最大值
            switch (col_meta_->type)
            {
            case ColType::TYPE_INT:
            {
                auto max_int = std::numeric_limits<int>::max();                    // 获取int类型的最大值
                memcpy(upper_key_.get() + curr_offset_, &max_int, col_meta_->len); // 将最大值复制到upper_key_
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                auto max_float = std::numeric_limits<float>::max();                  // 获取float类型的最大值
                memcpy(upper_key_.get() + curr_offset_, &max_float, col_meta_->len); // 将最大值复制到upper_key_
                break;
            }
            case ColType::TYPE_STRING:
                memset(upper_key_.get() + curr_offset_, 0xff, col_meta_->len); // 将字符串类型的最大值设置为全ff
                break;
            default:
                break;
            }

            curr_offset_ += col_meta_->len; // 更新偏移量
        }

        // 设置索引扫描的范围
        if (is_range_query)
        {
            switch (range_cond_[0].op)
            {
            case CompOp::OP_LE:
            case CompOp::OP_LT:
                lower_position_ = Iid({ih_->file_hdr_->first_leaf_, 0});                                               // 设置扫描的起始位置为第一个叶子节点
                upper_position_ = ih_->upper_bound(upper_key_.get());                                                  // 获取上界位置
                ix_scan_ = std::make_unique<IxScan>(ih_, lower_position_, upper_position_, ih_->buffer_pool_manager_); // 初始化索引扫描器
                break;
            default:
                lower_position_ = ih_->lower_bound(lower_key_.get());                                                  // 获取下界位置
                auto last_leaf = ih_->fetch_node(ih_->file_hdr_->last_leaf_);                                          // 获取最后一个叶子节点
                upper_position_.page_no = ih_->file_hdr_->last_leaf_;                                                  // 设置上界位置为最后一个叶子节点
                upper_position_.slot_no = last_leaf->get_size();                                                       // 设置上界位置为最后一个槽位
                ih_->buffer_pool_manager_->unpin_page(last_leaf->get_page_id(), false);                                // 解除页面固定
                delete last_leaf;                                                                                      // 删除最后一个叶子节点
                ix_scan_ = std::make_unique<IxScan>(ih_, lower_position_, upper_position_, ih_->buffer_pool_manager_); // 初始化索引扫描器
                break;
            }
        }
        else
        {
            lower_position_ = ih_->lower_bound(lower_key_.get());                                                  // 获取下界位置
            upper_position_ = ih_->upper_bound(upper_key_.get());                                                  // 获取上界位置
            ix_scan_ = std::make_unique<IxScan>(ih_, lower_position_, upper_position_, ih_->buffer_pool_manager_); // 初始化索引扫描器
        }

        // 查找满足条件的记录
        while (!ix_scan_->is_end())
        {
            rid_ = ix_scan_->rid();                      // 获取当前记录的标识符
            auto rec_ = fh_->get_record(rid_, context_); // 获取记录

            if (compare(cols_, conds_, rec_))
            {                                                                                       // 比较记录是否满足条件
                sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), rid_.page_no}, false); // 解除页面固定
                break;                                                                              // 记录满足条件，退出循环
            }

            sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), rid_.page_no}, false); // 解除页面固定
            ix_scan_->next();                                                                   // 移动到下一个记录
        }
    }

    /**
     * @brief 获取下一个满足条件的记录
     */
    void nextTuple() override
    {
        for (ix_scan_->next(); !ix_scan_->is_end(); ix_scan_->next())
        {
            rid_ = ix_scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (compare(cols_, conds_, rec))
            {
                sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), rid_.page_no}, false);
                break;
            }
            sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), rid_.page_no}, false);
        }
    }

    /**
     * @brief 获取当前记录的数据
     * @return 当前记录的数据
     */
    std::unique_ptr<RmRecord> Next() override
    {
#ifdef DEBUG
        assert(!is_end());
#endif
        auto rec_ = fh_->get_record(rid_, context_);
        auto rec_copy_ = std::make_unique<RmRecord>(rec_->size, rec_->data);
        sm_manager_->buffer_pool_manager_->unpin_page({fh_->GetFd(), rid_.page_no}, false);
        return rec_copy_;
    }

    /**
     * @brief 检查索引扫描是否结束
     * @return 是否结束
     */
    bool is_end() const override { return ix_scan_->is_end(); }

    /**
     * @brief 获取当前记录的标识符
     * @return 当前记录的标识符
     */
    Rid &rid() override { return rid_; }

    /**
     * @brief 获取每个记录的长度
     * @return 记录的长度
     */
    size_t tupleLen() const override { return len_; }

    /**
     * @brief 获取表的列元数据
     * @return 表的列元数据
     */
    const std::vector<ColMeta> &cols() const override { return cols_; }
};
