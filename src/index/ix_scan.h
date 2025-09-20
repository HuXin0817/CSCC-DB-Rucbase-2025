#pragma once

#include <mutex>

#include "ix_defs.h"
#include "ix_index_handle.h"

/**
 * @class IxScan
 * @brief 用于遍历叶子结点的扫描类
 *
 * IxScan 类继承自 RecScan 类，提供了在 B+ 树的叶子结点中进行扫描的功能
 * 它使用起始和结束的索引（Iid）来限制扫描的范围，并通过 BufferPoolManager
 * 来管理页面缓冲
 */
class IxScan : public RecScan
{
private:
    const IxIndexHandle *ih_; // 索引句柄
    Iid iid_;                 // 当前扫描的位置，初始为 lower（用于遍历的指针）
    Iid end_;                 // 扫描结束的位置，初始为 upper
    BufferPoolManager *bpm_;  // 页面缓冲管理器

public:
    txn_id_t txn_id_{}; // 事务id

    /**
     * @brief 构造函数
     * @param ih 索引句柄
     * @param lower 扫描起始位置
     * @param upper 扫描结束位置
     * @param bpm 页面缓冲管理器
     */
    explicit IxScan(const IxIndexHandle *ih, const Iid &lower, const Iid &upper, BufferPoolManager *bpm) : ih_(ih), iid_(lower), end_(upper), bpm_(bpm) {}

    void next() override;

    /**
     * @brief 判断扫描是否到达末尾
     * @return 如果扫描到达末尾，返回 true；否则返回 false
     */
    bool is_end() const override { return iid_ == end_; }

    /**
     * @brief 获取当前记录的 RID
     * @return 当前记录的 RID
     */
    Rid rid() const override { return ih_->get_rid(iid_); }

    /**
     * @brief 获取当前扫描的位置
     * @return 当前扫描的位置 Iid
     */
    const Iid &iid() const { return iid_; }
};
