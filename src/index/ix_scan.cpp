#include "ix_scan.h"

/**
 * @brief IxScan::next
 *
 * 移动到下一个记录的位置，若当前记录是叶子节点的最后一个记录，
 * 则移动到下一个叶子节点的第一个记录。
 */
void IxScan::next()
{
    // 获取当前节点
    auto node = ih_->fetch_node(iid_.page_no);

    // 增加 slot_no，指向下一个记录
    iid_.slot_no++;

    // 检查是否需要移动到下一个叶子节点
    // 如果当前节点不是最后一个叶子节点，且当前记录是叶子节点的最后一个记录
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node->get_size())
    {
        // 移动到下一个叶子节点
        iid_.slot_no = 0;                     // 重置 slot_no，指向下一个叶子节点的第一个记录
        iid_.page_no = node->get_next_leaf(); // 获取下一个叶子节点的页面号
    }

    // 释放页面，解除固定
    bpm_->unpin_page(node->get_page_id(), false);
}
