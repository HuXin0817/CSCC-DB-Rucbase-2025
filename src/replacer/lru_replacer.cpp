#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

/**
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LRUReplacer::victim(frame_id_t *frame_id)
{
    //  利用lru_replacer中的LRUlist_,LRUHash_实现LRU策略
    //  选择合适的frame指定为淘汰页面,赋值给*frame_id
    std::scoped_lock lock{latch_}; // 如果编译报错可以替换成其他lock

    if (LRUlist_.empty())
    {
        return false;
    }

    // 获取最近最少使用的页面（位于LRUlist_的末尾）
    *frame_id = LRUlist_.back();

    // 从LRUReplacer中移除该页面
    LRUlist_.pop_back();
    LRUhash_.erase(*frame_id);

    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id)
{
    // 固定指定id的frame
    // 在数据结构中移除该frame
    std::scoped_lock lock{latch_};

    auto it = LRUhash_.find(frame_id);
    if (it != LRUhash_.end())
    {
        LRUlist_.erase(it->second);
        LRUhash_.erase(it);
    }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id)
{
    //  支持并发锁
    //  选择一个frame取消固定
    std::scoped_lock lock{latch_};

    // 如果frame已经存在于hash中，则直接返回
    if (LRUhash_.find(frame_id) != LRUhash_.end())
    {
        return;
    }

    // 将frame添加到LRUlist_的头部
    LRUlist_.push_front(frame_id);
    LRUhash_[frame_id] = LRUlist_.begin();
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
