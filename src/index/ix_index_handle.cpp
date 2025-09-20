#include "ix_index_handle.h"

#include <common/common.h>

#define USE_BINARY_SEARCH

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const
{
#ifdef USE_BINARY_SEARCH
    int left = 0, right = page_hdr->num_key;
    while (left < right)
    {
        int mid = left + (right - left) / 2;
        if (ix_compare(target, get_key(mid), file_hdr->col_types_, file_hdr->col_lens_) > 0)
        {
            left = mid + 1;
        }
        else
        {
            right = mid;
        }
    }

    return left;
#else
    for (int i = 0; i < page_hdr->num_key; ++i)
    {
        if (ix_compare(get_key(i), target, file_hdr->col_types_, file_hdr->col_lens_) >= 0)
        {
            return i;
        }
    }
    return page_hdr->num_key;
#endif
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const
{
#ifdef USE_BINARY_SEARCH
    int left = 1, right = page_hdr->num_key;
    while (left < right)
    {
        int mid = left + (right - left) / 2;
        if (ix_compare(target, get_key(mid), file_hdr->col_types_, file_hdr->col_lens_) >= 0)
        {
            left = mid + 1;
        }
        else
        {
            right = mid;
        }
    }

    return left;
#else
    for (int i = 0; i < page_hdr->num_key; ++i)
    {
        if (ix_compare(get_key(i), target, file_hdr->col_types_, file_hdr->col_lens_) > 0)
        {
            return i;
        }
    }
    return page_hdr->num_key;
#endif
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 *
 * 此函数在当前叶子节点中查找目标键值，并返回键值对是否存在它首先遍历节点中的所有键，
 * 并将每个键与目标键进行比较如果找到匹配的键，则返回true，并将对应的Rid通过参数传出
 * 如果没有找到匹配的键，则返回false
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value)
{
    auto index = lower_bound(key);
    if (index == page_hdr->num_key)
    {
        return false;
    }

    if (ix_compare(key, get_key(index), file_hdr->col_types_, file_hdr->col_lens_))
    {
        return false;
    }

    *value = get_rid(index);

    return true;
}

/**
 * @brief 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 *
 * 该函数在当前非叶子节点中查找目标键所在的孩子节点（子树），并返回该孩子节点的存储页面编号
 *
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) const
{
    auto idx = upper_bound(key);

    auto child_page_no = get_rid(idx - 1)->page_no;

    return child_page_no;
}

/**
 * @brief 在指定位置插入n个连续的键值对
 *
 * 该函数将键值对插入到指定位置，并在必要时移动现有的键值对以腾出空间插入的键值对来自提供的键值和RID数组
 *
 * @param pos 要插入键值对的位置
 * @param key 连续键值对的起始地址
 * @param rid 连续键值对的起始地址
 * @param n 键值对数量
 * @note 键值对将插入到pos位置，原有的键值对将移动以腾出空间
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n)
{
    if (pos < 0 || pos > page_hdr->num_key)
    {
        return;
    }

    int num = page_hdr->num_key - pos;

    char *key_slot = get_key(pos);

    int length = file_hdr->col_tot_len_;
    memmove(key_slot + n * file_hdr->col_tot_len_, key_slot, num * length);
    memcpy(key_slot, key, n * length);

    Rid *rid_slot = get_rid(pos);
    length = sizeof(Rid);
    memmove(rid_slot + n, rid_slot, num * length);
    memcpy(rid_slot, rid, n * length);
    page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对
 * 该函数会在指定位置插入一个键值对，并返回插入后的键值对数量
 *
 * @param key 要插入的键
 * @param value 要插入的值（RID）
 * @return int 插入后结点中键值对的数量
 */
int IxNodeHandle::insert(const char *key, const Rid &value)
{
    int idx = lower_bound(key);

    if (idx != page_hdr->num_key && ix_compare(key, get_key(idx), file_hdr->col_types_, file_hdr->col_lens_) == 0)
    {
        throw IndexEntryAlreadyExistError();
    }

    if (idx == page_hdr->num_key)
    {
        insert_pair(idx, key, value);
        return page_hdr->num_key;
    }
    if (ix_compare(key, get_key(idx), file_hdr->col_types_, file_hdr->col_lens_) != 0)
    {
        insert_pair(idx, key, value);
        return page_hdr->num_key;
    }
    return page_hdr->num_key;
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos)
{
    if (pos >= page_hdr->num_key || pos < 0)
    {
        return;
    }

    int size_ = page_hdr->num_key - pos - 1;
    char *key_slot = get_key(pos);
    int length = file_hdr->col_tot_len_;
    memmove(key_slot, key_slot + length, size_ * length);
    Rid *rid_slot = get_rid(pos);
    length = sizeof(Rid);
    memmove(rid_slot, rid_slot + 1, size_ * length);
    page_hdr->num_key -= 1;
}

/**
 * @brief 用于在结点中删除指定key的键值对函数返回删除后的键值对数量
 * 该函数会查找并删除指定的key对应的键值对，并返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return int 删除操作后结点中剩余的键值对数量
 */
int IxNodeHandle::remove(const char *key)
{
    int index = lower_bound(key);

    if (index != page_hdr->num_key && !ix_compare(key, get_key(index), file_hdr->col_types_, file_hdr->col_lens_))
    {
        erase_pair(index);
    }

    return page_hdr->num_key;
}

/**
 * @brief IxIndexHandle 构造函数
 *
 * 该构造函数用于初始化一个 IxIndexHandle 对象它从磁盘读取文件头信息，
 * 并设置当前文件的页面编号
 *
 * @param disk_manager 磁盘管理器指针
 * @param buffer_pool_manager 缓冲池管理器指针
 * @param fd 文件描述符
 */
IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd) : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd)
{
    auto buf = new_char(PAGE_SIZE);
    memset(buf.get(), 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf.get(), PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf.get());
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 *
 * 该函数从B+树的根节点开始，根据给定的键值不断向下查找，直到找到包含该键值的叶子结点
 * 返回找到的叶子结点以及根结点是否加锁的信息
 *
 * @param key 要查找的目标key值
 * @return IxNodeHandle * 返回目标叶子结点
 * @note 查找完叶子结点后，需要在函数外部解锁并解固定该叶子结点，否则下次锁定该结点会造成阻塞
 */
IxNodeHandle *IxIndexHandle::find_leaf_page(const char *key)
{
    auto root = fetch_node(file_hdr_->root_page_);

    while (!root->page_hdr->is_leaf)
    {
        page_id_t page_no = root->internal_lookup(key);

        buffer_pool_manager_->unpin_page(root->get_page_id(), false);

        root = fetch_node(page_no);
    }

    return root;
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * 该函数从B+树的根节点开始，根据给定的键值不断向下查找，直到找到包含该键值的叶子结点
 * 然后在叶子结点中查找目标键值，并将找到的结果存储在result容器中
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器，存放找到的Rid值
 * @return bool 返回目标键值对是否存在，存在则返回true，否则返回false
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> &result)
{
    auto leaf = find_leaf_page(key);

    Rid *rid = nullptr;
    bool exist = leaf->leaf_lookup(key, &rid);
    if (exist)
    {
        result.emplace_back(*rid);
    }

    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    return exist;
}

/**
 * @brief 将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 *
 * 该函数将给定的B+树节点拆分为两个节点拆分过程中，原节点的一半键值对被移动到新节点
 * 新节点被插入到原节点的右侧，并保持父节点和叶子节点指针的正确性
 *
 * @param node 需要拆分的结点
 * @return 拆分得到的新节点new_node
 * @note 拆分完成后，需要在函数外部对原节点和新节点进行unpin操作
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node)
{
    auto split_node_ = create_node();

    auto pos = node->page_hdr->num_key >> 1;

    split_node_->page_hdr->is_leaf = node->page_hdr->is_leaf;
    split_node_->page_hdr->parent = node->page_hdr->parent;
    split_node_->page_hdr->next_free_page_no = node->page_hdr->next_free_page_no;

    split_node_->insert_pairs(0, node->get_key(pos), node->get_rid(pos), node->page_hdr->num_key - pos);

    node->page_hdr->num_key = pos;

    if (split_node_->page_hdr->is_leaf)
    {
        split_node_->page_hdr->prev_leaf = node->get_page_no();
        split_node_->page_hdr->next_leaf = node->page_hdr->next_leaf;

        if (split_node_->page_hdr->next_leaf != INVALID_PAGE_ID)
        {
            auto next_leaf_ = fetch_node(split_node_->page_hdr->next_leaf);
            next_leaf_->page_hdr->prev_leaf = split_node_->get_page_no();
            buffer_pool_manager_->unpin_page(next_leaf_->get_page_id(), true);
        }

        node->page_hdr->next_leaf = split_node_->get_page_no();
    }
    else
    {
        for (int i = 0; i < split_node_->page_hdr->num_key; i++)
        {
            maintain_child(split_node_, i);
        }
    }

    return split_node_;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node)
{
    if (old_node->get_page_no() == file_hdr_->root_page_)
    {
        auto new_root_ = create_node();
        new_root_->page_hdr->is_leaf = false;
        new_root_->page_hdr->num_key = 0;
        new_root_->page_hdr->parent = INVALID_PAGE_ID;
        new_root_->page_hdr->next_free_page_no = IX_NO_PAGE;

        new_root_->insert_pair(0, old_node->get_key(0), {old_node->get_page_no(), -1});
        new_root_->insert_pair(1, key, {new_node->get_page_no(), -1});

        new_node->page_hdr->parent = old_node->page_hdr->parent = new_root_->get_page_no();

        file_hdr_->root_page_ = new_root_->get_page_no();
    }
    else
    {
        auto parent_node_ = fetch_node(old_node->get_parent_page_no());
        auto pos = parent_node_->find_child(old_node);
        parent_node_->insert_pair(pos + 1, key, {new_node->get_page_no(), -1});

        if (parent_node_->page_hdr->num_key == parent_node_->get_max_size())
        {
            auto split_node_ = split(parent_node_);
            insert_into_parent(parent_node_, split_node_->get_key(0), split_node_);
        }
    }
}

/**
 * @brief 将指定键值对插入到B+树中
 *
 * 该函数从B+树的根节点开始查找合适的叶子节点，然后将指定的键值对插入到该叶子节点中
 * 如果插入后的叶子节点超过了最大容量，则需要进行节点分裂，并将分裂后的新节点插入到父节点中
 * 整个过程中会更新树的结构和文件头的信息
 *
 * @param key 要插入的键值对的键
 * @param value 要插入的键值对的值（RID）
 * @param context 上下文指针，包含事务和锁管理器等信息
 * @return page_id_t 插入到的叶结点的page_no
 * @note 该函数内部会处理页面的分裂和父节点的更新调用者需要确保事务的一致性，并在适当的时候进行事务提交或回滚
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Context *context)
{
    std::unique_lock lock(latch_);

    auto leaf_node_ = find_leaf_page(key);
    int num = leaf_node_->page_hdr->num_key;

    bool exist_;
    try
    {
        exist_ = (num != leaf_node_->insert(key, value));
    }
    catch (const IndexEntryAlreadyExistError &)
    {
        latch_.unlock();
        buffer_pool_manager_->unpin_page(leaf_node_->get_page_id(), exist_);
        throw;
    }

    if (context != nullptr)
    {
        WriteRecord write_record_(WriteType::IX_INSERT_TUPLE, disk_manager_->get_file_name(fd_), value, RmRecord(key, file_hdr_->col_tot_len_));
        context->txn_->append_write_record(write_record_);
    }

    if (exist_ && leaf_node_->page_hdr->num_key == leaf_node_->get_max_size())
    {
        auto split_node_ = split(leaf_node_);
        if (file_hdr_->last_leaf_ == leaf_node_->get_page_no())
        {
            file_hdr_->last_leaf_ = split_node_->get_page_no();
        }
        insert_into_parent(leaf_node_, split_node_->get_key(0), split_node_);
        buffer_pool_manager_->unpin_page(split_node_->get_page_id(), true);
    }

    buffer_pool_manager_->unpin_page(leaf_node_->get_page_id(), exist_);
    return leaf_node_->get_page_id().page_no;
}

/**
 * @brief 删除索引中的一个键值对
 *
 * @param key 要删除的键
 * @param value 要删除的值对应的记录ID
 * @param context 上下文指针，包含事务和锁管理器等信息
 * @return true 如果删除成功
 * @return false 如果删除失败
 */
bool IxIndexHandle::delete_entry(const char *key, const Rid &value, Context *context)
{
    std::unique_lock<std::mutex> lock(latch_);

    if (context != nullptr)
    {
        WriteRecord write_record_(WriteType::IX_DELETE_TUPLE, disk_manager_->get_file_name(fd_), value, RmRecord(key, file_hdr_->col_tot_len_));
        context->txn_->append_write_record(write_record_);
        // context->log_mgr_->add_ix_delete_log_to_buffer(context->txn_->get_transaction_id(), key, value, disk_manager_->get_file_name(fd_));
    }

    auto leaf_node_ = find_leaf_page(key);

    int index = leaf_node_->lower_bound(key);

    bool exist = (index != leaf_node_->page_hdr->num_key) && !ix_compare(key, leaf_node_->get_key(index), file_hdr_->col_types_, file_hdr_->col_lens_);

    if (exist)
    {
        Rid *existing_rid = leaf_node_->get_rid(index);
        if (existing_rid->page_no == value.page_no && existing_rid->slot_no == value.slot_no)
        {
            leaf_node_->erase_pair(index);
        }
        else
        {
            exist = false;
        }
    }

    if (exist)
    {
        coalesce_or_redistribute(leaf_node_);
    }

    buffer_pool_manager_->unpin_page(leaf_node_->get_page_id(), exist);
    return exist;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * 该函数在删除键值对后，根据节点的大小决定是否需要合并或重分配节点
 * 如果节点的大小小于最小值，则需要进行合并或重分配
 *
 * @param node 执行完删除操作的结点
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note 用户需要首先找到输入节点的兄弟节点
 * 如果兄弟节点的大小加上输入节点的大小大于等于2倍的最小大小，则进行重分配
 * 否则，进行合并（Coalesce）
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node)
{
    if (node->get_page_no() == file_hdr_->root_page_)
    {
        return adjust_root(node);
    }

    if (node->page_hdr->num_key >= node->get_min_size())
    {
        maintain_parent(node);
        return false;
    }

    auto parent_node_ = fetch_node(node->get_parent_page_no());

    auto idx_ = parent_node_->find_child(node);

    auto neighbor_node_ = fetch_node(parent_node_->get_rid(idx_ + (idx_ ? -1 : 1))->page_no);

    if (node->page_hdr->num_key + neighbor_node_->page_hdr->num_key >= node->get_min_size() * 2)
    {
        redistribute(neighbor_node_, node, parent_node_, idx_);

        buffer_pool_manager_->unpin_page(parent_node_->get_page_id(), true);
        buffer_pool_manager_->unpin_page(neighbor_node_->get_page_id(), true);
        return false;
    }

    coalesce(&neighbor_node_, &node, &parent_node_, idx_);

    buffer_pool_manager_->unpin_page(parent_node_->get_page_id(), true);
    buffer_pool_manager_->unpin_page(neighbor_node_->get_page_id(), true);
    return true;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 *
 * 该函数在根节点被删除一个键值对后进行调整如果根节点大小小于最小大小，
 * 并且根节点是内部节点且只有一个子节点，则将该子节点提升为新的根节点
 * 如果根节点是叶子节点且为空，则删除根节点
 *
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note 根节点的大小可以小于最小大小，并且该方法仅在coalesce_or_redistribute()中调用
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node)
{
    if (!old_root_node->is_leaf_page() && old_root_node->page_hdr->num_key == 1)
    {
        auto child = fetch_node(old_root_node->get_rid(0)->page_no);

        release_node_handle();

        file_hdr_->root_page_ = child->get_page_no();
        child->set_parent_page_no(IX_NO_PAGE);

        buffer_pool_manager_->unpin_page(child->get_page_id(), true);

        return true;
    }

    if (old_root_node->is_leaf_page() && old_root_node->page_hdr->num_key == 0)
    {
        release_node_handle();

        file_hdr_->root_page_ = 2;

        return true;
    }

    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 *
 * 该函数从一个页面向其兄弟页面重新分配键值对如果index == 0，则将兄弟页面的第一个键值对移动到输入"node"的末尾，
 * 否则将兄弟页面的最后一个键值对移动到输入"node"的头部
 *
 * @param neighbor_node sibling page of input "node" 输入"node"的兄弟页面
 * @param node input from method coalesce_or_redistribute() 方法coalesce_or_redistribute()的输入节点
 * @param parent the parent of "node" and "neighbor_node" "node"和"neighbor_node"的父节点
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 *       index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 *       index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 *       注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, const int &index)
{
    auto erase_pos_ = index ? neighbor_node->page_hdr->num_key - 1 : 0;
    auto insert_pos_ = index ? 0 : node->page_hdr->num_key;

    node->insert_pair(insert_pos_, neighbor_node->get_key(erase_pos_), *(neighbor_node->get_rid(erase_pos_)));

    neighbor_node->erase_pair(erase_pos_);

    maintain_child(node, insert_pos_);

    maintain_parent(index ? node : neighbor_node);
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesce_or_redistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index)
{
    if (!index)
    {
        std::swap(node, neighbor_node);
        index++;
    }

    if ((*node)->is_leaf_page() && (*node)->get_page_no() == file_hdr_->last_leaf_)
    {
        file_hdr_->last_leaf_ = (*neighbor_node)->get_page_no();
    }

    int insert_pos = (*neighbor_node)->page_hdr->num_key;

    (*neighbor_node)->insert_pairs(insert_pos, (*node)->get_key(0), (*node)->get_rid(0), (*node)->page_hdr->num_key);

    for (int i = 0; i < (*node)->page_hdr->num_key; i++)
    {
        maintain_child(*neighbor_node, i + insert_pos);
    }

    if ((*node)->is_leaf_page())
    {
        erase_leaf(*node);
    }

    release_node_handle();

    (*parent)->erase_pair(index);

    return coalesce_or_redistribute(*parent);
}

/**
 * @brief 将Iid转换成Rid
 *
 * 这个函数将Iid（索引内部生成的索引槽位置）转换成Rid（记录的位置）
 * Iid的slot_no作为node的rid_idx（key_idx），每个Iid对应的索引槽存储了一对(key, rid)，
 * 其中key指向要建立索引的属性首地址，rid表示插入/删除记录的位置
 *
 * @param iid 索引槽位置
 * @return Rid 记录的位置
 * @note Iid和Rid存储的信息不同，Rid是上层传递的记录位置，Iid是索引内部生成的索引槽位置
 * @throws IndexEntryNotFoundError 如果iid的slot_no超出范围，则抛出异常
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const
{
    auto node = fetch_node(iid.page_no);

    if (iid.slot_no >= node->page_hdr->num_key)
    {
        throw IndexEntryNotFoundError();
    }

    buffer_pool_manager_->unpin_page(node->get_page_id(), false);

    return *node->get_rid(iid.slot_no);
}

/**
 * @brief 在B+树中查找指定key的下界（即第一个大于等于key的元素）
 *
 * 这个函数在B+树中查找指定key的下界，返回对应的Iid（索引槽位置）
 * 通过调用find_leaf_page找到包含key的叶子节点，然后在叶子节点中查找下界位置
 *
 * @param key 要查找的键值
 * @return Iid 返回对应的Iid（索引槽位置）
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换，可以用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key)
{
    auto node = find_leaf_page(key);

    int key_idx = node->lower_bound(key);
    Iid iid{};

    if (key_idx == node->page_hdr->num_key)
    {
        if (node->get_next_leaf() == IX_LEAF_HEADER_PAGE)
        {
            iid = leaf_end();
        }
        else
        {
            iid = {.page_no = node->get_next_leaf(), .slot_no = 0};
        }
    }
    else
    {
        iid = {.page_no = node->get_page_no(), .slot_no = key_idx};
    }

    buffer_pool_manager_->unpin_page(node->get_page_id(), false);

    return iid;
}

/**
 * @brief 在B+树中查找指定key的上界（即第一个大于key的元素）
 *
 * 这个函数在B+树中查找指定key的上界，返回对应的Iid（索引槽位置）
 * 通过调用find_leaf_page找到包含key的叶子节点，然后在叶子节点中查找上界位置
 *
 * @param key 要查找的键值
 * @return Iid 返回对应的Iid（索引槽位置）
 */
Iid IxIndexHandle::upper_bound(const char *key)
{
    auto node = find_leaf_page(key);

    int key_idx = node->upper_bound(key);
    Iid iid{};

    if (key_idx >= node->page_hdr->num_key)
    {
        if (node->get_next_leaf() == IX_LEAF_HEADER_PAGE)
        {
            iid = leaf_end();
        }
        else
        {
            iid = {.page_no = node->get_next_leaf(), .slot_no = 0};
        }
    }
    else
    {
        iid = {.page_no = node->get_page_no(), .slot_no = key_idx};
    }

    buffer_pool_manager_->unpin_page(node->get_page_id(), false);

    return iid;
}

/**
 * @brief 获取索引中最后一个叶子节点的最后一个键值对的下一个位置
 *
 * 该函数返回一个Iid，指向索引中最后一个叶子节点的最后一个键值对的下一个位置
 * 主要用于IxScan的结束位置标记
 *
 * @return Iid 指向最后一个叶子节点的最后一个键值对的后一个位置
 */
Iid IxIndexHandle::leaf_end() const
{
    auto node = fetch_node(file_hdr_->last_leaf_);

    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->page_hdr->num_key};

    buffer_pool_manager_->unpin_page(node->get_page_id(), false);

    return iid;
}

/**
 * @brief 获取索引中第一个叶子节点的第一个键值对的位置
 *
 * 该函数返回一个Iid，指向索引中第一个叶子节点的第一个键值对的位置
 * 主要用于IxScan的开始位置标记
 *
 * @return Iid 指向第一个叶子节点的第一个键值对的位置
 */
Iid IxIndexHandle::leaf_begin() const
{
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};

    return iid;
}

/**
 * @brief 获取指定页号的节点
 *
 * 该函数根据给定的页号从缓冲池管理器中获取页面，并返回对应的节点句柄
 * 调用者需要确保在函数外部对返回的节点进行unpin操作
 *
 * @param page_no 要获取的节点的页号
 * @return IxNodeHandle* 指向获取的节点句柄
 * @throws InternalError 如果获取页面失败，则抛出内部错误异常
 * @note 记得在函数外部对返回的节点进行unpin操作！
 */
IxNodeHandle *IxIndexHandle::fetch_node(const int &page_no) const
{
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});

    if (page == nullptr)
    {
        throw InternalError("fetch node failed");
    }

    auto node_handle = new IxNodeHandle(file_hdr_, page);

    return node_handle;
}

/**
 * @brief 创建一个新结点
 *
 * 该函数创建一个新的B+树节点，并增加文件头中的页数计数新创建的页面会被标记为有效，并且需要在外部进行unpin操作
 *
 * @return IxNodeHandle* 指向新创建的节点句柄
 * @note 记得在函数外部对返回的节点进行unpin操作！
 * @note 注意：对于索引的处理是，删除某个页面后，认为该被删除的页面是free_page而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create_node，那么first_page_no一直没变，一直是IX_NO_PAGE与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::create_node()
{
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};

    Page *page = buffer_pool_manager_->new_page(&new_page_id);

    auto node_handle = new IxNodeHandle(file_hdr_, page);

    return node_handle;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * 该函数从给定的节点开始，向上遍历其父节点链，更新每个父节点的第一个键值，
 * 直到根节点或发现不需要更新为止
 *
 * @param node 需要开始更新的节点
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node)
{
    auto curr = node;
    IxNodeHandle *parent;

    while (curr->get_parent_page_no() != IX_NO_PAGE)
    {
        parent = fetch_node(curr->get_parent_page_no());

        int rank = parent->find_child(curr);

        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);

        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0)
        {
            buffer_pool_manager_->unpin_page(parent->get_page_id(), false);
            break;
        }

        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);

        buffer_pool_manager_->unpin_page(curr->get_page_id(), true);
        curr = parent;
    }

    buffer_pool_manager_->unpin_page(curr->get_page_id(), true);
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * 该函数在删除指定的叶子节点之前调用，用于更新其前驱和后继叶子节点的指针
 * 具体来说，它会将叶子节点的前驱节点的next指针指向叶子节点的后继节点，
 * 并将后继节点的prev指针指向叶子节点的前驱节点，以确保链表的连通性
 *
 * @param leaf 要删除的叶子节点
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf)
{
    assert(leaf->is_leaf_page());

    auto prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);

    auto next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle() { file_hdr_->num_pages_--; }

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 *
 * 该函数用于更新指定子节点的父节点信息，将其父节点设置为传入的node
 * 该操作仅在node不是叶子节点时进行
 *
 * @param node 父节点
 * @param child_idx 子节点在父节点中的索引位置
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx)
{
    if (!node->is_leaf_page())
    {
        int child_page_no = node->value_at(child_idx);

        auto child = fetch_node(child_page_no);

        child->set_parent_page_no(node->get_page_no());

        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    }
}
