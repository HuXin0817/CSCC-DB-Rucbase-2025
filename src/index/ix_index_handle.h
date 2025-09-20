#pragma once

#include <shared_mutex>

#include "common/context.h"
#include "ix_defs.h"
#include "transaction/transaction.h"

/**
 * @brief 管理B+树中的每个节点
 */
class IxNodeHandle
{
    friend class IxIndexHandle;
    friend class IxScan;
    friend class GapReadLockExecutor;

private:
    const IxFileHdr *file_hdr; // 节点所在文件的头部信息
    Page *page;                // 存储节点的页面
    IxPageHdr *page_hdr;       // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;                // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;                 // page->data的第三部分，指针指向首地址

public:
    IxNodeHandle() = default;

    /**
     * @brief 构造函数，用于初始化节点句柄
     * @param file_hdr_ 指向文件头部的指针
     * @param page_ 指向页面的指针
     */
    IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_) : file_hdr(file_hdr_), page(page_)
    {
        page_hdr = reinterpret_cast<IxPageHdr *>(page->get_data());
        keys = page->get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    /**
     * @brief 获取节点中键值对的数量
     * @return 键值对的数量
     */
    int get_size() const { return page_hdr->num_key; }

    /**
     * @brief 设置节点中键值对的数量
     * @param size 键值对的数量
     */
    void set_size(int size) { page_hdr->num_key = size; }

    /**
     * @brief 获取节点可以容纳的最大键值对数量
     * @return 最大键值对数量
     */
    int get_max_size() { return file_hdr->btree_order_ + 1; }

    /**
     * @brief 获取节点中键值对的最小数量
     * @return 最小键值对数量
     */
    int get_min_size() { return get_max_size() / 2; }

    /**
     * @brief 获取指定位置的键值
     * @param i 键值的位置
     * @return 键值
     */
    int key_at(int i) const { return *(int *)get_key(i); }

    /**
     * @brief 获取指定位置的值（子节点页面号）
     * @param i 值的位置
     * @return 子节点页面号
     */
    page_id_t value_at(int i) const { return get_rid(i)->page_no; }

    /**
     * @brief 获取当前节点的页面号
     * @return 页面号
     */
    page_id_t get_page_no() { return page->get_page_id().page_no; }

    /**
     * @brief 获取当前节点的页面ID
     * @return 页面ID
     */
    PageId get_page_id() { return page->get_page_id(); }

    /**
     * @brief 获取下一个叶子节点的页面号
     * @return 下一个叶子节点的页面号
     */
    page_id_t get_next_leaf() { return page_hdr->next_leaf; }

    /**
     * @brief 获取上一个叶子节点的页面号
     * @return 上一个叶子节点的页面号
     */
    page_id_t get_prev_leaf() { return page_hdr->prev_leaf; }

    /**
     * @brief 获取父节点的页面号
     * @return 父节点的页面号
     */
    page_id_t get_parent_page_no() { return page_hdr->parent; }

    /**
     * @brief 判断节点是否为叶子节点
     * @return 是叶子节点返回true，否则返回false
     */
    bool is_leaf_page() { return page_hdr->is_leaf; }

    /**
     * @brief 判断节点是否为根节点
     * @return 是根节点返回true，否则返回false
     */
    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    /**
     * @brief 设置下一个叶子节点的页面号
     * @param page_no 页面号
     */
    void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

    /**
     * @brief 设置上一个叶子节点的页面号
     * @param page_no 页面号
     */
    void set_prev_leaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }

    /**
     * @brief 设置父节点的页面号
     * @param parent 父节点的页面号
     */
    void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }

    /**
     * @brief 获取指定位置的键指针
     * @param key_idx 键的位置
     * @return 键指针
     */
    char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

    /**
     * @brief 获取指定位置的值（RID）
     * @param rid_idx 值的位置
     * @return RID指针
     */
    Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }

    /**
     * @brief 设置指定位置的键
     * @param key_idx 键的位置
     * @param key 键的值
     */
    void set_key(int key_idx, const char *key) { memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_); }

    /**
     * @brief 设置指定位置的值（RID）
     * @param rid_idx 值的位置
     * @param rid 值（RID）
     */
    void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; }

    /**
     * @brief 在当前节点中查找第一个>=target的key_idx
     * @param target 目标键
     * @return 键的位置
     */
    int lower_bound(const char *target) const;

    /**
     * @brief 在当前节点中查找第一个>target的key_idx
     * @param target 目标键
     * @return 键的位置
     */
    int upper_bound(const char *target) const;

    /**
     * @brief 在指定位置插入n个连续的键值对
     * @param pos 要插入的位置
     * @param key 键值对的起始地址
     * @param rid 键值对的起始地址
     * @param n 键值对数量
     */
    void insert_pairs(int pos, const char *key, const Rid *rid, int n);

    /**
     * @brief 用于内部节点根据key查找子节点
     * @param key 目标键
     * @return 子节点的页面号
     */
    page_id_t internal_lookup(const char *key) const;

    /**
     * @brief 用于叶子节点查找键值对
     * @param key 目标键
     * @param value 查找到的值（输出参数）
     * @return 是否查找成功
     */
    bool leaf_lookup(const char *key, Rid **value);

    /**
     * @brief 在节点中插入键值对
     * @param key 要插入的键
     * @param value 要插入的值（RID）
     * @return 插入后键值对的数量
     */
    int insert(const char *key, const Rid &value);

    /**
     * @brief 在指定位置插入单个键值对
     * @param pos 要插入的位置
     * @param key 要插入的键
     * @param rid 要插入的值（RID）
     */
    void insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }

    /**
     * @brief 删除指定位置的键值对
     * @param pos 要删除的位置
     */
    void erase_pair(int pos);

    /**
     * @brief 删除指定键的键值对
     * @param key 要删除的键
     * @return 删除后键值对的数量
     */
    int remove(const char *key);

    /**
     * @brief 在内部节点中删除最后一个键，并返回最后一个子节点的页面号
     * @return 最后一个子节点的页面号
     */
    page_id_t remove_and_return_only_child()
    {
        assert(get_size() == 1);
        page_id_t child_page_no = value_at(0);
        erase_pair(0);
        assert(get_size() == 0);
        return child_page_no;
    }

    /**
     * @brief 由parent调用，寻找child在parent中的位置
     * @param child 子节点句柄
     * @return 子节点在父节点中的位置
     */
    int find_child(IxNodeHandle *child)
    {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++)
        {
            if (get_rid(rid_idx)->page_no == child->get_page_no())
            {
                break;
            }
        }
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }
};

/**
 * @brief B+树的主类，管理索引操作
 */
class IxIndexHandle
{
    friend class IxScan;
    friend class IxManager;
    friend class IndexScanExecutor;
    friend class GapReadLockExecutor;

private:
    DiskManager *disk_manager_;              // 磁盘管理器，用于管理磁盘操作
    BufferPoolManager *buffer_pool_manager_; // 缓冲池管理器，用于管理内存中的缓存页面
    int fd_;                                 // 存储B+树的文件描述符
    IxFileHdr *file_hdr_;                    // B+树文件头部信息，包含根页面等信息
    std::mutex latch_;

public:
    /**
     * @brief 构造函数，初始化索引句柄
     * @param disk_manager 磁盘管理器指针
     * @param buffer_pool_manager 缓冲池管理器指针
     * @param fd 文件描述符
     */
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

    /**
     * @brief 根据键获取值
     * @param key 键
     * @param result 存储结果的向量
     * @return 是否成功找到值
     */
    bool get_value(const char *key, std::vector<Rid> &result);

    /**
     * @brief 插入键值对
     * @param key 键
     * @param value 值（RID）
     * @param context 对应上下文
     * @return 插入后键值对所在的页面号
     */
    page_id_t insert_entry(const char *key, const Rid &value, Context *context);

    /**
     * @brief 删除键值对
     * @param key 键
     * @param value 值（RID）
     * @param context 对应上下文
     * @return 是否成功删除
     */
    bool delete_entry(const char *key, const Rid &value, Context *context);

    /**
     * @brief 获取B+树文件头部信息
     * @return B+树文件头部信息指针
     */
    IxFileHdr *get_ix_file_hdr() { return file_hdr_; }

    int GetFd() { return fd_; }

private:
    /**
     * @brief 获取第一个大于等于目标键的位置
     * @param key 目标键
     * @return 对应位置的Iid
     */
    Iid lower_bound(const char *key);

    /**
     * @brief 获取第一个大于目标键的位置
     * @param key 目标键
     * @return 对应位置的Iid
     */
    Iid upper_bound(const char *key);

    /**
     * @brief 获取缓冲池管理器
     * @return 缓冲池管理器指针
     */
    BufferPoolManager *get_buffer_pool_manager() { return buffer_pool_manager_; }

    /**
     * @brief 获取指定页号的节点
     * @param page_no 页号
     * @return 对应的节点句柄
     */
    IxNodeHandle *fetch_node(const int &page_no) const;

    /**
     * @brief 将节点分裂成两个节点
     * @param node 要分裂的节点
     * @return 分裂后新节点的句柄
     */
    IxNodeHandle *split(IxNodeHandle *node);

    /**
     * @brief 将键值对插入到父节点中
     * @param old_node 原节点
     * @param key 键
     * @param new_node 新节点
     */
    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node);

    /**
     * @brief 合并或重分配节点
     * @param node 要处理的节点
     * @return 是否需要删除节点
     */
    bool coalesce_or_redistribute(IxNodeHandle *node = nullptr);

    /**
     * @brief 重分配键值对
     * @param neighbor_node 兄弟节点
     * @param node 当前节点
     * @param parent 父节点
     * @param index 当前节点在父节点中的索引
     */
    void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, const int &index);

    /**
     * @brief 合并节点
     * @param neighbor_node 兄弟节点指针
     * @param node 当前节点指针
     * @param parent 父节点指针
     * @param index 当前节点在父节点中的索引
     * @return 是否需要删除父节点
     */
    bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index);

    /**
     * @brief 查找包含目标键的叶子节点
     * @param key 目标键
     * @return 包含目标键的叶子节点句柄
     */
    IxNodeHandle *find_leaf_page(const char *key);

    /**
     * @brief 获取索引中最后一个叶子节点的最后一个键值对的下一个位置
     * @return 对应位置的Iid
     */
    Iid leaf_end() const;

    /**
     * @brief 获取索引中第一个叶子节点的第一个键值对的位置
     * @return 对应位置的Iid
     */
    Iid leaf_begin() const;

    /**
     * @brief 判断索引是否为空
     * @return 如果索引为空，返回true；否则返回false
     */
    bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

    /**
     * @brief 调整根节点
     * @param old_root_node 原根节点
     * @return 是否需要删除根节点
     */
    bool adjust_root(IxNodeHandle *old_root_node);

    /**
     * @brief 创建一个新节点
     * @return 新节点句柄
     */
    IxNodeHandle *create_node();

    /**
     * @brief 更新父节点
     * @param node 要更新的节点
     */
    void maintain_parent(IxNodeHandle *node);

    /**
     * @brief 删除叶子节点
     * @param leaf 要删除的叶子节点
     */
    void erase_leaf(IxNodeHandle *leaf);

    /**
     * @brief 删除节点并更新页面计数
     * @param node 要删除的节点
     */
    void release_node_handle();

    /**
     * @brief 更新子节点的父节点指针
     * @param node 父节点
     * @param child_idx 子节点在父节点中的索引
     */
    void maintain_child(IxNodeHandle *node, int child_idx);

    /**
     * @brief 将Iid转换为Rid
     * @param iid 索引槽位置
     * @return 记录的位置
     */
    Rid get_rid(const Iid &iid) const;
};
