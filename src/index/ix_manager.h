#pragma once

#include <shared_mutex>
#include <string>
#include <vector>

#include "ix_defs.h"
#include "ix_index_handle.h"
#include "system/sm_meta.h"

/**
 * @brief 索引管理器类，用于管理索引文件的创建、销毁、打开和关闭等操作
 */
class IxManager
{
private:
    DiskManager *disk_manager_;              // 磁盘管理器指针
    BufferPoolManager *buffer_pool_manager_; // 缓冲池管理器指针

public:
    /**
     * @brief 构造函数
     *
     * @param disk_manager 磁盘管理器指针
     * @param buffer_pool_manager 缓冲池管理器指针
     */
    IxManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager) : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {}

    /**
     * @brief 获取索引文件的名称（带扩展名）
     *
     * @param filename 原始文件名
     * @param index_cols 索引涉及的字段
     * @return std::string 索引文件名
     */
    static std::string get_index_name(const std::string &filename, const std::vector<std::string> &index_cols)
    {
        std::string index_name = filename;
        for (const auto &index_col : index_cols)
            index_name += "_" + index_col;
        index_name += ".idx";

        return index_name;
    }

    /**
     * @brief 获取索引文件的名称（带扩展名）
     *
     * @param filename 原始文件名
     * @param index_cols 索引涉及的字段元数据
     * @return std::string 索引文件名
     */
    static std::string get_index_name(const std::string &filename, const std::vector<ColMeta> &index_cols)
    {
        std::string index_name = filename;
        for (const auto &index_col : index_cols)
            index_name += "_" + index_col.name;
        index_name += ".idx";

        return index_name;
    }

    /**
     * @brief 检查指定索引是否存在
     *
     * @param filename 文件名
     * @param index_cols 索引涉及的字段元数据
     * @return bool 索引是否存在
     */
    bool exists(const std::string &filename, const std::vector<ColMeta> &index_cols)
    {
        auto ix_name = get_index_name(filename, index_cols);
        return disk_manager_->is_file(ix_name);
    }

    /**
     * @brief 检查指定索引是否存在
     *
     * @param filename 文件名
     * @param index_cols 索引涉及的字段名
     * @return bool 索引是否存在
     */
    bool exists(const std::string &filename, const std::vector<std::string> &index_cols)
    {
        auto ix_name = get_index_name(filename, index_cols);
        return disk_manager_->is_file(ix_name);
    }

    /**
     * @brief 创建索引文件
     *
     * @param filename 文件名
     * @param index_cols 索引涉及的字段元数据
     */
    void create_index(const std::string &filename, const std::vector<ColMeta> &index_cols)
    {
        std::string ix_name = get_index_name(filename, index_cols);
        // 创建索引文件
        disk_manager_->create_file(ix_name);
        // 打开索引文件
        int fd = disk_manager_->open_file(ix_name);

        // 创建文件头并写入文件
        int col_tot_len = 0;
        int col_num = index_cols.size();
        for (auto &col : index_cols)
        {
            col_tot_len += col.len;
        }
        if (col_tot_len > IX_MAX_COL_LEN)
        {
            throw InvalidColLengthError(col_tot_len);
        }
        // 计算 B+ 树的阶数（btree_order），即每个节点最多可插入的键值对数量
        int btree_order = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(Rid)) - 1);
        assert(btree_order > 2);

        // 创建文件头并写入文件
        auto *fhdr = new IxFileHdr(IX_NO_PAGE, IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE, col_num, col_tot_len, btree_order, (btree_order + 1) * col_tot_len, IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE);
        for (int i = 0; i < col_num; ++i)
        {
            fhdr->col_types_.push_back(index_cols[i].type);
            fhdr->col_lens_.push_back(index_cols[i].len);
        }
        fhdr->update_tot_len();

        auto data = new_char(fhdr->tot_len_);
        fhdr->serialize(data.get());

        disk_manager_->write_page(fd, IX_FILE_HDR_PAGE, data.get(), fhdr->tot_len_);

        char page_buf[PAGE_SIZE]; // 初始化页面缓冲区，并将其写入磁盘
        memset(page_buf, 0, PAGE_SIZE);
        // 创建叶子列表头页面并写入文件
        {
            memset(page_buf, 0, PAGE_SIZE);
            auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
            *phdr = {
                .next_free_page_no = IX_NO_PAGE,
                .parent = IX_NO_PAGE,
                .num_key = 0,
                .is_leaf = true,
                .prev_leaf = IX_INIT_ROOT_PAGE,
                .next_leaf = IX_INIT_ROOT_PAGE,
            };
            disk_manager_->write_page(fd, IX_LEAF_HEADER_PAGE, page_buf, PAGE_SIZE);
        }
        // 创建根节点并写入文件
        {
            memset(page_buf, 0, PAGE_SIZE);
            auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
            *phdr = {
                .next_free_page_no = IX_NO_PAGE,
                .parent = IX_NO_PAGE,
                .num_key = 0,
                .is_leaf = true,
                .prev_leaf = IX_LEAF_HEADER_PAGE,
                .next_leaf = IX_LEAF_HEADER_PAGE,
            };
            // 必须写入 PAGE_SIZE，以防止将来 fetch_node() 时出错
            disk_manager_->write_page(fd, IX_INIT_ROOT_PAGE, page_buf, PAGE_SIZE);
        }

        disk_manager_->set_fd2pageno(fd, IX_INIT_NUM_PAGES - 1); // 设置文件页号

        disk_manager_->close_file(fd); // 关闭索引文件
    }

    /**
     * @brief 销毁索引文件
     *
     * @param filename 文件名
     * @param index_cols 索引涉及的字段元数据
     */
    void destroy_index(const std::string &filename, const std::vector<ColMeta> &index_cols)
    {
        std::string ix_name = get_index_name(filename, index_cols);
        disk_manager_->destroy_file(ix_name);
    }

    /**
     * @brief 销毁索引文件
     *
     * @param filename 文件名
     * @param index_cols 索引涉及的字段名
     */
    void destroy_index(const std::string &filename, const std::vector<std::string> &index_cols)
    {
        std::string ix_name = get_index_name(filename, index_cols);
        disk_manager_->destroy_file(ix_name);
    }

    /**
     * @brief 打开索引文件，创建并返回索引文件句柄指针
     *
     * @param filename 文件名
     * @param index_cols 索引涉及的字段元数据
     * @return std::unique_ptr<IxIndexHandle> 索引文件句柄指针
     */
    std::unique_ptr<IxIndexHandle> open_index(const std::string &filename, const std::vector<ColMeta> &index_cols)
    {
        std::string ix_name = get_index_name(filename, index_cols);
        int fd = disk_manager_->open_file(ix_name);
        return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
    }

    /**
     * @brief 打开索引文件，创建并返回索引文件句柄指针
     *
     * @param filename 文件名
     * @param index_cols 索引涉及的字段名
     * @return std::unique_ptr<IxIndexHandle> 索引文件句柄指针
     */
    std::unique_ptr<IxIndexHandle> open_index(const std::string &filename, const std::vector<std::string> &index_cols)
    {
        std::string ix_name = get_index_name(filename, index_cols);
        int fd = disk_manager_->open_file(ix_name);
        return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd);
    }

    /**
     * @brief 关闭索引文件
     *
     * @param ih 索引文件句柄指针
     */
    void close_index(const IxIndexHandle *ih)
    {
        auto data = new_char(ih->file_hdr_->tot_len_);
        ih->file_hdr_->serialize(data.get());
        disk_manager_->write_page(ih->fd_, IX_FILE_HDR_PAGE, data.get(), ih->file_hdr_->tot_len_);
        // 将缓冲区的所有页面刷到磁盘
        buffer_pool_manager_->flush_all_pages(ih->fd_);
        // 关闭文件
        disk_manager_->close_file(ih->fd_);
    }
};
