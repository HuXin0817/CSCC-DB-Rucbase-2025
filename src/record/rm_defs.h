#pragma once

#include <list>

#include "defs.h"
#include "storage/buffer_pool_manager.h"

constexpr int RM_NO_PAGE = -1;
constexpr int RM_FILE_HDR_PAGE = 0;
constexpr int RM_FIRST_RECORD_PAGE = 1;
constexpr int RM_MAX_RECORD_SIZE = 512;

/* 文件头，记录表数据文件的元信息，写入磁盘中文件的第0号页面 */
struct RmFileHdr
{
    int record_size;          // 表中每条记录的大小，由于不包含变长字段，因此当前字段初始化后保持不变
    int num_pages;            // 文件中分配的页面个数（初始化为1）
    int num_records_per_page; // 每个页面最多能存储的元组个数
    int first_free_page_no;   // 文件中当前第一个包含空闲空间的页面号（初始化为-1）
    int bitmap_size;          // 每个页面bitmap大小
};

/* 表数据文件中每个页面的页头，记录每个页面的元信息 */
struct RmPageHdr
{
    int next_free_page_no; // 当前页面满了之后，下一个包含空闲空间的页面号（初始化为-1）
    int num_records;       // 当前页面中当前已经存储的记录个数（初始化为0）
};

/* 表中的记录 */
struct RmRecord
{
    char *data = nullptr;    // 记录的数据
    int size = 0;            // 记录的大小
    bool allocated_ = false; // 是否已经为数据分配空间

    // 默认构造函数
    RmRecord() : data(nullptr), size(0), allocated_(false) {}

    // 带参数的构造函数
    RmRecord(int size_, char *data_)
    {
        size = size_;
        data = new char[size_];
        memcpy(data, data_, size_);
        allocated_ = true;
    }

    // 带参数的构造函数
    RmRecord(char *data_, int size_)
    {
        size = size_;
        data = new char[size_];
        memcpy(data, data_, size_);
        allocated_ = true;
    }

    // 带参数的构造函数
    RmRecord(int size_, const char *data_)
    {
        size = size_;
        data = new char[size_];
        memcpy(data, data_, size_);
        allocated_ = true;
    }

    // 带参数的构造函数
    RmRecord(const char *data_, int size_)
    {
        size = size_;
        data = new char[size_];
        memcpy(data, data_, size_);
        allocated_ = true;
    }

    explicit RmRecord(const std::string &str)
    {
        size = str.size();
        data = new char[size];
        std::memcpy(data, str.c_str(), size);
        allocated_ = true;
    }

    bool operator==(const RmRecord &other) const
    {
        if (size != other.size)
        {
            return false;
        }
        return std::memcmp(data, other.data, size) == 0;
    }

    void reset(const std::string &str)
    {
        if (allocated_)
        {
            delete[] data;
        }
        size = str.size();
        data = new char[size];
        std::memcpy(data, str.c_str(), size);
        allocated_ = true;
    }

    std::string to_string() const { return {data, static_cast<size_t>(size)}; }

    RmRecord(const RmRecord &other)
    {
        size = other.size;
        data = new char[size];
        memcpy(data, other.data, size);
        allocated_ = true;
    };

    RmRecord &operator=(const RmRecord &other)
    {
        size = other.size;
        data = new char[size];
        std::memcpy(data, other.data, size);
        allocated_ = true;
        return *this;
    };

    explicit RmRecord(int size_)
    {
        size = size_;
        data = new char[size_];
        allocated_ = true;
    }

    /**
     * @brief 序列化记录
     * 将记录序列化到指定的内存位置。
     * @param dest 指向目标内存位置的指针
     * @return 序列化后的总长度
     */
    size_t serialize(char *dest) const
    {
        size_t offset = 0;
        offset += encode::serialize("[[", dest + offset);
        offset += encode::serialize(size, dest + offset);
        offset += encode::serialize(" ", dest + offset);
        offset += encode::serialize(data, dest + offset, size);
        offset += encode::serialize("]] ", dest + offset);
        return offset;
    }

    /**
     * @brief 反序列化记录
     * 从指定的内存位置反序列化记录。
     * @param dest 指向源内存位置的指针
     * @return 反序列化后的总长度
     */
    size_t deserialize(const char *dest)
    {
        size_t offset = 0;
        offset += encode::deserialize("[[", dest + offset);
        offset += encode::deserialize(size, dest + offset);
        offset += encode::deserialize(" ", dest + offset);
        offset += encode::deserialize(data, dest + offset, size);
        offset += encode::deserialize("]] ", dest + offset);
        return offset;
    }

    ~RmRecord()
    {
        if (allocated_)
        {
            delete[] data;
        }
        allocated_ = false;
        data = nullptr;
    }
};
