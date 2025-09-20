#pragma once

#include <csv-parser/single_include/csv.hpp>

#include "common/context.h"
#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"

class Context;

struct ColDef
{
    std::string name; // Column name
    ColType type;     // Type of column
    int len;          // Length of column
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager
{
public:
    DbMeta db_;                                                           // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> fhs_;  // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> ihs_; // file name -> index file handle, 当前数据库中每个索引的文件
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    RmManager *rm_manager_;
    IxManager *ix_manager_;

    bool io_enabled_ = true;

    SmManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, RmManager *rm_manager, IxManager *ix_manager) : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), rm_manager_(rm_manager), ix_manager_(ix_manager) {}

    ~SmManager() = default;

    BufferPoolManager *get_bpm() { return buffer_pool_manager_; }

    RmManager *get_rm_manager() { return rm_manager_; }

    IxManager *get_ix_manager() { return ix_manager_; }

    void set_log_offset(size_t offset)
    {
        db_.log_offset_ = offset;
        flush_meta();
    }

    bool is_dir(const std::string &db_name);

    void create_db(const std::string &db_name);

    void drop_db(const std::string &db_name);

    void open_db(const std::string &db_name);

    void close_db();

    void flush_meta() const;

    void show_tables(Context *context);

    void desc_table(const std::string &tab_name, Context *context);

    void create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context);

    void drop_table(const std::string &tab_name, Context *context);

    void create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);

    void drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);

    void drop_index(const std::string &tab_name, const std::vector<ColMeta> &col_names, Context *context);

    void show_index(const std::string &tab_name, Context *context);

    void load_csv_data(const std::string &csv_file_path, const std::string &tab_name);
};
