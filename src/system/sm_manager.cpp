
#include "sm_manager.h"

#include <common/common.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string &db_name)
{
    struct stat st
    {
    };
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string &db_name)
{
    // 如果数据库文件夹已存在，则抛出数据库已存在异常
    if (is_dir(db_name))
    {
        throw DatabaseExistsError(db_name);
    }
    // 创建数据库文件夹
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0)
    {
        throw UnixError();
    }
    // 切换到数据库文件夹
    if (chdir(db_name.c_str()) < 0)
    {
        throw UnixError();
    }
    // 创建数据库元数据
    auto *new_db = new DbMeta();
    new_db->name_ = db_name;
    std::ofstream ofs(DB_META_NAME);
    ofs << *new_db; // 保存元数据到文件
    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);
    // 切换回上一级目录
    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string &db_name)
{
    // 如果数据库文件夹不存在，则抛出数据库未找到异常
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    // 删除数据库文件夹及其内容
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string &db_name)
{
    // 如果数据库文件夹不存在，则抛出数据库未找到异常
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    // 如果已有数据库打开，则抛出数据库已存在异常
    if (!db_.name_.empty())
    {
        throw DatabaseExistsError(db_name);
    }
    // 切换到数据库文件夹
    if (chdir(db_name.c_str()) < 0)
    {
        throw UnixError();
    }
    // 加载数据库元数据
    std::ifstream ifs(DB_META_NAME);
    if (!ifs)
    {
        throw UnixError();
    }
    ifs >> db_;
    // 打开所有表和索引文件
    for (auto &tab : db_.tabs_)
    {
        const std::string &tab_name = tab.first;
        auto &tab_meta = tab.second;
        fhs_[tab_name] = rm_manager_->open_file(tab_name);
        for (auto &index : tab_meta.indexes)
        {
            auto index_name = IxManager::get_index_name(tab_name, index.cols);
            ihs_[index_name] = ix_manager_->open_index(tab_name, index.cols);
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() const
{
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db()
{
    // 如果没有数据库打开，则抛出数据库未找到异常
    if (db_.name_.empty())
    {
        throw DatabaseNotFoundError("db not open");
    }
    // 刷新元数据到磁盘
    flush_meta();
    // 清空数据库元数据
    db_.name_.clear();
    db_.tabs_.clear();
    fhs_.clear();
    ihs_.clear();
    // 切换回上一级目录
    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context *context)
{
    std::fstream outfile;
    if (io_enabled_)
    {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "| Tables |\n";
    }
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_)
    {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        if (io_enabled_)
        {
            outfile << "| " << tab.name << " |\n";
        }
    }
    printer.print_separator(context);
    if (io_enabled_)
    {
        outfile.close();
    }
}

/**
 * @description: 显示索引
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::show_index(const std::string &tab_name, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);
    std::fstream outfile;
    if (io_enabled_)
    {
        outfile.open("output.txt", std::ios::out | std::ios::app);
    }
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"index"}, context);
    printer.print_separator(context);
    for (auto &index : tab.indexes)
    {
        if (io_enabled_)
        {
            outfile << "| " << tab.name << " | unique | (" << index.cols[0].name;
            for (size_t i = 1; i < index.cols.size(); ++i)
            {
                outfile << "," << index.cols[i].name;
            }
            outfile << ") |\n";
        }
        printer.print_record({IxManager::get_index_name(tab_name, index.cols)}, context);
    }
    printer.print_separator(context);
    if (io_enabled_)
    {
        outfile.close();
    }
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string &tab_name, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);
    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    for (auto &col : tab.cols)
    {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context)
{
    // 如果表已存在，则抛出表已存在异常
    if (db_.is_table(tab_name))
    {
        throw TableExistsError(tab_name);
    }
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs)
    {
        ColMeta col(tab_name, col_def.name, col_def.type, ast::AggFuncType::default_type, col_def.len, curr_offset, false);
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    int record_size = curr_offset;
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string &tab_name, Context *context)
{
    // 如果表不存在，则抛出表未找到异常
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }
    TabMeta &tab = db_.get_table(tab_name);
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name);
    for (auto &index : tab.indexes)
    {
        auto index_name = IxManager::get_index_name(tab_name, index.cols);
        auto ih_ = std::move(ihs_.at(index_name));
        ix_manager_->close_index(ih_.get());
        ix_manager_->destroy_index(tab_name, index.cols);
        ihs_.erase(index_name);
    }
    tab.indexes.clear();
    db_.tabs_.erase(tab_name);
    fhs_.erase(tab_name);
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);
    if (tab.is_index(col_names))
    {
        throw IndexExistsError(tab_name, col_names);
    }
    std::vector<ColMeta> cols;
    int tot_col_len = 0;
    for (auto &col_name : col_names)
    {
        cols.emplace_back(*tab.get_col(col_name));
        tot_col_len += cols.back().len;
    }
    auto fh_ = fhs_[tab_name].get();
    ix_manager_->create_index(tab_name, cols);
    auto ih = ix_manager_->open_index(tab_name, cols);
    int idx = -1;
    for (RmScan rmScan(fh_); !rmScan.is_end(); rmScan.next())
    {
        auto rec = fh_->get_record(rmScan.rid(), context);
        auto insert_data = std::make_unique<char[]>(tot_col_len + 4).get();
        std::memcpy(insert_data + tot_col_len, &idx, 4);
        int offset = 0;
        for (auto &col : cols)
        {
            std::memcpy(insert_data + offset, rec->data + col.offset, col.len);
            offset += col.len;
        }
        try
        {
            ih->insert_entry(insert_data, rmScan.rid(), context);
        }
        catch (IndexEntryAlreadyExistError &)
        {
        }
    }
    auto index_name = IxManager::get_index_name(tab_name, col_names);
    if (ihs_.count(index_name))
    {
        throw IndexExistsError(tab_name, col_names);
    }
    ihs_.emplace(index_name, std::move(ih));
    IndexMeta indexMeta = {tab_name, tot_col_len, static_cast<int>(cols.size()), cols};
    tab.indexes.emplace_back(indexMeta);
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);
    if (!tab.is_index(col_names))
    {
        return;
    }
    auto index_name = IxManager::get_index_name(tab_name, col_names);
    if (ihs_.count(index_name) == 0)
    {
        return;
    }
    auto ih = std::move(ihs_.at(index_name));
    ix_manager_->close_index(ih.get());
    ix_manager_->destroy_index(tab_name, col_names);
    ihs_.erase(index_name);
    auto index_meta = tab.get_index_meta(col_names);
    tab.indexes.erase(index_meta);
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<ColMeta> &cols, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> cols_name;
    cols_name.reserve(cols.size());
    for (auto &col : cols)
    {
        cols_name.emplace_back(col.name);
    }
    auto index_meta = tab.get_index_meta(cols_name);
    auto index_name = IxManager::get_index_name(tab_name, cols);
    if (ihs_.count(index_name) == 0)
    {
        return;
    }
    auto ih_ = std::move(ihs_.at(index_name));
    ix_manager_->close_index(ih_.get());
    ix_manager_->destroy_index(tab_name, cols);
    ihs_.erase(index_name);
    tab.indexes.erase(index_meta);
    flush_meta();
}

void SmManager::load_csv_data(const std::string &csv_file_path, const std::string &tab_name)
{
    // 创建CSVReader对象，指定文件路径
    csv::CSVReader reader(csv_file_path);

    auto tab_ = db_.get_table(tab_name);
    auto fh_ = fhs_.at(tab_name).get();

    size_t record_size = fh_->file_hdr_.record_size;
    auto record = new char[record_size];

    // 读取CSV文件的每一行
    for (const csv::CSVRow &row : reader)
    {
        std::memset(record, 0, record_size);
        auto offset = 0;
        for (const auto &col : tab_.cols)
        {
            switch (col.type)
            {
            case ColType::TYPE_INT:
            {
                int value = row[col.name].get<int>();
                memcpy(record + offset, &value, col.len);
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                float value = row[col.name].get<float>();
                memcpy(record + offset, &value, col.len);
                break;
            }
            case ColType::TYPE_STRING:
            {
                std::string value = row[col.name].get<>();
                memcpy(record + offset, value.c_str(), value.size());
                break;
            }
            }

            offset += col.len;
        }

        auto rid_ = fh_->insert_record(record, nullptr);
        // Insert into index
        for (const auto &index : tab_.indexes)
        {
            auto idx_name = IxManager::get_index_name(tab_name, index.cols);
            auto ih = ihs_.at(idx_name).get();
            auto key = new_char(index.col_tot_len);
            int offset_ = 0;
            for (size_t i = 0; i < static_cast<unsigned long>(index.col_num); ++i)
            {
                std::memcpy(key.get() + offset_, record + index.cols[i].offset, index.cols[i].len);
                offset_ += index.cols[i].len;
            }

            ih->insert_entry(key.get(), rid_, nullptr);
        }
    }
}
