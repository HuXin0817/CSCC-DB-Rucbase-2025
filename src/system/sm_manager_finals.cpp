#include "sm_manager_finals.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "record/rm_scan_finals.h"
#include "record_printer.h"

bool IxIndexHandle::unique_check = true;

std::atomic<int> NameManager::uuid{0};
std::string NameManager::fd2name[MAX_TABLE_NUMBER];
phmap::flat_hash_map<std::string, int> NameManager::name2fd;

bool SmManager::is_dir(const std::string &db_name)
{
    struct stat st
    {
    };
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void SmManager::create_db(const std::string &db_name)
{
    if (is_dir(db_name))
    {
        throw RMDBError();
    }

    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0)
    {
        throw RMDBError();
    }
}

void SmManager::drop_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw RMDBError();
    }

    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0)
    {
        throw RMDBError();
    }
}

void SmManager::open_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw RMDBError();
    }

    if (!db_.name_.empty())
    {
        throw RMDBError();
    }

    if (chdir(db_name.c_str()) < 0)
    {
        throw RMDBError();
    }

    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile.close();
}

void SmManager::close_db()
{
    if (db_.name_.empty())
    {
        throw RMDBError();
    }

    db_.name_.clear();
    db_.tabs_.clear();

    if (chdir("..") < 0)
    {
        throw RMDBError();
    }
}

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
        printer.print_record({tab->name_}, context);
        if (io_enabled_)
        {
            outfile << "| " << tab->name_ << " |\n";
        }
    }
    printer.print_separator(context);
    if (io_enabled_)
    {
        outfile.close();
    }
}

void SmManager::show_index(const std::string &tab_name, Context *context)
{
    auto tab = db_.get_table(tab_name);
    std::fstream outfile;
    if (io_enabled_)
    {
        outfile.open("output.txt", std::ios::out | std::ios::app);
    }
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"index"}, context);
    printer.print_separator(context);
    for (auto &index : tab->indexes)
    {
        if (io_enabled_)
        {
            outfile << "| " << tab->name_ << " | unique | (" << index.cols_[0].name;
            for (size_t i = 1; i < index.cols_.size(); ++i)
            {
                outfile << "," << index.cols_[i].name;
            }
            outfile << ") |\n";
        }
        printer.print_record({get_index_name(tab_name, index.cols_)}, context);
    }
    printer.print_separator(context);
    if (io_enabled_)
    {
        outfile.close();
    }
}

void SmManager::desc_table(const std::string &tab_name, Context *context)
{
    auto tab = db_.get_table(tab_name);
    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    for (auto &col : tab->cols)
    {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    printer.print_separator(context);
}

void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context)
{
    if (db_.is_table(tab_name))
    {
        throw RMDBError();
    }
    int curr_offset = 0;
    auto tab = std::make_unique<TabMeta>(tab_name);
    for (size_t i = 0; i < col_defs.size(); i++)
    {
        auto &col_def = col_defs[i];
        ColMeta col(tab_name, col_def.name, col_def.type, ast::AggFuncType::default_type, col_def.len, curr_offset, false, i);
        curr_offset += col_def.len;
        tab->push_back(col);

        col_meta_map_[col.name] = col;
    }
    int record_size = curr_offset;
    fhs_[tab->fd_] = std::make_unique<RmFileHandle>(record_size, tab_name);
    db_.tabs_[tab_name] = std::move(tab);
}

void SmManager::drop_table(const std::string &tab_name, Context *context)
{
    if (!db_.is_table(tab_name))
    {
        throw RMDBError();
    }
    db_.tabs_.erase(tab_name);
}

void SmManager::create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
    auto tab = db_.get_table(tab_name);
    if (tab->is_index(col_names))
    {
        throw RMDBError();
    }
    std::vector<ColMeta> cols;
    cols.reserve(col_names.size());
    for (auto &col_name : col_names)
    {
        cols.emplace_back(tab->get_col(col_name));
    }
    auto fh_ = fhs_[NameManager::get_fd(tab_name)].get();

    auto index_name = get_index_name(tab_name, col_names);

    IndexMeta indexMeta(index_name, cols);

    auto ih = std::make_unique<IxIndexHandle>(indexMeta);
    for (RmScan rmScan(fh_); !rmScan.is_end(); rmScan.next())
    {
        ih->insert_entry(rmScan.rid());
    }
    ihs_[indexMeta.fd_] = std::move(ih);
    tab->push_back(indexMeta);
}

void SmManager::drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
    auto tab = db_.get_table(tab_name);
    if (!tab->is_index(col_names))
    {
        return;
    }
    auto index_name = get_index_name(tab_name, col_names);
    tab->erase_index(index_name);
}

void SmManager::drop_index(const std::string &tab_name, const std::vector<ColMeta> &cols, Context *context)
{
    std::vector<std::string> cols_name;
    cols_name.reserve(cols.size());
    for (auto &col : cols)
    {
        cols_name.emplace_back(col.name);
    }
    drop_index(tab_name, cols_name, context);
}

void SmManager::load_csv_data(const std::string &csv_file_path, const std::string &tab_name)
{
    IxIndexHandle::unique_check = false;

    std::ifstream file(csv_file_path);
    auto tab_ = db_.get_table(tab_name);
    auto fh_ = fhs_[tab_->fd_].get();

    // Skip the first line (header)
    std::string line;
    std::getline(file, line, '\n');
    for (;;)
    {
        std::getline(file, line, '\n');
        if (line.empty())
        {
            break;
        }
        std::stringstream line_stream(line);
        std::string cell;
        auto offset = 0;
        char *record_data = new char[fh_->record_size];

        for (const auto &col : tab_->cols)
        {
            std::getline(line_stream, cell, ',');
            switch (col.type)
            {
            case ColType::TYPE_INT:
            {
                int value = parse_int(cell);
                std::memcpy(record_data + offset, &value, col.len);
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                float value = parse_float(cell);
                std::memcpy(record_data + offset, &value, col.len);
                break;
            }
            case ColType::TYPE_STRING:
            {
                std::memcpy(record_data + offset, cell.c_str(), col.len);
                break;
            }
            }
            offset += col.len;
        }

        fh_->insert_record(record_data);

        // Insert into index
        for (const auto &index : tab_->indexes)
        {
            ihs_[index.fd_]->insert_entry(record_data);
        }
    }
    fh_->ban=true;
    file.close();
}
