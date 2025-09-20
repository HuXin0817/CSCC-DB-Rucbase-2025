#pragma once

#include "common/context_finals.h"
#include "index/ix_index_handle_finals.h"
#include "record/rm_file_handle_finals.h"
#include "storage/memory_pool_manager.h"
#include "../deps/parallel_hashmap/phmap.h"
class Context;

struct ColDef {
  std::string name;
  ColType type;
  int len;
};

class SmManager {
public:
  SmManager(PoolManager *memory_pool_manager)
      : memory_pool_manager_(memory_pool_manager) {}

  PoolManager *memory_pool_manager_;

  DbMeta db_;
  std::unique_ptr<RmFileHandle> fhs_[MAX_TABLE_NUMBER];
  std::unique_ptr<IxIndexHandle> ihs_[MAX_TABLE_NUMBER];

  phmap::flat_hash_map<std::string, ColMeta> col_meta_map_;

  bool io_enabled_ = true;

  static bool is_dir(const std::string &db_name);

  void create_db(const std::string &db_name);

  void drop_db(const std::string &db_name);

  void open_db(const std::string &db_name);

  void close_db();

  void show_tables(Context *context);

  void desc_table(const std::string &tab_name, Context *context);

  void create_table(const std::string &tab_name,
                    const std::vector<ColDef> &col_defs, Context *context);

  void drop_table(const std::string &tab_name, Context *context);

  void create_index(const std::string &tab_name,
                    const std::vector<std::string> &col_names,
                    Context *context);

  void drop_index(const std::string &tab_name,
                  const std::vector<std::string> &col_names, Context *context);

  void drop_index(const std::string &tab_name,
                  const std::vector<ColMeta> &col_names, Context *context);

  void show_index(const std::string &tab_name, Context *context);

  void load_csv_data(const std::string &csv_file_path,
                     const std::string &tab_name);
};
