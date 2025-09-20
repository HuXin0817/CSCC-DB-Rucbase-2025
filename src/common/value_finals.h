#pragma once

#include <limits.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "config_finals.h"
#include "defs_finals.h"
#include "errors_finals.h"
#include "parser/ast.h"
#include "record/rm_defs_finals.h"
#include "../deps/parallel_hashmap/phmap.h"

#include <unordered_set>

inline int parse_int(const std::string &str) {
    int result = 0;
    const char *cstr = str.c_str();
    bool negative = (*cstr == '-');
    if (negative)
        ++cstr;
    while (*cstr) {
        result = (result << 3) + (result << 1) + (*cstr - '0');
        ++cstr;
    }
    return negative ? -result : result;
}

inline float parse_float(const std::string &str) {
    float result = 0.0f;
    float factor = 1.0f;
    const char *cstr = str.c_str();
    bool negative = (*cstr == '-');
    if (negative)
        ++cstr;
    bool decimal_found = false;
    while (*cstr) {
        if (*cstr == '.') {
            decimal_found = true;
            ++cstr;
            continue;
        }
        if (decimal_found) {
            factor *= 0.1f;
            result += (*cstr - '0') * factor;
        } else {
            result = result * 10.0f + (*cstr - '0');
        }
        ++cstr;
    }
    return negative ? -result : result;
}

class NameManager {
public:
    static const int get_fd(const std::string &name) {
        auto it = name2fd.find(name);
        if (it != name2fd.end()) {
            return it->second;
        } else {
            auto fd = uuid++;
            fd2name[fd] = name;
            name2fd.emplace(name, fd);
            return fd;
        }
    }

    static const std::string &get_name(int fd) { return fd2name[fd]; }

private:
    static std::atomic<int> uuid;
    static std::string fd2name[MAX_TABLE_NUMBER];
    static phmap::flat_hash_map<std::string, int> name2fd;
};

struct Value {
    ColType type;
    union {
        int int_val;
        float float_val;
    };
    float sum_value;
    int count_value;

    std::string str_val;

    std::shared_ptr<RmRecord> raw;


    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void init_raw(int len) {
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            *reinterpret_cast<int *>(raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            *reinterpret_cast<float *>(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < static_cast<int>(str_val.size())) {
                throw RMDBError();
            }
            memset(raw->data, 0, len);
            std::memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }


    bool operator==(const Value &other) const {
        if (type == other.type) {
            switch (type) {
                case TYPE_INT:
                    return int_val == other.int_val;
                case TYPE_FLOAT:
                    return float_val == other.float_val;
                case TYPE_STRING:
                    return str_val == other.str_val;
                default:
                    return false;
            }
        } else if (can_cast_type(other.type)) {
            if (type == TYPE_INT)
                return static_cast<float>(int_val) == other.float_val;
            return float_val == static_cast<float>(other.int_val);
        }
        throw RMDBError();
    }


    bool operator!=(const Value &other) const {
        return !(*this == other);
    }


    bool operator<(const Value &other) const {
        if (type != other.type)
            throw RMDBError();
        switch (type) {
            case TYPE_INT:
                return int_val < other.int_val;
            case TYPE_FLOAT:
                return float_val < other.float_val;
            case TYPE_STRING:
                return str_val < other.str_val;
            default:
                throw RMDBError();
        }
    }


    bool operator<=(const Value &other) const {
        return *this < other || *this == other;
    }


    bool operator>(const Value &other) const {
        return !(*this <= other);
    }


    bool operator>=(const Value &other) const {
        return !(*this < other);
    }


    bool can_cast_type(ColType to) const {
        if (type == to)
            return true;
        if (type == TYPE_INT && to == TYPE_FLOAT)
            return true;
        if (type == TYPE_FLOAT && to == TYPE_INT)
            return true;
        return false;
    }


    Value() {
    }
};

namespace std {
    template<>
    struct hash<Value> {
        std::size_t operator()(const Value &v) const {
            std::size_t h1 = std::hash<int>{}(static_cast<int>(v.type));
            std::size_t h2;
            switch (v.type) {
                case ColType::TYPE_INT:
                    h2 = std::hash<int>{}(v.int_val);
                    break;
                case ColType::TYPE_FLOAT:
                    h2 = std::hash<float>{}(v.float_val);
                    break;
                case ColType::TYPE_STRING:
                    h2 = std::hash<std::string>{}(v.str_val);
                    break;

                default:
                    h2 = 0;
            }
            return h1 ^ (h2 << 1);
        }
    };
}

struct ColMeta {
    std::string tab_name;
    std::string name;
    ColType type;
    ast::AggFuncType agg_func_type;
    int len{};
    int offset{};
    bool index{};
    int idx;

    ColMeta() = default;

    ColMeta(std::string tabName, std::string name, ColType type, ast::AggFuncType agg_func_type, int len, int offset,
            bool index, int id = -1) : tab_name(std::move(tabName)), name(std::move(name)), type(type),
                                       agg_func_type(agg_func_type), len(len), offset(offset), index(index), idx(id) {
    };
};

inline std::string get_index_name(const std::string &filename, const std::vector<std::string> &index_cols) {
    std::string index_name = filename;
    for (const auto &index_col: index_cols)
        index_name += "^" + index_col;
    return index_name;
}

inline std::string get_index_name(const std::string &filename, const std::vector<ColMeta> &index_cols) {
    std::string index_name = filename;
    for (const auto &index_col: index_cols)
        index_name += "^" + index_col.name;
    return index_name;
}

struct IndexMeta {
    IndexMeta() = default;

    IndexMeta(const std::string &index_name, const std::vector<ColMeta> &cols) {
        index_name_ = index_name;
        fd_ = NameManager::get_fd(index_name);
        cols_ = cols;
    }

    int fd_;
    std::string index_name_;
    std::vector<ColMeta> cols_;
};

struct TabMeta {
    int fd_;
    std::string name_;
    int col_tot_len = 0;
    std::vector<ColMeta> cols;
    phmap::flat_hash_map<std::string, ColMeta> cols_idx_;
    std::vector<IndexMeta> indexes;
    phmap::flat_hash_map<std::string, IndexMeta> indexes_idx_;
    std::unordered_set<std::string> col_in_index_;

    bool is_col_in_index(const std::string &col_name) const {
        return col_in_index_.count(col_name)==1;
    }

    explicit TabMeta(const std::string &name) {
        fd_ = NameManager::get_fd(name);
        name_ = name;
    }

    bool is_index(const std::vector<std::string> &col_names) const {
        auto idx_name = get_index_name(name_, col_names);
        return indexes_idx_.find(idx_name) != indexes_idx_.end();
    }

    void push_back(const IndexMeta &index) {
        indexes.push_back(index);
        indexes_idx_[index.index_name_] = index;
        for (const auto &col: index.cols_) {
            col_in_index_.insert(col.name);
        }
    }

    void push_back(const ColMeta &col) {
        cols.push_back(col);
        cols_idx_[col.name] = col;
        col_tot_len += col.len;
    }

    void erase_index(std::string &index_name) {
        for (auto it = indexes.begin(); it != indexes.end(); it++) {
            if (it->index_name_ == index_name) {
                indexes.erase(it);
                indexes_idx_.erase(index_name);
                return;
            }
        }
    }

    const IndexMeta &get_index_meta(const std::vector<std::string> &col_names) {
        auto idx_name = get_index_name(name_, col_names);
        auto it = indexes_idx_.find(idx_name);
        if (it == indexes_idx_.end()) {
            throw RMDBError();
        } else {
            return it->second;
        }
    }

    const ColMeta &get_col(const std::string &col_name) { return cols_idx_.at(col_name); }
};

class DbMeta {
    friend class SmManager;

public:
    std::string name_;
    phmap::flat_hash_map<std::string, std::unique_ptr<TabMeta> > tabs_;

    bool is_table(const std::string &tab_name) const {
        return tabs_.find(tab_name) != tabs_.end();
    }

    TabMeta *get_table(const std::string &tab_name) {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end()) {
            throw RMDBError();
        }
        return pos->second.get();
    }
};
