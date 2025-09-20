//
// Created by mt on 2025/8/9.
//

#ifndef RMDB_CACHE_H
#define RMDB_CACHE_H

#include <cstring>
#include "defs_finals.h"
#include "system/sm_manager_finals.h"
#include "common/context_finals.h"
#include "errors_finals.h"
#include "transaction/txn_defs_finals.h"

class DBCahce {
public:
    explicit DBCahce(SmManager *sm_manager) : sm_manager(sm_manager) {
    }

    bool has_cache(const char *sql, Context *ctx) const;

    void do_insert_cache(const char *sql, Context *ctx) const;

    static void do_begin_cache(Context *ctx);

    static void do_commit_cache(Context *ctx);

    SmManager *sm_manager;
};

inline bool DBCahce::has_cache(const char *sql, Context *ctx) const {
    switch (sql[0]) {
        case 'i': {
            do_insert_cache(sql, ctx);
            return true;
        }
        case 'c': {
            if (sql[1] == 'o') {
                do_commit_cache(ctx);
                return true;
            }
            return false;
        }
        case 'b': {
            do_begin_cache(ctx);
            return true;
        }
        default: {
            return false;
        }
    }
}

inline void DBCahce::do_begin_cache(Context *ctx) {
    ctx->txn_->txn_mode_ = true; // Set the transaction mode to false
}

inline void DBCahce::do_commit_cache(Context *ctx) {
    ctx->txn_->txn_mode_ = false; // Commit the transaction
}

inline void DBCahce::do_insert_cache(const char *sql, Context *ctx) const {
    int now_lex = 12; // Skip over "insert into "
    std::string table_name;

    // Direct table name parsing without creating intermediate strings
    while (sql[now_lex] != ' ' && sql[now_lex] != '\0') {
        table_name += sql[now_lex++];
    }

    // Skip spaces and "values"
    while (sql[now_lex] == ' ') ++now_lex;
    now_lex += 6;
    while (sql[now_lex] == ' ' || sql[now_lex] == '(') ++now_lex;

    const auto tab = sm_manager->db_.get_table(table_name);
    const auto fh_ = sm_manager->fhs_[tab->fd_].get();
    char *insert_data = sm_manager->memory_pool_manager_->allocate(fh_->record_size);

    // Efficient data parsing using direct index manipulation
    for (size_t col_idx = 0; col_idx < tab->cols.size(); ++col_idx) {
        const auto &col = tab->cols[col_idx];
        char *col_data = insert_data + col.offset;

        // Skip leading spaces
        while (sql[now_lex] == ' ') ++now_lex;

        // Directly handle each column type efficiently
        switch (col.type) {
            case TYPE_INT: {
                int value = 0;
                bool negative = false;
                if (sql[now_lex] == '-') {
                    negative = true;
                    ++now_lex; // Skip the negative sign
                }
                // Manually parse the integer
                while (sql[now_lex] >= '0' && sql[now_lex] <= '9') {
                    value = value * 10 + (sql[now_lex] - '0');
                    ++now_lex;
                }
                if (negative) value = -value; // Apply the negative sign if needed
                std::memcpy(col_data, &value, sizeof(value));
                break;
            }
            case TYPE_FLOAT: {
                bool negative = false;
                if (sql[now_lex] == '-') {
                    negative = true;
                    ++now_lex; // Skip the negative sign
                }

                // Parse integer part
                double integer_part = 0.0;
                while (sql[now_lex] >= '0' && sql[now_lex] <= '9') {
                    integer_part = integer_part * 10.0 + (sql[now_lex] - '0');
                    ++now_lex;
                }

                double value = integer_part;

                // Parse fractional part if exists
                if (sql[now_lex] == '.') {
                    now_lex++; // Skip decimal point
                    double fractional_part = 0.0;
                    double decimal_place = 0.1;

                    while (sql[now_lex] >= '0' && sql[now_lex] <= '9') {
                        fractional_part += (sql[now_lex] - '0') * decimal_place;
                        decimal_place *= 0.1;
                        ++now_lex;
                    }
                    value += fractional_part;
                }

                if (negative) value = -value; // Apply the negative sign if needed
                float float_value = static_cast<float>(value);
                std::memcpy(col_data, &float_value, sizeof(float_value));
                break;
            }
            case TYPE_STRING: {
                ++now_lex; // Skip opening quote
                int start_lex = now_lex;
                while (sql[now_lex] != '\'' && sql[now_lex] != '\0') {
                    ++now_lex;
                }
                int len = now_lex - start_lex;
                std::memcpy(col_data, &sql[start_lex], len);
                *(col_data + len) = '\0'; // Null terminate
                ++now_lex; // Skip closing quote
                break;
            }
            default: {
                break;
            }
        }

        // Skip trailing spaces and comma (if not the last column)
        while (sql[now_lex] == ' ') ++now_lex;
        if (col_idx < tab->cols.size() - 1 && sql[now_lex] == ',') {
            ++now_lex; // Skip comma
            while (sql[now_lex] == ' ') ++now_lex; // Skip spaces after comma
        }
    }

    // Stage 3: Insert data into indexes and record file
    const auto &indexes = tab->indexes;
    if (IxIndexHandle::unique_check) {
        for (auto &index: indexes) {
            auto ih = sm_manager->ihs_[index.fd_].get();
            if (ih->exists_entry(insert_data)) {
                sm_manager->memory_pool_manager_->deallocate(insert_data, fh_->record_size);
                throw IndexEntryAlreadyExistError();
            }
        }
    }

    // Parallelize index insertion (if possible)
    for (auto &index: indexes) {
        auto ih = sm_manager->ihs_[index.fd_].get();
        ih->insert_entry(insert_data);
    }

    // Insert into record file
    fh_->insert_record(insert_data);
    ctx->txn_->append_write_record(WriteType::INSERT_TUPLE, tab->fd_, insert_data);
}


#endif // RMDB_CACHE_H
