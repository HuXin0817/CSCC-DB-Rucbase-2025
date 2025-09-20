#pragma once

#include <queue>
#include <shared_mutex>
#include <utility>
#include <shared_mutex>
#include <cstring>
//#include "btree.h"
#include "common/context_finals.h"
#include "common/value_finals.h"
#include "transaction/transaction_finals.h"
#include "../deps/parallel_hashmap/btree.h"

class IxScan;

class IndexScanExecutor;

#define rmdb_btree phmap::btree_set<char *, IxCompare>

class IxCompare {
private:
    static constexpr size_t MAX_COLS = 16; // Maximum number of columns supported
    struct ColInfo { int offset; int len; ColType type; };
    
    ColInfo colinfos_[MAX_COLS];
    int int_offsets_[MAX_COLS];
    size_t col_count_ = 0;
    size_t int_count_ = 0;
    bool single_col_ = false;
    bool single_int_ = false;
    bool all_int_ = false;
    // Small-N unrolled all-int fast path
    bool small_all_int_ = false;
    int small_int_cnt_ = 0;
    int small_int_off_[4] = {0,0,0,0};

public:
    IxCompare() = default;

    explicit IxCompare(const IndexMeta &index_meta) noexcept {
        // Use fixed-size arrays to make the class nothrow copy constructible
        col_count_ = (index_meta.cols_.size() < MAX_COLS) ? index_meta.cols_.size() : MAX_COLS;
        int_count_ = 0;
        
        for (size_t i = 0; i < col_count_; ++i) {
            const auto &c = index_meta.cols_[i];
            colinfos_[i] = {c.offset, c.len, c.type};
            if (c.type == TYPE_INT && int_count_ < MAX_COLS) {
                int_offsets_[int_count_++] = c.offset;
            }
        }
        
        single_col_ = (col_count_ == 1);
        single_int_ = (col_count_ == 1 && colinfos_[0].type == TYPE_INT);
        all_int_ = (int_count_ == col_count_);
        // Prepare small-N unrolled offsets for common TPCC patterns (2,3,4 ints)
        small_all_int_ = all_int_ && col_count_ <= 4;
        if (small_all_int_) {
            small_int_cnt_ = static_cast<int>(col_count_);
            for (int i = 0; i < small_int_cnt_; ++i) small_int_off_[i] = colinfos_[i].offset;
        }
    }

    inline bool operator()(const char *a, const char *b) const {
        // Fast path: single int column
        if (single_int_) {
            int ia, ib;
            std::memcpy(&ia, a + colinfos_[0].offset, sizeof(int));
            std::memcpy(&ib, b + colinfos_[0].offset, sizeof(int));
            return ia < ib;
        }

        // Fast path: all columns are int (lexicographic by column order)
        if (all_int_) {
            // Unrolled for small-N common cases: (2),(3),(4)
            if (small_all_int_) {
                switch (small_int_cnt_) {
                    case 2: {
                        int a0, b0; std::memcpy(&a0, a + small_int_off_[0], 4); std::memcpy(&b0, b + small_int_off_[0], 4);
                        if (a0 != b0) return a0 < b0;
                        int a1, b1; std::memcpy(&a1, a + small_int_off_[1], 4); std::memcpy(&b1, b + small_int_off_[1], 4);
                        return a1 < b1;
                    }
                    case 3: {
                        int a0, b0; std::memcpy(&a0, a + small_int_off_[0], 4); std::memcpy(&b0, b + small_int_off_[0], 4);
                        if (a0 != b0) return a0 < b0;
                        int a1, b1; std::memcpy(&a1, a + small_int_off_[1], 4); std::memcpy(&b1, b + small_int_off_[1], 4);
                        if (a1 != b1) return a1 < b1;
                        int a2, b2; std::memcpy(&a2, a + small_int_off_[2], 4); std::memcpy(&b2, b + small_int_off_[2], 4);
                        return a2 < b2;
                    }
                    case 4: {
                        int a0, b0; std::memcpy(&a0, a + small_int_off_[0], 4); std::memcpy(&b0, b + small_int_off_[0], 4);
                        if (a0 != b0) return a0 < b0;
                        int a1, b1; std::memcpy(&a1, a + small_int_off_[1], 4); std::memcpy(&b1, b + small_int_off_[1], 4);
                        if (a1 != b1) return a1 < b1;
                        int a2, b2; std::memcpy(&a2, a + small_int_off_[2], 4); std::memcpy(&b2, b + small_int_off_[2], 4);
                        if (a2 != b2) return a2 < b2;
                        int a3, b3; std::memcpy(&a3, a + small_int_off_[3], 4); std::memcpy(&b3, b + small_int_off_[3], 4);
                        return a3 < b3;
                    }
                }
            }
            // Compare following the original column order, using precomputed offsets
            for (size_t i = 0; i < int_count_; ++i) {
                int ia, ib;
                std::memcpy(&ia, a + int_offsets_[i], sizeof(int));
                std::memcpy(&ib, b + int_offsets_[i], sizeof(int));
                if (ia != ib) return ia < ib;
            }
            return false;
        }

        // Mixed types: generic path
        for (size_t i = 0; i < col_count_; ++i) {
            const auto &col = colinfos_[i];
            const char *value1 = a + col.offset;
            const char *value2 = b + col.offset;
            switch (col.type) {
                case TYPE_INT: {
                    int ia, ib;
                    std::memcpy(&ia, value1, sizeof(int));
                    std::memcpy(&ib, value2, sizeof(int));
                    if (ia != ib) return ia < ib;
                    break;
                }
                case TYPE_FLOAT: {
                    float fa, fb;
                    std::memcpy(&fa, value1, sizeof(float));
                    std::memcpy(&fb, value2, sizeof(float));
                    if (fa != fb) return fa < fb;
                    break;
                }
                case TYPE_STRING: {
                    int res = std::memcmp(value1, value2, col.len);
                    if (res != 0) return res < 0;
                    break;
                }
            }
        }
        return false;
    }
};

class IxIndexHandle {
public:
    static bool unique_check;

    rmdb_btree bp_tree_;
    mutable std::shared_mutex rw_mutex;  // mutable allows const methods to lock it

public:
    explicit IxIndexHandle(const IndexMeta &index_meta) : bp_tree_(IxCompare(index_meta)) {}

    bool exists_entry(char *key) const {
        // std::shared_lock lk(rw_mutex);  // shared lock for read-only operation
        return bp_tree_.contains(key);
    }

    auto find_entry(char *key) const {
        return bp_tree_.find(key);
    }

    void insert_entry(char *key) {
        std::unique_lock lk(rw_mutex);  // exclusive lock for write operation
        bp_tree_.insert(key);
    }

    void delete_entry(char *key) {
        std::unique_lock lk(rw_mutex);  // exclusive lock for write operation
        bp_tree_.erase(key);
    }

    auto upper_bound(char *key) const {
        // std::shared_lock lk(rw_mutex);
        return bp_tree_.upper_bound(key);
    }

    auto lower_bound(char *key) const {
        // std::shared_lock lk(rw_mutex);
        return bp_tree_.lower_bound(key);
    }

    auto begin() const {
        // std::shared_lock lk(rw_mutex);
        return bp_tree_.begin();
    }

    auto end() const {
        //std::shared_lock lk(rw_mutex);
        return bp_tree_.end();
    }
};
