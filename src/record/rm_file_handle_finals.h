#pragma once

#include <atomic> // 鐢ㄤ簬 ban 鏍囧織鐨勫井浼樺寲
#include <memory>
#include <mutex>
#include <unordered_set> // 鏇挎崲 vector

#include "common/context_finals.h"
#include "rm_defs_finals.h"

class RmFileHandle {
public:
  const int record_size;
  // 使用原子变量，可在不加锁的情况下快速检查 ban 状态
  std::atomic<bool> ban = false;

  // 使用哈希集实现 O(1) 的平均操作复杂度
  std::unordered_set<char *> records_;
  // 互斥锁保护 records_
  mutable std::mutex mutex_; // 设为 mutable 以便在 const 成员函数中加锁

  explicit RmFileHandle(int record_size, const std::string table_name)
      : record_size(record_size) {
  }

  // 禁止拷贝和移动，确保句柄的唯一性和安全性
  RmFileHandle(const RmFileHandle &) = delete;
  RmFileHandle &operator=(const RmFileHandle &) = delete;

  std::unique_ptr<RmRecord> get_record(const char *rid) const {
    // 假设 RmRecord 构造不修改 rid 指向的内容
    return std::make_unique<RmRecord>(const_cast<char *>(rid), record_size);
  }

  void insert_record(char *rid) {
    if (ban.load(std::memory_order_relaxed)) {
      return;
    }
    std::lock_guard lk(mutex_);
    records_.insert(rid); // O(1) 平均复杂度
  }

  void delete_record(const char *rid) {
    if (ban.load(std::memory_order_relaxed)) {
      return;
    }
    std::lock_guard lk(mutex_);
    records_.erase(const_cast<char *>(rid)); // O(1) 平均复杂度
  }

  void update_record(const char *old_rid, char *new_rid) {
    if (ban.load(std::memory_order_relaxed)) {
      return;
    }
    std::lock_guard lk(mutex_);
    // 先删除旧的，再插入新的
    if (records_.erase(const_cast<char *>(old_rid)) > 0) {
      records_.insert(new_rid);
    }
  }

  // 提供一个获取当前记录数的方法可能很有用
  size_t get_record_count() const {
    std::lock_guard lk(mutex_);
    return records_.size();
  }
};