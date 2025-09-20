#pragma once

#include "execution_manager_finals.h"
#include "executor_abstract_finals.h"

class UpdateExecutor : public AbstractExecutor {
private:
  std::vector<SetClause> set_clauses_;
  SmManager *sm_manager_;
  std::vector<char *> old_rids_;
  std::vector<char *> new_rids_;
  std::vector<IndexMeta> *indexes;

public:
  UpdateExecutor(SmManager *sm_manager, const std::string &tab_name,
                 std::vector<SetClause> set_clauses, std::vector<char *> rids,
                 Context *context_) {
    sm_manager_ = sm_manager;
    set_clauses_ = std::move(set_clauses);
    auto tab_ = sm_manager_->db_.get_table(tab_name);
    auto fh_ = sm_manager_->fhs_[tab_->fd_].get();
    old_rids_ = std::move(rids);
    auto rid_size = static_cast<int>(old_rids_.size());
    indexes = &tab_->indexes;

    bool col_in_index = false;
    for (const auto &set_clause : set_clauses_) {
      if (tab_->is_col_in_index(set_clause.lhs->name)) {
        col_in_index = true;
        break;
      }
    }

    if (!col_in_index) {
      // 不涉及索引的更新，使用原地更新
      perform_in_place_update(tab_, fh_, context_);
      return;
    }

    for (auto old_rid_ : old_rids_) {
      auto new_rid_ =
          sm_manager_->memory_pool_manager_->allocate(fh_->record_size);
      memcpy(new_rid_, old_rid_, fh_->record_size);
      update_record(new_rid_);
      new_rids_.push_back(new_rid_);
    }
    for (auto old_rid_ : old_rids_) {
      for (auto &index : *indexes) {
        sm_manager_->ihs_[index.fd_]->delete_entry(old_rid_);
      }
    }

    for (auto &index : *indexes) {
      auto ih_ = sm_manager_->ihs_[index.fd_].get();
      for (auto new_rid_ : new_rids_) {
        if (IxIndexHandle::unique_check)
        {
          if (ih_->exists_entry(new_rid_))
          {
            handle_index_entry_already_exist_error();
            throw IndexEntryAlreadyExistError();
          }
        }

        ih_->insert_entry(new_rid_);
      }
    }

    for (size_t i = 0; i < rid_size; i++) {
      fh_->update_record(old_rids_[i], new_rids_[i]);
      context_->txn_->append_write_record(WriteType::UPDATE_TUPLE_ON_INDEX, tab_->fd_,
                                          old_rids_[i], new_rids_[i]);
    }
  }

private:
  void perform_in_place_update(TabMeta *tab_, RmFileHandle *fh_,
                               Context *context_) {
    // 不涉及索引的原地更新
    for (auto rid_ : old_rids_) {
      auto bak_rid_ =
          sm_manager_->memory_pool_manager_->allocate(fh_->record_size);
      memcpy(bak_rid_, rid_, fh_->record_size);
      // 使用备份的原始记录作为参考，避免在更新过程中读取到已修改的值
      update_record(rid_);
      context_->txn_->append_write_record(WriteType::UPDATE_TUPLE, tab_->fd_,
                                          bak_rid_, rid_);
    }
  }

  void handle_index_entry_already_exist_error() {
    for (auto &index : *indexes) {
      auto ih_ = sm_manager_->ihs_[index.fd_].get();
      for (auto new_rid_ : new_rids_) {
        ih_->delete_entry(new_rid_);
      }
      for (auto old_rid_ : old_rids_) {
        ih_->insert_entry(old_rid_);
      }
    }
  }

  void update_record(char *record) {
    for (const auto &set_clause : set_clauses_) {
      char *data_ptr = record + set_clause.lhs->offset;

      switch (set_clause.lhs->type) {
      case TYPE_INT:
        switch (set_clause.op) {
        case SELF_ADD:
          *reinterpret_cast<int *>(data_ptr) += set_clause.rhs.int_val;
          break;
        case SELF_SUB:
          *reinterpret_cast<int *>(data_ptr) -= set_clause.rhs.int_val;
          break;
        case SELF_MUT:
          *reinterpret_cast<int *>(data_ptr) *= set_clause.rhs.int_val;
          break;
        case SELF_DIV:
          if (set_clause.rhs.int_val != 0) {
            *reinterpret_cast<int *>(data_ptr) /= set_clause.rhs.int_val;
          } else {
            std::cerr << "Division by zero in update_record" << std::endl;
          }
          break;
        case ASSINGMENT:
          *reinterpret_cast<int *>(data_ptr) = set_clause.rhs.int_val;
          break;
        case UNKNOWN:
          break;
        }
        break;

      case TYPE_FLOAT:
        switch (set_clause.op) {
        case SELF_ADD:
          *reinterpret_cast<float *>(data_ptr) += set_clause.rhs.float_val;
          break;
        case SELF_SUB:
          *reinterpret_cast<float *>(data_ptr) -= set_clause.rhs.float_val;
          break;
        case SELF_MUT:
          *reinterpret_cast<float *>(data_ptr) *= set_clause.rhs.float_val;
          break;
        case SELF_DIV:
          if (set_clause.rhs.float_val != 0.0f) {
            *reinterpret_cast<float *>(data_ptr) /= set_clause.rhs.float_val;
          } else {
            std::cerr << "Division by zero in update_record" << std::endl;
          }
          break;
        case ASSINGMENT:
          *reinterpret_cast<float *>(data_ptr) = set_clause.rhs.float_val;
          break;
        case UNKNOWN:
          break;
        }
        break;

      case TYPE_STRING: {
        int len = set_clause.lhs->len;

        switch (set_clause.op) {
        case ASSINGMENT:
          std::memset(data_ptr, 0, len);
          std::memcpy(data_ptr, set_clause.rhs.str_val.c_str(), len);
          break;
        case SELF_ADD:
          std::strncat(data_ptr, set_clause.rhs.str_val.c_str(),
                       len - std::strlen(data_ptr) - 1);
          break;
        case SELF_SUB:
          // Implement string subtraction if needed
          break;
        case SELF_MUT:
          break;
        case SELF_DIV:
          break;
        case UNKNOWN:
          break;
        }
      } break;

      default:
        std::cerr << "Unknown type in update_record" << std::endl;
        break;
      }
    }
  }
};
