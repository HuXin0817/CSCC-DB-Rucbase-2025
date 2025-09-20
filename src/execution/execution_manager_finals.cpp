

#include "execution_manager_finals.h"

#include "execution_group_finals.h"
#include "executor_delete_finals.h"
#include "executor_index_scan_finals.h"
#include "executor_insert_finals.h"
#include "executor_nestedloop_join_finals.h"
#include "executor_projection_finals.h"
#include "executor_seq_scan_finals.h"
#include "executor_update_finals.h"
#include "record_printer.h"

const char *help_info =
        "Supported SQL syntax:\n"
        "  command ;\n"
        "command:\n"
        "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
        "  DROP TABLE table_name\n"
        "  CREATE INDEX table_name (column_name)\n"
        "  DROP INDEX table_name (column_name)\n"
        "  INSERT INTO table_name VALUES (value [, value ...])\n"
        "  DELETE FROM table_name [WHERE where_clause]\n"
        "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
        "  SELECT selector FROM table_name [WHERE where_clause]\n"
        "type:\n"
        "  {INT | FLOAT | CHAR(n)}\n"
        "where_clause:\n"
        "  cond [AND cond ...]\n"
        "cond:\n"
        "  column op {column | value}\n"
        "column:\n"
        "  [table_name.]column_name\n"
        "op:\n"
        "  {= | <> | < | > | <= | >=}\n"
        "selector:\n"
        "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(const std::shared_ptr<Plan> &plan, Context *context) {
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch (x->tag) {
            case T_CreateTable: {
                sm_manager_->create_table(x->tab_name_, x->cols_, context);
                break;
            }
            case T_DropTable: {
                sm_manager_->drop_table(x->tab_name_, context);
                break;
            }
            case T_CreateIndex: {
                sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            case T_DropIndex: {
                sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            default:
                throw RMDBError();
        }
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(const std::shared_ptr<Plan> &plan, txn_id_t *txn_id, Context *context) {
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch (x->tag) {
            case T_Help: {
                std::memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
                *(context->offset_) = strlen(help_info);
                break;
            }
            case T_ShowTable: {
                sm_manager_->show_tables(context);
                break;
            }
            case T_DescTable: {
                sm_manager_->desc_table(x->tab_name_, context);
                break;
            }
            case T_DescIndex: {
                sm_manager_->show_index(x->tab_name_, context);
                break;
            }
            case T_Create_StaticCheckPoint: {
                break;
            }
            case T_Crash: {
                break;
            }
            case T_Transaction_begin: {
                // 显示开启一个事务
                context->txn_->set_txn_mode(true);
                if (!ban_fh_ && !IxIndexHandle::unique_check) {
                    ban_fh_ = true;
                    Context::MAX_OFFSET_LENGTH = BUFFER_LENGTH >> 4;
                    for (auto &fh_: sm_manager_->fhs_) {
                        if (fh_ != nullptr) {
                            fh_->ban = true;
                        }
                    }
                }
                break;
            }
            case T_Transaction_commit: {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->commit(context->txn_);
                break;
            }
            case T_Transaction_rollback: {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context->txn_);
                break;
            }
            case T_Transaction_abort: {
                context->txn_ = txn_mgr_->get_transaction(*txn_id);
                txn_mgr_->abort(context->txn_);
                break;
            }
            case T_LoadData: {
                ban_fh_ = true;
                sm_manager_->load_csv_data(x->file_name_, x->tab_name_);
                break;
            }
            case T_IoEnable: {
                sm_manager_->io_enabled_ = x->io_enable_;
                break;
            }
            default:
                throw RMDBError();
        }
    } else if (auto x = std::dynamic_pointer_cast<SetKnobPlan>(plan)) {
        switch (x->set_knob_type_) {
            case ast::SetKnobType::EnableNestLoop: {
                planner_->set_enable_nestedloop_join(x->bool_value_);
                break;
            }
            case ast::SetKnobType::EnableSortMerge: {
                planner_->set_enable_sortmerge_join(x->bool_value_);
                break;
            }
            default: {
                throw RMDBError();
            }
        }
    }
}

std::string handleAggregateFunction(const TabCol &sel_col) {
    switch (sel_col.aggFuncType) {
        case ast::AggFuncType::COUNT:
            return "COUNT(" + sel_col.col_name + ")";
        case ast::AggFuncType::SUM:
            return "SUM(" + sel_col.col_name + ")";
        case ast::AggFuncType::MAX:
            return "MAX(" + sel_col.col_name + ")";
        case ast::AggFuncType::MIN:
            return "MIN(" + sel_col.col_name + ")";
        case ast::AggFuncType::AVG:
            return "AVG(" + sel_col.col_name + ")";
        default:
            throw RMDBError();
    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, const std::vector<TabCol> &sel_cols,
                            Context *context) {
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col: sel_cols) {
        if (sel_col.alias.empty()) {
            if (sel_col.aggFuncType == ast::default_type) {
                captions.push_back(sel_col.col_name);
            } else {
                captions.push_back(handleAggregateFunction(sel_col));
            }
        } else {
            captions.push_back(sel_col.alias);
        }
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    if (sm_manager_->io_enabled_) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for (const auto &caption: captions) {
            outfile << " " << caption << " |";
        }
        outfile << "\n";
    }

    // Print records
    size_t num_rec = 0;
    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        if (!sm_manager_->io_enabled_ && context->data_send_is_full()) {
            break;
        }
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        for (auto &col: executorTreeRoot->cols()) {
            std::string col_str;
            char *rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                auto val = *(int *) rec_buf;
                if (val == INT_MAX)
                    col_str = "";
                else
                    col_str = std::to_string(*(int *) rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                auto val = *(float *) rec_buf;
                if (val == FLT_MAX)
                    col_str = "";
                else
                    col_str = std::to_string(*(float *) rec_buf);
            } else if (col.type == TYPE_STRING) {
                col_str = std::string((char *) rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            }
            columns.push_back(col_str);
        }
        // print record into buffer
        rec_printer.print_record(columns, context);
        // print record into file
        if (sm_manager_->io_enabled_) {
            outfile << "|";
            for (const auto &column: columns) {
                outfile << " " << column << " |";
            }
            outfile << "\n";
        }
        num_rec++;
    }
    if (sm_manager_->io_enabled_) {
        outfile.close();
    }
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) {
    exec->Next();
}

std::unordered_set<Value> QlManager::sub_select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot,
                                                     bool converse_to_float) {
    std::unordered_set<Value> results;
    // 确保一列
    if (executorTreeRoot->cols().size() != 1)
        throw RMDBError();
    const auto &col = executorTreeRoot->cols()[0];

    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        char *rec_buf = Tuple->data + col.offset;
        Value value;
        switch (col.type) {
            case TYPE_INT: {
                auto val = *reinterpret_cast<int *>(rec_buf);
                if (val != INT_MAX) {
                    if (!converse_to_float)
                        value.set_int(val);
                    else
                        value.set_float(static_cast<float>(val));
                }
                break;
            }
            case TYPE_FLOAT: {
                auto val = *reinterpret_cast<float *>(rec_buf);
                if (val != FLT_MAX) {
                    value.set_float(val);
                }
                break;
            }
            case TYPE_STRING: {
                std::string col_str(reinterpret_cast<char *>(rec_buf), col.len);
                col_str.resize(strlen(col_str.c_str()));
                value.set_str(col_str);
                break;
            }
            default: {
                throw RMDBError();
            }
        }
        results.insert(value);
    }

    return results;
}
