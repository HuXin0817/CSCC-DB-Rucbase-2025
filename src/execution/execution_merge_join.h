#include <cfloat>
#include <utility>

//
// Created by root on 24-6-12.
//

#ifndef RMDB_EXECUTION_MERGE_JOIN_H
#define RMDB_EXECUTION_MERGE_JOIN_H

class MergeJoinExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> left_executor_;  // 左表执行器
    std::unique_ptr<AbstractExecutor> right_executor_; // 右表执行器
    size_t tuple_length_;                              // 连接后每条记录的长度
    std::vector<ColMeta> columns_;                     // 连接后的记录的字段

    std::vector<std::string> tables;

    std::vector<Condition> join_conds_; // 连接条件

    // 等值连接左列属性
    std::vector<ColMeta>::const_iterator left_join_col;
    // 等值连接右列属性
    std::vector<ColMeta>::const_iterator right_join_col;

    std::unique_ptr<RmRecord> left_record_;
    std::unique_ptr<RmRecord> right_record_;
    bool left_end_;
    bool right_end_;

public:
    MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, std::vector<Condition> conds, const TabCol &left_col, const TabCol &right_col, std::vector<std::string> tables_) : left_executor_(std::move(left)), right_executor_(std::move(right)), tables(std::move(tables_)), join_conds_(std::move(conds))
    {
        tuple_length_ = left_executor_->tupleLen() + right_executor_->tupleLen();
        columns_ = left_executor_->cols();
        auto right_columns = right_executor_->cols();
        for (auto &col : right_columns)
        {
            col.offset += left_executor_->tupleLen();
        }
        columns_.insert(columns_.end(), right_columns.begin(), right_columns.end());

        left_join_col = get_col(left_executor_->cols(), left_col);
        right_join_col = get_col(right_executor_->cols(), right_col);
        print();
    }

    void beginTuple() override
    {
        left_executor_->beginTuple();
        right_executor_->beginTuple();
        left_record_ = left_executor_->Next();
        right_record_ = right_executor_->Next();
        left_end_ = left_executor_->is_end();
        right_end_ = right_executor_->is_end();
        find_next_valid_tuple();
    }

    void nextTuple() override
    {
        assert(!is_end());
        right_executor_->nextTuple();
        if (right_executor_->is_end())
        {
            right_end_ = true;
            return;
        }
        right_record_ = right_executor_->Next();
        find_next_valid_tuple();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        assert(!is_end());
        auto join_record = std::make_unique<RmRecord>(tuple_length_);
        memcpy(join_record->data, left_record_->data, left_executor_->tupleLen());
        memcpy(join_record->data + left_executor_->tupleLen(), right_record_->data, right_executor_->tupleLen());

        return join_record;
    }

    Rid &rid() override { return _abstract_rid; }

    size_t tupleLen() const override { return tuple_length_; }

    const std::vector<ColMeta> &cols() const override { return columns_; }

    bool is_end() const override { return left_end_ || right_end_; }

private:
    void find_next_valid_tuple()
    {
        while (!is_end())
        {
            int res = compare_record(left_record_, right_record_);
            if (res == 0 && evaluateConditions(left_record_.get(), right_record_.get()))
            {
                break; // 找到有效元组
            }
            if (res > 0)
            {
                right_executor_->nextTuple();
                if (right_executor_->is_end())
                {
                    right_end_ = true;
                    break;
                }
            }
            else
            {
                left_executor_->nextTuple();
                if (left_executor_->is_end())
                {
                    left_end_ = true;
                    break;
                }
            }
            left_record_ = left_executor_->Next();
            right_record_ = right_executor_->Next();
        }
    }

    bool evaluateCondition(const RmRecord *left_record, const RmRecord *right_record, const Condition &cond)
    {
        auto lhs_column_meta = get_col(columns_, cond.lhs_col);
        auto rhs_column_meta = get_col(columns_, cond.rhs_col);
        auto lhs_data = left_record->data + lhs_column_meta->offset;
        auto rhs_data = right_record->data + rhs_column_meta->offset - left_executor_->tupleLen();
        ColType rhs_type = rhs_column_meta->type;
        if (lhs_column_meta->type != rhs_type)
        {
            return false;
        }
        int comparison_result = compareValues(lhs_data, rhs_data, lhs_column_meta->len, rhs_type);
        switch (cond.op)
        {
        case OP_EQ:
            return comparison_result == 0;
        case OP_NE:
            return comparison_result != 0;
        case OP_LT:
            return comparison_result < 0;
        case OP_GT:
            return comparison_result > 0;
        case OP_LE:
            return comparison_result <= 0;
        case OP_GE:
            return comparison_result >= 0;
        default:
            throw InternalError("Unexpected operator type");
        }
    }

    bool evaluateConditions(const RmRecord *left_record, const RmRecord *right_record)
    {
        return std::all_of(join_conds_.begin(), join_conds_.end(), [&](const Condition &cond)
                           { return evaluateCondition(left_record, right_record, cond); });
    }

    static int compareValues(const char *a, const char *b, int column_length, ColType column_type)
    {
        switch (column_type)
        {
        case TYPE_INT:
        {
            int ai = *(int *)a;
            int bi = *(int *)b;
            return ai > bi ? 1 : (ai < bi ? -1 : 0);
        }
        case TYPE_FLOAT:
        {
            double af = *(double *)a;
            double bf = *(double *)b;
            return af > bf ? 1 : (af < bf ? -1 : 0);
        }
        case TYPE_STRING:
            return memcmp(a, b, column_length);
        default:
            throw InternalError("Unexpected data type");
        }
    }
    int compare_record(std::unique_ptr<RmRecord> &left_record, std::unique_ptr<RmRecord> &right_record)
    {
        if (left_join_col->type != right_join_col->type)
        {
            throw InternalError("Unexpected data type");
        }
        return compareValues(left_record->data + left_join_col->offset, right_record->data + right_join_col->offset, left_join_col->len, left_join_col->type);
    }

    void print()
    {
        std::fstream outfile;
        outfile.open("sorted_results.txt", std::ios::out | std::ios::app);

        if (tables[0] == left_join_col->tab_name)
        {
            print_left(outfile);
            print_right(outfile);
        }
        else
        {
            print_right(outfile);
            print_left(outfile);
        }

        outfile.close();
    }

    void print_left(std::fstream &outfile)
    {
        std::vector<std::string> captions;
        for (auto &sel_col : left_executor_->cols())
        {
            captions.push_back(sel_col.name);
        }

        // print header into file
        outfile << "|";
        for (const auto &caption : captions)
        {
            outfile << " " << caption << " |";
        }
        outfile << "\n";

        for (left_executor_->beginTuple(); !left_executor_->is_end(); left_executor_->nextTuple())
        {
            auto Tuple = left_executor_->Next();
            std::vector<std::string> columns;
            for (auto &col : left_executor_->cols())
            {
                std::string col_str;
                char *rec_buf = Tuple->data + col.offset;
                if (col.type == TYPE_INT)
                {
                    auto val = *(int *)rec_buf;
                    if (val == INT_MAX)
                        col_str = "";
                    else
                        col_str = std::to_string(*(int *)rec_buf);
                }
                else if (col.type == TYPE_FLOAT)
                {
                    auto val = *(float *)rec_buf;
                    if (val == FLT_MAX)
                        col_str = "";
                    else
                        col_str = std::to_string(*(float *)rec_buf);
                }
                else if (col.type == TYPE_STRING)
                {
                    col_str = std::string((char *)rec_buf, col.len);
                    col_str.resize(strlen(col_str.c_str()));
                }
                columns.push_back(col_str);
            }

            outfile << "|";
            for (const auto &column : columns)
            {
                outfile << " " << column << " |";
            }
            outfile << "\n";
        }
    }

    void print_right(std::fstream &outfile)
    {
        std::vector<std::string> captions;
        for (auto &sel_col : right_executor_->cols())
        {
            captions.push_back(sel_col.name);
        }

        // print header into file
        outfile << "|";
        for (const auto &caption : captions)
        {
            outfile << " " << caption << " |";
        }
        outfile << "\n";

        for (right_executor_->beginTuple(); !right_executor_->is_end(); right_executor_->nextTuple())
        {
            auto Tuple = right_executor_->Next();
            std::vector<std::string> columns;
            for (auto &col : right_executor_->cols())
            {
                std::string col_str;
                char *rec_buf = Tuple->data + col.offset;
                if (col.type == TYPE_INT)
                {
                    auto val = *(int *)rec_buf;
                    if (val == INT_MAX)
                        col_str = "";
                    else
                        col_str = std::to_string(*(int *)rec_buf);
                }
                else if (col.type == TYPE_FLOAT)
                {
                    auto val = *(float *)rec_buf;
                    if (val == FLT_MAX)
                        col_str = "";
                    else
                        col_str = std::to_string(*(float *)rec_buf);
                }
                else if (col.type == TYPE_STRING)
                {
                    col_str = std::string((char *)rec_buf, col.len);
                    col_str.resize(strlen(col_str.c_str()));
                }
                columns.push_back(col_str);
            }

            outfile << "|";
            for (const auto &column : columns)
            {
                outfile << " " << column << " |";
            }
            outfile << "\n";
        }
    }
};

#endif // RMDB_EXECUTION_MERGE_JOIN_H
